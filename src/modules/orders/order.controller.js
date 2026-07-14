import pool from "../../db/pool.js";
import { withUserAudit } from "../../middleware/dbAudit.js";
import logger from "../../services/logger.js";
import {
  calculateETA,
  computeDaysUntilEta,
} from "../../services/calculateEta.js";
import { moveReceiverToNextStatus } from "../../services/moveReceiverToNextStatus.js";
import { createOrderTracking } from "../../services/createOrderTracking.js";
import { notifyOrderStatusUpdate } from "../../services/sendOrderEmail.js";

function normalizeDate(dateStr) {
  if (!dateStr) return null;
  const date = new Date(dateStr);
  return isNaN(date.getTime()) ? null : date.toISOString().split("T")[0];
}

export async function createOrder(req, res) {
  let client;
  try {
    client = await pool.connect();
    await client.query("BEGIN");

    const b = req.body || {};
    const files = req.files || {};
    const isSender = (b.sender_type || "sender") === "sender";

    let shippingParties = [];
    try {
      shippingParties = JSON.parse(isSender ? b.receivers : b.senders) || [];
    } catch {}

    if (!shippingParties.length) {
      await client.query("ROLLBACK");
      return res.status(400).json({
        error: "Invalid create fields",
        details: `Shipping parties (${isSender ? "receivers" : "senders"}) is required`,
      });
    }

    let flatOrderItems = [];
    try {
      flatOrderItems = JSON.parse(b.order_items || "[]");
    } catch {}

    const itemsByParty = flatOrderItems.reduce((acc, item) => {
      const parts = (item.item_ref || item.itemRef || "").split("-");
      const idx = parts.length >= 6 ? parseInt(parts[3]) - 1 : 0;
      (acc[idx] = acc[idx] || []).push(item);
      return acc;
    }, {});

    let dropOffByParty = {};
    try {
      JSON.parse(b.drop_off_details || "[]").forEach((d) => {
        const idx = parseInt(d.receiver_index);
        if (!isNaN(idx))
          (dropOffByParty[idx] = dropOffByParty[idx] || []).push(d);
      });
    } catch {}

    if (!b.rgl_booking_number?.trim() || !b.sender_type?.trim()) {
      await client.query("ROLLBACK");
      return res.status(400).json({
        error: "Invalid create fields",
        details: "rglBookingNumber is required; senderType is required",
      });
    }

    const buildFileList = (existingKey, uploadKey) => {
      let list = [];
      try {
        list = JSON.parse(b[existingKey] || "[]");
        if (!Array.isArray(list)) list = [];
      } catch {}
      const uploaded = (files[uploadKey] || []).flatMap((f) => {
        const url = f.path || f.secure_url || f.url;
        return url
          ? [
              {
                url,
                public_id: f.filename || f.public_id,
                originalname: f.originalname,
                mimetype: f.mimetype,
                size: f.size,
                uploadedAt: new Date().toISOString(),
              },
            ]
          : [];
      });
      return [...list, ...uploaded];
    };

    const newAttachments = buildFileList("attachments_existing", "attachments");
    const newGatepass = buildFileList("gatepass_existing", "gatepass");

    const ownerPrefix = isSender ? "sender" : "receiver";
    const owner = {
      name: b[`${ownerPrefix}_name`] || "",
      contact: b[`${ownerPrefix}_contact`] || "",
      address: b[`${ownerPrefix}_address`] || "",
      email: b[`${ownerPrefix}_email`] || "",
      ref: b[`${ownerPrefix}_ref`] || "",
      remarks: b[`${ownerPrefix}_remarks`] || "",
    };

    const duplicateCheck = await client.query(
      `
      SELECT
        EXISTS(
          SELECT 1
          FROM orders
          WHERE booking_ref = $1
        ) AS booking_ref_exists,

        EXISTS(
          SELECT 1
          FROM orders
          WHERE rgl_booking_number = $2
        ) AS rgl_booking_exists
      `,
      [b.booking_ref, b.rgl_booking_number],
    );

    const { booking_ref_exists, rgl_booking_exists } = duplicateCheck.rows[0];

    if (booking_ref_exists || rgl_booking_exists) {
      await client.query("ROLLBACK");

      return res.status(409).json({
        success: false,
        error: [
          booking_ref_exists && "Booking Ref already exists",
          rgl_booking_exists && "RGL Booking Number already exists",
        ]
          .filter(Boolean)
          .join("; "),
      });
    }

    const ordersResult = await withUserAudit(
      req,
      `INSERT INTO orders (
        booking_ref, status, rgl_booking_number, place_of_loading, point_of_origin,
        final_destination, place_of_delivery, order_remarks, attachments
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
      RETURNING id, booking_ref, status, created_at, created_by, updated_by`,
      [
        b.booking_ref,
        b.status || "Created",
        b.rgl_booking_number,
        b.place_of_loading,
        b.point_of_origin,
        b.final_destination,
        b.place_of_delivery,
        b.order_remarks || "",
        JSON.stringify(newAttachments),
      ],
    );
    const { id: orderId } = ordersResult.rows[0];
    const newOrder = ordersResult.rows[0];

    const senderResult = await client.query(
      `INSERT INTO senders (
        order_id, sender_name, sender_contact, sender_address, sender_email,
        sender_ref, sender_remarks, sender_type, selected_sender_owner
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
      RETURNING id`,
      [
        orderId,
        owner.name,
        owner.contact,
        owner.address,
        owner.email,
        owner.ref,
        owner.remarks,
        b.sender_type || "sender",
        b.selected_sender_owner || "",
      ],
    );
    const senderId = senderResult.rows[0].id;
    const trackingData = [];
    const trackingRows = [];

    for (let i = 0; i < shippingParties.length; i++) {
      const p = shippingParties[i];
      const items = itemsByParty[i] || [];

      const totalNumber = items.reduce(
        (s, it) => s + (parseInt(it.total_number || it.totalNumber) || 0),
        0,
      );
      const totalWeight = items.reduce(
        (s, it) => s + (parseFloat(it.weight) || 0),
        0,
      );

      const etaCalc = p.eta
        ? normalizeDate(p.eta)
        : await calculateETA(client, p.status || "Created");
      const normEta =
        typeof etaCalc === "string" ? etaCalc : etaCalc?.eta || null;
      const normEtd = p.etd ? normalizeDate(p.etd) : null;
      const receiverItemRefs = items
        .map((it) => it.item_ref || it.itemRef || "")
        .filter(Boolean)
        .join(",");

      const recResult = await withUserAudit(
        req,
        `INSERT INTO receivers (
          order_id, receiver_name, receiver_contact, receiver_address, receiver_email,
          receiver_marks_and_number, eta, etd, shipping_line,
          consignment_vessel, consignment_number, consignment_marks, consignment_voyage,
          total_number, total_weight, remarks, containers, status, full_partial, qty_delivered, item_ref
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20, $21)
        RETURNING id`,
        [
          orderId,
          p.receiver_name || p.receiverName || "",
          p.receiver_contact || p.receiverContact || "",
          p.receiver_address || p.receiverAddress || "",
          p.receiver_email || p.receiverEmail || "",
          p.receiver_marks_and_number || "",
          normEta,
          normEtd,
          "",
          "",
          "",
          "",
          "",
          totalNumber || null,
          totalWeight || null,
          p.remarks || "",
          JSON.stringify(p.containers || p.containerDetails || []),
          p.status || "Created",
          p.full_partial || "Full",
          p.qty_delivered ? parseInt(p.qty_delivered) : null,
          receiverItemRefs,
        ],
      );
      const receiverId = recResult.rows[0].id;

      for (const item of items) {
        const itemRef = item.item_ref || item.itemRef || "";
        await withUserAudit(
          req,
          `INSERT INTO order_items (
            order_id, receiver_id, item_ref, pickup_location, delivery_address,
            category, subcategory, type, total_number, weight, container_details
          ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
          [
            orderId,
            receiverId,
            itemRef,
            item.pickup_location || item.pickupLocation || "",
            item.delivery_address || item.deliveryAddress || "",
            item.category || "",
            item.subcategory || "",
            item.type || "",
            parseInt(item.total_number || item.totalNumber) || 0,
            parseFloat(item.weight) || 0,
            JSON.stringify(
              item.containerDetails || item.container_details || [],
            ),
          ],
        );

        trackingRows.push([
          orderId,
          senderId,
          owner.ref || null,
          receiverId,
          item.status || "Order Created",
          normEta,
          normEtd || normEta,
          req.user?.username || req.user?.email || req.user?.id || "system",
          itemRef,
        ]);
      }

      for (const row of trackingRows) {
        await client.query(
          `INSERT INTO order_tracking (
            order_id,
            sender_id,
            sender_ref,
            receiver_id,
            status,
            eta,
            etd,
            created_by,
            item_ref
          )
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
          row,
        );
      }
      trackingRows.length = 0;

      for (const d of dropOffByParty[i] || []) {
        await withUserAudit(
          req,
          `INSERT INTO drop_off_details (
            order_id, receiver_id, drop_method, dropoff_name, drop_off_cnic, drop_off_mobile, plate_no, drop_date
          ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
          [
            orderId,
            receiverId,
            d.drop_method || null,
            d.dropoff_name || null,
            d.drop_off_cnic || null,
            d.drop_off_mobile || null,
            d.plate_no || null,
            d.drop_date ? normalizeDate(d.drop_date) : null,
          ],
        );
      }

      trackingData.push({
        receiverId,
        status: p.status || b.status,
        totalShippingDetails: items.length,
        totalDropOffDetails: (dropOffByParty[i] || []).length,
      });
    }

    await withUserAudit(
      req,
      `INSERT INTO transport_details (
        order_id, transport_type, drop_method, dropoff_name, drop_off_cnic, drop_off_mobile,
        plate_no, drop_date, collection_method, collection_scope, qty_delivered,
        client_receiver_name, client_receiver_id, client_receiver_mobile, delivery_date, gatepass,
        third_party_transport, driver_name, driver_contact, driver_nic, driver_pickup_location, truck_number
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)
      RETURNING id`,
      [
        orderId,
        b.transport_type,
        b.drop_method || null,
        b.dropoff_name || null,
        b.drop_off_cnic || null,
        b.drop_off_mobile || null,
        b.plate_no || null,
        b.drop_date ? normalizeDate(b.drop_date) : null,
        b.collection_method || null,
        b.collection_scope || null,
        b.qty_delivered ? parseInt(b.qty_delivered) : null,
        b.client_receiver_name || null,
        b.client_receiver_id || null,
        b.client_receiver_mobile || null,
        b.delivery_date ? normalizeDate(b.delivery_date) : null,
        JSON.stringify(newGatepass),
        b.third_party_transport || null,
        b.driver_name || null,
        b.driver_contact || null,
        b.driver_nic || null,
        b.driver_pickup_location || null,
        b.truck_number || null,
      ],
    );

    await client.query("COMMIT");

    if (
      b.send_email_notification === "true" ||
      b.send_email_notification === true
    ) {
      const recipientEmail = owner.email?.trim();
      if (recipientEmail) {
        try {
          await pool.query(
            `INSERT INTO email_queue (order_id, recipient_email, recipient_name, email_type)
         VALUES ($1, $2, $3, $4)`,
            [orderId, recipientEmail, owner.name || null, "order_created"],
          );
        } catch (queueErr) {
          console.error(
            "[createOrder] Failed to enqueue email:",
            queueErr.message,
          );
        }
      } else {
        console.warn(
          `[createOrder] Order ${orderId} requested email but owner has no email`,
        );
      }
    }

    res.status(201).json({
      success: true,
      order: newOrder,
      tracking: trackingData,
      attachments: newAttachments,
      gatepass: newGatepass,
    });
  } catch (error) {
    console.error("Error processing order:", error);
    if (client) await client.query("ROLLBACK");
    return res.status(500).json({
      error: "Internal server error",
      details: error.message,
      stack: process.env.NODE_ENV === "development" ? error.stack : undefined,
    });
  } finally {
    if (client) client.release();
  }
}

export async function updateOrder(req, res) {
  let client;
  try {
    client = await pool.connect();
    await client.query("BEGIN");

    const { id } = req.params;
    const updates = req.body || {};
    const files = req.files || {};

    let hasAnyChange = false;
    let hasAttachmentChange = false;
    let hasGatepassChange = false;

    const currentOrderRes = await client.query(
      "SELECT * FROM orders WHERE id = $1",
      [id],
    );
    if (currentOrderRes.rowCount === 0) {
      await client.query("ROLLBACK");
      return res.status(404).json({ error: "Order not found" });
    }
    const currentOrder = currentOrderRes.rows[0];

    const currentSenderRes = await client.query(
      "SELECT * FROM senders WHERE order_id = $1",
      [id],
    );
    const currentSender = currentSenderRes.rows[0] || {};

    const currentTransportRes = await client.query(
      "SELECT * FROM transport_details WHERE order_id = $1",
      [id],
    );
    const currentTransport = currentTransportRes.rows[0] || {};

    let incomingReceivers = [];
    const panel2Key =
      updates.sender_type === "sender" ? "receivers" : "senders";
    if (updates[panel2Key]) {
      try {
        incomingReceivers = JSON.parse(updates[panel2Key]);
      } catch (e) {
        console.warn("[updateOrder] Failed to parse receivers/senders:", e);
      }
    }

    let incomingOrderItems = [];
    if (updates.order_items) {
      try {
        incomingOrderItems = JSON.parse(updates.order_items);
      } catch (e) {
        console.warn("[updateOrder] Failed to parse order_items:", e);
      }
    }

    let incomingDropOffs = [];
    if (updates.drop_off_details) {
      try {
        incomingDropOffs = JSON.parse(updates.drop_off_details);
      } catch (e) {
        console.warn("[updateOrder] Failed to parse drop_off_details:", e);
      }
    }

    let currentAttachments = currentOrder.attachments || [];
    if (typeof currentAttachments === "string") {
      currentAttachments = JSON.parse(currentAttachments) || [];
    }

    let newAttachments = currentAttachments;

    if (updates.attachments_existing) {
      newAttachments = JSON.parse(updates.attachments_existing) || [];
      hasAttachmentChange = true;
    }

    if (files.attachments?.length > 0) {
      const uploaded = files.attachments.map((file) => ({
        url: file.path || file.secure_url || file.location || "",
        public_id: file.filename || file.public_id || "",
        originalname: file.originalname || "",
        mimetype: file.mimetype || "",
        size: file.size || 0,
        uploadedAt: new Date().toISOString(),
      }));
      newAttachments = [...newAttachments, ...uploaded];
      hasAttachmentChange = true;
    }

    const attachmentsJson = JSON.stringify(newAttachments);
    if (hasAttachmentChange) hasAnyChange = true;

    let currentGatepass = currentTransport.gatepass || [];
    if (typeof currentGatepass === "string") {
      currentGatepass = JSON.parse(currentGatepass) || [];
    }

    let newGatepass = currentGatepass;

    if (updates.gatepass_existing) {
      newGatepass = JSON.parse(updates.gatepass_existing) || [];
      hasGatepassChange = true;
    }

    if (files.gatepass?.length > 0) {
      const uploaded = files.gatepass.map((file) => ({
        url: file.path || file.secure_url || file.location || "",
        public_id: file.filename || file.public_id || "",
        originalname: file.originalname || "",
        mimetype: file.mimetype || "",
        size: file.size || 0,
        uploadedAt: new Date().toISOString(),
      }));
      newGatepass = [...newGatepass, ...uploaded];
      hasGatepassChange = true;
    }

    const gatepassJson = JSON.stringify(newGatepass);
    if (hasGatepassChange) hasAnyChange = true;

    const receiverIdMap = {};

    const existingReceiversRes = await client.query(
      `SELECT id FROM receivers WHERE order_id = $1 ORDER BY id`,
      [id],
    );
    const existingReceiverIds = existingReceiversRes.rows.map((r) => r.id);

    for (let i = 0; i < incomingReceivers.length; i++) {
      const rec = incomingReceivers[i];

      const name = rec.receiver_name || rec.sender_name || "";
      const contact = rec.receiver_contact || rec.sender_contact || "";
      const address = rec.receiver_address || rec.sender_address || "";
      const email = rec.receiver_email || rec.sender_email || "";
      const marks = rec.receiver_marks_and_number || rec.marks_and_number || "";
      const eta = rec.eta ? normalizeDate(rec.eta) : null;
      const etd = rec.etd ? normalizeDate(rec.etd) : null;
      const status = rec.status || "Order Created";
      const remarks = rec.remarks || "";
      const fullPartial = rec.full_partial || "Full";
      const qtyDelivered = rec.qty_delivered || null;
      const shippingLine = rec.shipping_line || "";

      let containersJson = "[]";
      if (rec.containers) containersJson = JSON.stringify(rec.containers);

      const existingId = rec.id || existingReceiverIds[i] || null;
      let receiverId;

      if (existingId) {
        await client.query(
          `UPDATE receivers SET 
            receiver_name = $2, receiver_contact = $3, receiver_address = $4,
            receiver_email = $5, receiver_marks_and_number = $6,
            eta = $7, etd = $8, status = $9, remarks = $10,
            full_partial = $11, qty_delivered = $12, shipping_line = $13,
            containers = $14, updated_at = NOW()
           WHERE id = $1`,
          [
            existingId,
            name,
            contact,
            address,
            email,
            marks,
            eta,
            etd,
            status,
            remarks,
            fullPartial,
            qtyDelivered,
            shippingLine,
            containersJson,
          ],
        );
        receiverId = existingId;
      } else {
        const insertRes = await client.query(
          `INSERT INTO receivers (order_id, receiver_name, receiver_contact, receiver_address, 
            receiver_email, receiver_marks_and_number, eta, etd, status, remarks, 
            full_partial, qty_delivered, shipping_line, containers, created_at, updated_at)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,NOW(),NOW()) RETURNING id`,
          [
            id,
            name,
            contact,
            address,
            email,
            marks,
            eta,
            etd,
            status,
            remarks,
            fullPartial,
            qtyDelivered,
            shippingLine,
            containersJson,
          ],
        );
        receiverId = insertRes.rows[0].id;
      }

      receiverIdMap[String(i)] = receiverId;

      if (etd) {
        await client.query(
          `
          UPDATE order_tracking
             SET etd = $1
           WHERE id = (
             SELECT id
             FROM order_tracking
             WHERE order_id = $2
               AND receiver_id = $3
             ORDER BY created_time DESC
             LIMIT 1
           )
        `,
          [etd, id, receiverId],
        );
      }

      const itemsForThisReceiver = incomingOrderItems.filter(
        (item) =>
          String(item.receiver_index ?? item.receiverIndex) === String(i),
      );

      for (const item of itemsForThisReceiver) {
        const containerDetailsJson = item.container_details
          ? JSON.stringify(item.container_details)
          : "[]";

        const itemRef =
          item.item_ref ||
          `ITEM-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
        const existingItemId = item.existing_id || null;

        if (existingItemId) {
          await client.query(
            `UPDATE order_items SET receiver_id = $2, total_number = $3, weight = $4, 
              total_weight = $5, container_details = $6, category = $7, subcategory = $8, 
              type = $9, pickup_location = $10, delivery_address = $11, 
              shipping_line = $12, item_ref = $13, updated_at = NOW()
             WHERE id = $1`,
            [
              existingItemId,
              receiverId,
              item.total_number || null,
              item.weight || null,
              item.total_weight || null,
              containerDetailsJson,
              item.category || null,
              item.subcategory || null,
              item.type || null,
              item.pickup_location || null,
              item.delivery_address || null,
              item.shipping_line || null,
              itemRef,
            ],
          );
        } else {
          await client.query(
            `INSERT INTO order_items (order_id, receiver_id, total_number, weight, total_weight,
              container_details, category, subcategory, type, pickup_location, 
              delivery_address, shipping_line, item_ref, created_at, updated_at)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,NOW(),NOW())`,
            [
              id,
              receiverId,
              item.total_number || null,
              item.weight || null,
              item.total_weight || null,
              containerDetailsJson,
              item.category || null,
              item.subcategory || null,
              item.type || null,
              item.pickup_location || null,
              item.delivery_address || null,
              item.shipping_line || null,
              itemRef,
            ],
          );
        }
      }
    }

    {
      const senderName =
        updates.sender_name ?? currentSender.sender_name ?? null;
      const senderContact =
        updates.sender_contact ?? currentSender.sender_contact ?? null;
      const senderAddress =
        updates.sender_address ?? currentSender.sender_address ?? null;
      const senderEmail =
        updates.sender_email ?? currentSender.sender_email ?? null;
      const senderRef = updates.sender_ref ?? currentSender.sender_ref ?? null;
      const senderRemarks =
        updates.sender_remarks ?? currentSender.sender_remarks ?? null;
      const selectedSenderOwner =
        updates.selected_sender_owner ??
        currentSender.selected_sender_owner ??
        "individual";
      const senderType =
        updates.sender_type_owner ?? currentSender.sender_type ?? "individual";

      const senderFieldsPresent = [
        "sender_name",
        "sender_contact",
        "sender_address",
        "sender_email",
        "sender_ref",
        "sender_remarks",
        "selected_sender_owner",
      ].some((k) => updates[k] !== undefined);

      if (senderFieldsPresent || currentSenderRes.rowCount > 0) {
        if (currentSenderRes.rowCount > 0) {
          await client.query(
            `UPDATE senders SET
               sender_name = $2, sender_contact = $3, sender_address = $4,
               sender_email = $5, sender_ref = $6, sender_remarks = $7,
               selected_sender_owner = $8
             WHERE id = $1`,
            [
              currentSender.id,
              senderName,
              senderContact,
              senderAddress,
              senderEmail,
              senderRef,
              senderRemarks,
              selectedSenderOwner,
            ],
          );
        } else {
          await client.query(
            `INSERT INTO senders (
               order_id, sender_name, sender_contact, sender_address,
               sender_email, sender_ref, sender_remarks, selected_sender_owner
             ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
            [
              id,
              senderName,
              senderContact,
              senderAddress,
              senderEmail,
              senderRef,
              senderRemarks,
              selectedSenderOwner,
            ],
          );
        }
        hasAnyChange = true;
      }
    }

    if (incomingDropOffs.length > 0) {
      hasAnyChange = true;

      await client.query(`DELETE FROM drop_off_details WHERE order_id = $1`, [
        id,
      ]);

      for (const drop of incomingDropOffs) {
        const idx = String(drop.receiver_index ?? drop.receiverIndex);
        const realReceiverId = receiverIdMap[idx];

        if (!realReceiverId) {
          console.warn(`[DropOff] Skipping - no receiver for index ${idx}`);
          continue;
        }

        const dropDate =
          drop.drop_date || drop.dropDate
            ? normalizeDate(drop.drop_date || drop.dropDate)
            : null;

        await client.query(
          `INSERT INTO drop_off_details (
            order_id, receiver_id, drop_method, dropoff_name,
            drop_off_cnic, drop_off_mobile, plate_no, drop_date,
            created_at, updated_at
          ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW(),NOW())`,
          [
            id,
            realReceiverId,
            drop.drop_method || null,
            drop.dropoff_name || null,
            drop.drop_off_cnic || null,
            drop.drop_off_mobile || null,
            drop.plate_no || null,
            dropDate,
          ],
        );
      }
    }

    const transportUpdateQuery = `
      INSERT INTO transport_details (order_id, transport_type, collection_scope, 
        collection_method, third_party_transport, driver_name, driver_contact, 
        driver_nic, driver_pickup_location, truck_number, client_receiver_name, 
        client_receiver_id, client_receiver_mobile, delivery_date, gatepass,
        created_at, updated_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, NOW(), NOW())
      ON CONFLICT (order_id) DO UPDATE SET
        transport_type = EXCLUDED.transport_type,
        collection_scope = EXCLUDED.collection_scope,
        collection_method = EXCLUDED.collection_method,
        third_party_transport = EXCLUDED.third_party_transport,
        driver_name = EXCLUDED.driver_name,
        driver_contact = EXCLUDED.driver_contact,
        driver_nic = EXCLUDED.driver_nic,
        driver_pickup_location = EXCLUDED.driver_pickup_location,
        truck_number = EXCLUDED.truck_number,
        client_receiver_name = EXCLUDED.client_receiver_name,
        client_receiver_id = EXCLUDED.client_receiver_id,
        client_receiver_mobile = EXCLUDED.client_receiver_mobile,
        delivery_date = EXCLUDED.delivery_date,
        gatepass = EXCLUDED.gatepass,
        updated_at = NOW()
    `;

    await client.query(transportUpdateQuery, [
      id,
      updates.transport_type || currentTransport.transport_type || null,
      updates.collection_scope ||
        currentTransport.collection_scope ||
        "Partial",
      updates.collection_method || currentTransport.collection_method || null,
      updates.third_party_transport ||
        currentTransport.third_party_transport ||
        null,
      updates.driver_name || currentTransport.driver_name || null,
      updates.driver_contact || currentTransport.driver_contact || null,
      updates.driver_nic || currentTransport.driver_nic || null,
      updates.driver_pickup_location ||
        currentTransport.driver_pickup_location ||
        null,
      updates.truck_number || currentTransport.truck_number || null,
      updates.client_receiver_name ||
        currentTransport.client_receiver_name ||
        null,
      updates.client_receiver_id || currentTransport.client_receiver_id || null,
      updates.client_receiver_mobile ||
        currentTransport.client_receiver_mobile ||
        null,
      updates.delivery_date
        ? normalizeDate(updates.delivery_date)
        : currentTransport.delivery_date,
      gatepassJson,
    ]);

    hasAnyChange = true;

    const updatedFields = {
      booking_ref: updates.booking_ref ?? currentOrder.booking_ref,
      status: updates.status ?? currentOrder.status,
      rgl_booking_number:
        updates.rgl_booking_number ?? currentOrder.rgl_booking_number,
      place_of_loading:
        updates.place_of_loading ?? currentOrder.place_of_loading,
      point_of_origin: updates.point_of_origin ?? currentOrder.point_of_origin,
      final_destination:
        updates.final_destination ?? currentOrder.final_destination,
      place_of_delivery:
        updates.place_of_delivery ?? currentOrder.place_of_delivery,
      order_remarks: updates.order_remarks ?? currentOrder.order_remarks,
      eta: updates.eta ? normalizeDate(updates.eta) : currentOrder.eta,
      etd: updates.etd ? normalizeDate(updates.etd) : currentOrder.etd,
      attachments: attachmentsJson,
    };

    const ordersSet = [];
    const ordersValues = [];
    let paramIndex = 1;

    Object.entries(updatedFields).forEach(([key, val]) => {
      if (val !== undefined && val !== null) {
        ordersSet.push(`${key} = $${paramIndex}`);
        ordersValues.push(val);
        paramIndex++;
      }
    });

    if (ordersSet.length > 0 || hasAnyChange) {
      ordersSet.push("updated_at = NOW()");
      ordersValues.push(id);

      await client.query(
        `UPDATE orders SET ${ordersSet.join(", ")} WHERE id = $${paramIndex} RETURNING *`,
        ordersValues,
      );
    }

    await client.query("COMMIT");

    const updatedOrder = await client.query(
      "SELECT * FROM orders WHERE id = $1",
      [id],
    );
    const updatedSender = await client.query(
      "SELECT * FROM senders WHERE order_id = $1",
      [id],
    );
    const updatedReceivers = await client.query(
      `SELECT *, receiver_marks_and_number AS "marksAndNumber"
       FROM receivers WHERE order_id = $1 ORDER BY id`,
      [id],
    );

    res.status(200).json({
      message: "Order updated successfully",
      order: updatedOrder.rows[0],
      sender: updatedSender.rows[0] || null,
      receivers: updatedReceivers.rows.map((r) => ({
        ...r,
        eta: r.eta ? String(r.eta).split("T")[0] : "",
        etd: r.etd ? String(r.etd).split("T")[0] : "",
      })),
    });
  } catch (error) {
    console.error("Error updating order:", error);
    if (client) await client.query("ROLLBACK");
    return res.status(500).json({
      error: "Failed to update order",
      message: error.message,
      detail: error.detail || null,
    });
  } finally {
    if (client) client.release();
  }
}

export const getOrders = async (req, res) => {
  const client = await pool.connect();

  try {
    const page = Math.max(1, Number(req.query.page) || 1);
    const limit = Math.min(100, Math.max(1, Number(req.query.limit) || 10));
    const offset = (page - 1) * limit;

    const { search, status } = req.query;

    const params = [];
    const where = [];

    if (search) {
      params.push(`%${search.trim()}%`);

      where.push(`
        (
          o.booking_ref ILIKE $${params.length}
          OR o.rgl_booking_number ILIKE $${params.length}
          OR s.sender_name ILIKE $${params.length}
          OR EXISTS (
            SELECT 1
            FROM receivers r
            WHERE r.order_id = o.id
            AND r.receiver_name ILIKE $${params.length}
          )
        )
      `);
    }

    if (status) {
      params.push(status);

      where.push(`
        EXISTS (
          SELECT 1
          FROM receivers r
          WHERE r.order_id = o.id
          AND r.status = $${params.length}
        )
      `);
    }

    const whereClause = where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";

    const query = `
      SELECT
        o.id,
        o.booking_ref,
        o.rgl_booking_number,
        o.status,
        o.place_of_loading,
        o.place_of_delivery,
        o.created_at,

        s.sender_name,

        COALESCE(receiver_data.receivers, '[]'::jsonb) AS receivers,

        COALESCE(order_totals.total_items, 0) AS total_items,
        COALESCE(order_totals.total_weight, 0) AS total_weight

      FROM orders o

      LEFT JOIN senders s
        ON s.order_id = o.id

      LEFT JOIN LATERAL (
        SELECT
          jsonb_agg(
            jsonb_build_object(
              'id', r.id,
              'receiverName', r.receiver_name,
              'status', r.status,
              'eta', r.eta,

              'shippingdetails',
              COALESCE(
                (
                  SELECT jsonb_agg(
                    jsonb_build_object(
                      'id', oi.id,
                      'category', oi.category,
                      'subcategory', oi.subcategory,
                      'type', oi.type,
                      'weight', COALESCE(oi.weight,0),
                      'totalNumber', COALESCE(oi.total_number,0),
                      'itemRef', oi.item_ref,
                      'status', oi.consignment_status,

                      'trackingEta', ot.eta,
                      'trackingStatus', ot.status,

                      'containerDetails',
                      COALESCE(
                        (
                          SELECT jsonb_agg(
                            jsonb_build_object(
                              'container',
                              jsonb_build_object(
                                'cid', cm.cid,
                                'container_number', cm.container_number
                              ),

                              'status',
                              cm.status,

                              'assign_total_box',
                              COALESCE(oi.assigned_boxes::text, '0'),

                              'assign_weight',
                              COALESCE(oi.assigned_weight_kg::text, '0'),

                              'remaining_items',
                              COALESCE((COALESCE(oi.total_number,0) - COALESCE(oi.assigned_boxes,0))::text, '0'),

                              'total_number',
                              COALESCE(oi.total_number::int, 0)
                            )
                          )
                          FROM receivers r2
                          CROSS JOIN LATERAL jsonb_array_elements_text(
                            COALESCE(r2.containers, '[]'::jsonb)
                          ) AS container_num

                          JOIN container_master cm
                            ON cm.container_number = container_num

                          WHERE r2.id = oi.receiver_id
                        ),
                        '[]'::jsonb
                      )
                    )
                    ORDER BY oi.id
                  )
                  FROM order_items oi
                  LEFT JOIN LATERAL (
                    SELECT ot.eta, ot.status
                    FROM order_tracking ot
                    WHERE ot.receiver_id = r.id
                      AND ot.item_ref = oi.item_ref
                    ORDER BY ot.id DESC
                    LIMIT 1
                  ) ot ON true
                  WHERE oi.receiver_id = r.id
                ),
                '[]'::jsonb
              )
            )
            ORDER BY r.id
          ) AS receivers

        FROM receivers r

        WHERE r.order_id = o.id

      ) receiver_data ON true

      LEFT JOIN LATERAL (
        SELECT
          COALESCE(SUM(oi.total_number),0) AS total_items,
          COALESCE(SUM(oi.weight),0) AS total_weight
        FROM receivers r
        JOIN order_items oi ON oi.receiver_id = r.id
        WHERE r.order_id = o.id
      ) order_totals ON true

      ${whereClause}

      ORDER BY o.created_at DESC

      LIMIT $${params.length + 1}
      OFFSET $${params.length + 2}
    `;

    const countQuery = `
      SELECT COUNT(*) AS total
      FROM orders o
      LEFT JOIN senders s
        ON s.order_id = o.id
      ${whereClause}
    `;

    const [orders, count] = await Promise.all([
      client.query(query, [...params, limit, offset]),
      client.query(countQuery, params),
    ]);

    const total = Number(count.rows[0].total);

    return res.status(200).json({
      success: true,
      data: orders.rows,
      pagination: {
        page,
        limit,
        total,
        totalPages: Math.ceil(total / limit),
      },
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({
      success: false,
      message: "Failed to fetch orders",
    });
  } finally {
    client.release();
  }
};

export async function getOrdersConsignments(req, res) {
  if (!req.user) {
    return res
      .status(401)
      .json({ success: false, message: "Authentication required" });
  }

  const client = await pool.connect();

  try {
    const safePage = Math.max(1, parseInt(req.query.page || "1", 10));
    const safeLimit = Math.max(
      1,
      Math.min(100, parseInt(req.query.limit || "10", 10)),
    );
    const safeOffset = (safePage - 1) * safeLimit;

    const { status, booking_ref, container_id, consignment_id, pol, pod } =
      req.query;

    let whereClause = "WHERE 1=1";
    let params = [];

    if (status) {
      params.push(status);
      whereClause += ` AND o.status = $${params.length}`;
    }

    if (booking_ref) {
      params.push(`%${booking_ref}%`);
      whereClause += ` AND o.booking_ref ILIKE $${params.length}`;
    }

    if (container_id) {
      const validIds = container_id
        .split(",")
        .map((id) => parseInt(id.trim()))
        .filter((id) => !isNaN(id));

      if (validIds.length === 0) {
        return res.json({
          success: true,
          data: [],
          pagination: {
            page: safePage,
            limit: safeLimit,
            total: 0,
            totalPages: 0,
          },
        });
      }

      const containerResult = await client.query(
        "SELECT cid, container_number FROM container_master WHERE cid = ANY($1::int[])",
        [validIds],
      );

      if (containerResult.rowCount === 0) {
        return res.json({
          success: true,
          data: [],
          pagination: {
            page: safePage,
            limit: safeLimit,
            total: 0,
            totalPages: 0,
          },
        });
      }

      const containerNumbers = containerResult.rows.map(
        (r) => r.container_number,
      );

      const otConditions = validIds
        .map((id) => {
          params.push(id);
          return `ot.container_id = $${params.length}`;
        })
        .join(" OR ");

      const cmConditions = containerNumbers
        .map((num) => {
          params.push(`%${num}%`);
          return `cm.container_number ILIKE $${params.length}`;
        })
        .join(" OR ");

      const receiverContainerConditions = containerNumbers
        .map((num) => {
          params.push(`%${num}%`);
          return `EXISTS (
          SELECT 1 FROM jsonb_array_elements_text(r.containers) AS cont
          WHERE cont ILIKE $${params.length}
        )`;
        })
        .join(" OR ");

      whereClause += ` AND (
        (${otConditions}) OR
        (${cmConditions}) OR
        EXISTS (
          SELECT 1 FROM receivers r
          WHERE r.order_id = o.id
            AND r.containers IS NOT NULL
            AND jsonb_typeof(r.containers) = 'array'
            AND (${receiverContainerConditions})
        )
      )`;

      if (consignment_id) {
        if (pol) {
          params.push(pol.trim());
          whereClause += ` AND LOWER(TRIM(o.place_of_loading)) = LOWER(TRIM($${params.length}))`;
        }

        if (pod) {
          params.push(pod.trim());
          whereClause += ` AND LOWER(TRIM(o.place_of_delivery)) = LOWER(TRIM($${params.length}))`;
        }

        params.push(parseInt(consignment_id, 10));
        const consignmentParam = params.length;

        whereClause += `
          AND EXISTS (
            SELECT 1
            FROM consignments c
            WHERE c.id = $${consignmentParam}
              AND c.orders @> to_jsonb(o.id)
          )
        `;
      } else {
        if (pol) {
          params.push(String(pol.trim()));
          whereClause += ` AND o.place_of_loading = $${params.length}`;
        }
        if (pod) {
          params.push(String(pod.trim()));
          whereClause += ` AND o.place_of_delivery = $${params.length}`;
        }
        params.push(validIds);
        const chContainerParam = params.length;
        whereClause += ` AND EXISTS (
            SELECT 1
            FROM container_assignment_history ch
            WHERE ch.order_id = o.id
              AND ch.cid = ANY($${chContainerParam}::int[])
              AND COALESCE(ch.status, 'Ready for Loading') IN (
                'Ready for Loading',
                'Loaded'
              )
          )
        `;
      }
    }

    const containerDetailsSub = `
      COALESCE((
        SELECT json_agg(json_build_object(
          'status', COALESCE(cs.derived_status, 'Created'),
          'container', json_build_object('cid', (cd_obj->'container'->>'cid')::int, 'container_number', COALESCE(cd_obj->'container'->>'container_number', 'Unknown')),
          'assign_weight',   COALESCE(cd_obj->>'assign_weight',   '0'),
          'assign_total_box',COALESCE(cd_obj->>'assign_total_box','0')
        ) ORDER BY COALESCE(cd_obj->'container'->>'container_number', ''))
        FROM jsonb_array_elements(COALESCE(oi.container_details, '[]'::jsonb)) cd_obj
        LEFT JOIN LATERAL (
          SELECT availability AS derived_status
          FROM container_status
          WHERE cid = (cd_obj->'container'->>'cid')::int
          ORDER BY sid DESC NULLS LAST LIMIT 1
        ) cs ON true
        WHERE (cd_obj->'container'->>'cid') ~ '^[0-9]+$'
              ), '[]'::json)
    `;

    const shippingDetailsSub = `
      (SELECT COALESCE(json_agg(json_build_object(
        'id', oi.id,
        'category', COALESCE(oi.category, ''),
        'subcategory', COALESCE(oi.subcategory, ''),
        'type', COALESCE(oi.type, ''),
        'itemRef', COALESCE(oi.item_ref, ''),
        'status', COALESCE(ot_item.status, 'Created'),
        'totalNumber', COALESCE(oi.total_number, 0),
        'weight', COALESCE(oi.weight, 0),
        'containerDetails',${containerDetailsSub},
        'remainingItems',  COALESCE(oi.total_number, 0) - COALESCE((
            SELECT SUM((cd_obj->>'assign_total_box')::int)
            FROM jsonb_array_elements(COALESCE(oi.container_details, '[]'::jsonb)) cd_obj
          ), 0)
      ) ORDER BY oi.id), '[]'::json)
      FROM order_items oi
      LEFT JOIN LATERAL (
        SELECT ot.status
        FROM order_tracking ot
        WHERE ot.item_ref = oi.item_ref
        ORDER BY ot.created_time DESC
        LIMIT 1
      ) ot_item ON true
      WHERE oi.receiver_id = r.id)
    `;

    const receiversSub = `
      (SELECT COALESCE(json_agg(rf ORDER BY rf.id), '[]'::json) FROM (
        SELECT
        r.id,
        r.receiver_name  AS receivername,
        r.containers,
        ${shippingDetailsSub} AS shippingdetails,
          COALESCE((
            SELECT json_agg(json_build_object(
              'drop_method', dod.drop_method,
              'dropoff_name', dod.dropoff_name,
              'drop_off_mobile', dod.drop_off_mobile,
              'plate_no', dod.plate_no,
              'drop_date', TO_CHAR(dod.drop_date, 'YYYY-MM-DD')
            ) ORDER BY dod.id)
            FROM drop_off_details dod WHERE dod.receiver_id = r.id
          ), '[]'::json) AS drop_off_details
        FROM receivers r WHERE r.order_id = o.id AND r.id IS NOT NULL
      ) rf) AS receivers
    `;

    const joins = `
      LEFT JOIN senders s ON o.id = s.order_id
      LEFT JOIN LATERAL (
        SELECT ot2.container_id FROM order_tracking ot2
        WHERE ot2.order_id = o.id ORDER BY ot2.created_time DESC LIMIT 1
      ) ot ON true
      LEFT JOIN container_master cm ON ot.container_id = cm.cid
    `;

    const dataQuery = `
      SELECT
        o.id,
        o.booking_ref,
        o.rgl_booking_number,
        o.place_of_loading,
        o.place_of_delivery,
        s.sender_name,
        ${receiversSub}
      FROM orders o
      ${joins}
      ${whereClause}
      ORDER BY o.created_at DESC
      LIMIT $${params.length + 1} OFFSET $${params.length + 2}
    `;

    const countQuery = `
      SELECT COUNT(DISTINCT o.id) AS total
      FROM orders o
      ${joins}
      ${whereClause}
    `;

    params.push(safeLimit, safeOffset);

    const [result, countResult] = await Promise.all([
      client.query(dataQuery, params),
      client.query(countQuery, params.slice(0, -2)),
    ]);

    const total = parseInt(countResult.rows[0]?.total || 0);

    const data = result.rows.map((row) => {
      let receivers = row.receivers || [];
      if (typeof receivers === "string") {
        try {
          receivers = JSON.parse(receivers);
        } catch {
          receivers = [];
        }
      }
      return {
        id: row.id,
        booking_ref: row.booking_ref,
        rgl_booking_number: row.rgl_booking_number,
        place_of_loading: row.place_of_loading,
        place_of_delivery: row.place_of_delivery,
        sender_name: row.sender_name || null,
        receivers,
      };
    });

    return res.status(200).json({
      success: true,
      data,
      pagination: {
        page: safePage,
        limit: safeLimit,
        total,
        totalPages: total === 0 ? 0 : Math.ceil(total / safeLimit),
      },
    });
  } catch (err) {
    console.error("Error in getOrdersConsignments:", err);
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  } finally {
    client.release();
  }
}

export async function getOrderById(req, res) {
  let client;
  try {
    const { id } = req.params;

    const numericId = parseInt(id, 10);
    if (isNaN(numericId) || numericId <= 0) {
      return res.status(400).json({
        error: "Invalid order ID",
        details: "Order ID must be a positive integer",
        received: id,
      });
    }

    client = await pool.connect();

    const selectFields = [
      "o.*",
      "s.sender_name, s.sender_contact, s.sender_address, s.sender_email, s.sender_ref, s.sender_remarks, s.sender_type, s.selected_sender_owner",
      "t.transport_type, t.third_party_transport, t.driver_name, t.driver_contact, t.driver_nic",
      "t.driver_pickup_location, t.truck_number, t.drop_method, t.dropoff_name, t.drop_off_cnic",
      "t.drop_off_mobile, t.plate_no, t.drop_date, t.collection_method, t.collection_scope, t.qty_delivered",
      "t.client_receiver_name, t.client_receiver_id, t.client_receiver_mobile, TO_CHAR(t.delivery_date, 'YYYY-MM-DD') AS delivery_date",
      "t.gatepass",
    ].join(", ");

    const orderResult = await client.query(
      `
      SELECT ${selectFields}
      FROM orders o
      LEFT JOIN senders s ON o.id = s.order_id
      LEFT JOIN transport_details t ON o.id = t.order_id
      WHERE o.id = $1
    `,
      [numericId],
    );

    if (orderResult.rowCount === 0) {
      return res.status(404).json({ error: "Order not found" });
    }

    const orderRow = orderResult.rows[0];

    const containerDetailsSub = `
      COALESCE((
        SELECT json_agg(
          jsonb_build_object(
            'status',          COALESCE(cs.derived_status, 'Created'),
            'container',       jsonb_build_object(
              'cid',           (elem->'container'->>'cid')::int,
              'container_number', COALESCE(elem->'container'->>'container_number', '')
            ),
            'total_number',    COALESCE((elem->>'total_number')::int, 0),
            'assign_weight',   COALESCE(elem->>'assign_weight', '0'),
            'remaining_items', COALESCE((elem->>'remaining_items')::int, 0),
            'assign_total_box', COALESCE(elem->>'assign_total_box', '0')
          ) ORDER BY (elem->'container'->>'container_number')
        )
        FROM jsonb_array_elements(COALESCE(oi.container_details, '[]'::jsonb)) elem
        LEFT JOIN LATERAL (
          SELECT availability AS derived_status
          FROM container_status
          WHERE cid = (elem->'container'->>'cid')::int
          ORDER BY sid DESC NULLS LAST LIMIT 1
        ) cs ON true
        WHERE (elem->'container'->>'cid') ~ '^[0-9]+$'
      ), '[]'::json)
    `;

    const receiversQuery = `
      SELECT 
        r.id, r.order_id,
        r.receiver_name, r.receiver_contact, r.receiver_address, r.receiver_email,
        r.receiver_marks_and_number AS "marksAndNumber",
        COALESCE(r.total_number, 0)::int    AS total_number,
        COALESCE(r.total_weight, 0)::numeric AS total_weight,
        r.receiver_ref, r.remarks, r.containers,
        r.status,
        TO_CHAR(r.eta, 'YYYY-MM-DD') AS eta,
        TO_CHAR(r.etd, 'YYYY-MM-DD') AS etd,
        r.shipping_line,
        r.consignment_vessel, r.consignment_number,
        r.consignment_marks, r.consignment_voyage,
        r.full_partial,
        COALESCE(r.qty_delivered, 0)::int AS qty_delivered,
        sd_full.shippingdetails
      FROM receivers r
      LEFT JOIN LATERAL (
      SELECT json_agg(
        json_build_object(
          'id',                oi.id,
          'order_id',          oi.order_id,
          'category',          COALESCE(oi.category, ''),
          'subcategory',       COALESCE(oi.subcategory, ''),
          'type',              COALESCE(oi.type, ''),
          'pickupLocation',    COALESCE(oi.pickup_location, ''),
          'deliveryAddress',   COALESCE(oi.delivery_address, ''),
          'totalNumber',       COALESCE(oi.total_number, 0)::int,
          'weight',            COALESCE(oi.weight, 0)::numeric,
          'totalWeight',       COALESCE(oi.total_weight, 0)::numeric,
          'itemRef',           COALESCE(oi.item_ref, ''),

          -- Get latest values from order_tracking
          'status',            COALESCE(ot.status, oi.consignment_status, ''),
          'eta',               TO_CHAR(ot.eta, 'YYYY-MM-DD'),
          'etd',               TO_CHAR(ot.etd, 'YYYY-MM-DD'),

          'shippingLine',      COALESCE(oi.shipping_line, ''),
          'containerDetails',  ${containerDetailsSub},
          'remainingItems',    GREATEST(
             0,
            COALESCE(oi.total_number, 0)::int -
            COALESCE((
              SELECT SUM((cd->>'assign_total_box')::int)
              FROM jsonb_array_elements(COALESCE(oi.container_details, '[]'::jsonb)) cd
            ), 0)
          )::int
        )
        ORDER BY oi.id
      ) AS shippingdetails
      FROM order_items oi

      LEFT JOIN LATERAL (
        SELECT status, eta, etd
        FROM order_tracking t
        WHERE t.receiver_id = r.id
          AND t.item_ref = oi.item_ref
        ORDER BY t.created_time DESC
        LIMIT 1
      ) ot ON TRUE

      WHERE oi.receiver_id = r.id
    ) sd_full ON TRUE
      WHERE r.order_id = $1
      ORDER BY r.id
    `;

    const receiversResult = await client.query(receiversQuery, [numericId]);

    let receivers = receiversResult.rows.map((row) => {
      return {
        ...row,
        marksAndNumber: row.marksAndNumber || "",
        receiverMarksNumber: row.marksAndNumber || "",
        shippingDetails: row.shippingdetails || [],
        containers: (() => {
          try {
            return typeof row.containers === "string"
              ? JSON.parse(row.containers)
              : row.containers || [];
          } catch (e) {
            return [];
          }
        })(),
        eta: row.eta || "",
        etd: row.etd || "",
      };
    });

    const usedContainersRes = await client.query(
      `
      SELECT DISTINCT cm.cid, cm.container_number
      FROM order_items oi
      CROSS JOIN jsonb_array_elements(COALESCE(oi.container_details, '[]'::jsonb)) elem
      JOIN container_master cm ON cm.cid = (elem->'container'->>'cid')::int
      WHERE oi.order_id = $1
    `,
      [numericId],
    );

    const containersMap = new Map(
      usedContainersRes.rows.map((c) => [c.cid, c.container_number]),
    );

    receivers.forEach((receiver) => {
      receiver.shippingDetails?.forEach((sd) => {
        if (Array.isArray(sd.containerDetails)) {
          sd.containerDetails.forEach((cd) => {
            const cid = cd.container?.cid;
            if (typeof cid === "number" && containersMap.has(cid)) {
              cd.container = {
                cid,
                container_number: containersMap.get(cid) || "",
              };
            }
          });
        }
      });
    });

    // Drop-off details per receiver
    const dropOffResult = await client.query(
      `
      SELECT 
        receiver_id,
        json_agg(
          json_build_object(
            'drop_method',     drop_method,
            'dropoff_name',    dropoff_name,
            'drop_off_cnic',   drop_off_cnic,
            'drop_off_mobile', drop_off_mobile,
            'plate_no',        plate_no,
            'drop_date',       TO_CHAR(drop_date, 'YYYY-MM-DD')
          ) ORDER BY id
        ) AS drop_off_details
      FROM drop_off_details
      WHERE order_id = $1
      GROUP BY receiver_id
    `,
      [numericId],
    );

    const dropOffMap = new Map(
      dropOffResult.rows.map((r) => [r.receiver_id, r.drop_off_details || []]),
    );

    receivers = receivers.map((r) => ({
      ...r,
      drop_off_details: dropOffMap.get(r.id) || [],
    }));

    // Assignment history
    const historyResult = await client.query(
      `
      SELECT h.*, cm.container_number
      FROM container_assignment_history h
      LEFT JOIN container_master cm ON h.cid = cm.cid
      WHERE h.order_id = $1
      ORDER BY h.id DESC
    `,
      [numericId],
    );

    // Parse JSONB fields safely
    let parsedAttachments = [];
    try {
      parsedAttachments =
        typeof orderRow.attachments === "string"
          ? JSON.parse(orderRow.attachments)
          : orderRow.attachments || [];
    } catch (e) {
      // ignore malformed attachments
    }

    let parsedGatepass = [];
    try {
      parsedGatepass =
        typeof orderRow.gatepass === "string"
          ? JSON.parse(orderRow.gatepass)
          : orderRow.gatepass || [];
    } catch (e) {
      // ignore malformed gatepass
    }

    const formattedOrderRow = {
      ...orderRow,
      eta: orderRow.eta ? String(orderRow.eta).split("T")[0] : "",
      etd: orderRow.etd ? String(orderRow.etd).split("T")[0] : "",
      drop_date: orderRow.drop_date
        ? String(orderRow.drop_date).split("T")[0]
        : "",
      deliveryDate: orderRow.delivery_date
        ? String(orderRow.delivery_date).split("T")[0]
        : "",
    };

    let overallStatus = "Created";
    if (receivers.length > 0) {
      const receiverStatuses = receivers.map((r) => r.status || "Created");
      if (receiverStatuses.includes("Cancelled")) {
        overallStatus = "Cancelled";
      } else {
        const statusOrder = {
          Created: 0,
          "In Process": 1,
          "Ready for Loading": 2,
          "Loaded Into Container": 3,
          Delivered: 4,
        };
        const maxIdx = Math.max(
          ...receiverStatuses.map((s) => statusOrder[s] || 0),
        );
        overallStatus =
          Object.keys(statusOrder).find((k) => statusOrder[k] === maxIdx) ||
          "Created";
      }
    }

    let overallEta = null;
    const withContainers = receivers.filter((r) =>
      r.shippingDetails?.some((sd) => sd.containerDetails?.length > 0),
    );

    if (withContainers.length > 0) {
      const etas = withContainers
        .map((r) => r.eta)
        .filter((eta) => eta && eta.trim() !== "")
        .map((eta) => {
          const d = new Date(eta.includes("T") ? eta : `${eta}T00:00:00.000Z`);
          return isNaN(d.getTime()) ? null : d.getTime();
        })
        .filter(Boolean)
        .sort((a, b) => a - b);

      overallEta = etas.length
        ? new Date(etas[0]).toISOString().split("T")[0]
        : null;
    }

    res.json({
      ...formattedOrderRow,
      eta: overallEta,
      overall_status: overallStatus,
      status: overallStatus,
      attachments: parsedAttachments,
      gatepass: parsedGatepass,
      collection_scope: orderRow.collection_scope,
      qty_delivered: orderRow.qty_delivered,
      receivers,
      assignmentHistory: historyResult.rows,
    });
  } catch (err) {
    console.error("Error in getOrderById:", err);
    res.status(500).json({
      error: "Failed to fetch order",
      details: err.message,
      stack: process.env.NODE_ENV === "development" ? err.stack : undefined,
    });
  } finally {
    if (client) client.release();
  }
}

async function withTransaction(operation) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const result = await operation(client);
    await client.query("COMMIT");
    return result;
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

function getStatusMessage(status) {
  const normalized = (status || "").trim().toLowerCase();

  const messages = {
    "ready for loading": "Your shipment is being prepared for loading.",
    "loaded into container": "Your items have been loaded into the container.",
    "shipment processing": "Your shipment is currently being processed.",
    "shipment in transit": "Your shipment is on its way to the destination.",
    "under processing":
      "Your shipment is under processing at the destination facility.",
    "arrived at sort facility":
      "Your shipment has arrived at our sorting facility.",
    "ready for delivery": "Your shipment is ready for final delivery.",
    "shipment delivered": "Your shipment has been successfully delivered.",
  };

  // Return the matching message or a safe generic fallback
  return (
    messages[normalized] ||
    "The status of your shipment has been updated. We’ll keep you informed as it progresses."
  );
}

function safeParseJsonArrayForMultiple(value) {
  if (!value) return [];
  if (Array.isArray(value)) return value;
  if (typeof value === "string" && value.trim()) {
    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? parsed : [];
    } catch (err) {
      console.error(
        `Invalid JSON in container_details:`,
        value.substring(0, 200) + "...",
        "→ error:",
        err.message,
      );
      return [];
    }
  }
  console.warn(`Unexpected type for container_details field:`, typeof value);
  return [];
}

export async function assignContainersBatch(req, res) {
  let client;
  try {
    client = await pool.connect();
    await client.query("DISCARD PLANS");
    await client.query("BEGIN");

    const { assignments = [] } = req.body;

    if (!Array.isArray(assignments) || assignments.length === 0) {
      return res.status(400).json({
        success: false,
        message: "assignments must be a non-empty array",
      });
    }

    const results = [];
    const skipped = [];
    const updatedReceivers = new Set();
    const updatedOrders = new Set();

    const currentUserEmail = req.user?.email || "unknown-user";

    for (const ass of assignments) {
      const orderIdNum = Number(ass.orderId);
      const receiverIdNum = Number(ass.receiverId);
      const detailIdxNum = Number(ass.detailIndex ?? "0");
      const containerIdNum = Number(ass.containerId);
      const qtyNum = Number(ass.qty);

      if (
        isNaN(orderIdNum) ||
        orderIdNum <= 0 ||
        isNaN(receiverIdNum) ||
        receiverIdNum <= 0 ||
        isNaN(containerIdNum) ||
        containerIdNum <= 0 ||
        isNaN(qtyNum) ||
        qtyNum <= 0 ||
        isNaN(detailIdxNum) ||
        detailIdxNum < 0
      ) {
        skipped.push({
          ...ass,
          reason: "Invalid numeric values in IDs or qty",
        });
        continue;
      }

      const contRes = await client.query(
        `
        SELECT 
          cm.cid, 
          cm.container_number,
          cs.availability,
          CASE 
            WHEN cs.availability = 'Cleared' THEN 'Cleared'
            WHEN cs.availability IN ('Available', 'Assigned to Job') THEN 'Available'
            ELSE cs.availability
          END as derived_status
        FROM container_master cm
        LEFT JOIN LATERAL (
          SELECT availability
          FROM container_status
          WHERE cid = cm.cid
          ORDER BY sid DESC NULLS LAST
          LIMIT 1
        ) cs ON true
        WHERE cm.cid = $1 
           OR cm.container_number = $2
        `,
        [containerIdNum, String(ass.containerId)],
      );

      if (contRes.rowCount === 0) {
        skipped.push({ ...ass, reason: "container not found" });
        continue;
      }

      const container = contRes.rows[0];
      if (
        !["Available", "Assigned to Job", "Ready for Loading"].includes(
          container.derived_status,
        )
      ) {
        skipped.push({
          ...ass,
          reason: `container not available (status: ${container.derived_status})`,
        });
        continue;
      }

      const cid = container.cid;
      const contNumber = container.container_number;

      const receiverRes = await client.query(
        `
        SELECT
          id,
          receiver_name,
          qty_delivered,
          total_number,
          total_weight,
          containers
        FROM receivers
        WHERE id = $1
          AND order_id = $2
        FOR UPDATE
        `,
        [receiverIdNum, orderIdNum],
      );

      if (receiverRes.rowCount === 0) {
        skipped.push({ ...ass, reason: "receiver not found in this order" });
        continue;
      }

      const receiver = receiverRes.rows[0];
      const remainingReceiverQty =
        Number(receiver.total_number) - Number(receiver.qty_delivered || 0);

      if (remainingReceiverQty <= 0) {
        skipped.push({ ...ass, reason: "receiver already fully delivered" });
        continue;
      }

      const itemsRes = await client.query(
        `
        SELECT
          id,
          item_ref,
          total_number,
          total_weight,
          assigned_boxes,
          assigned_weight_kg,
          container_details,
          weight
        FROM order_items
        WHERE receiver_id = $1
        ORDER BY id
        LIMIT 1 OFFSET $2
        FOR UPDATE
        `,
        [receiverIdNum, detailIdxNum],
      );

      if (itemsRes.rowCount === 0) {
        skipped.push({
          ...ass,
          reason: "no item found for this detail index",
        });
        continue;
      }

      const targetItem = itemsRes.rows[0];

      if (
        Number(targetItem.total_number) <=
        Number(targetItem.assigned_boxes || 0)
      ) {
        skipped.push({ ...ass, reason: "target item already fully assigned" });
        continue;
      }
      const targetItemId = targetItem.id;
      const itemRef = targetItem.item_ref;

      const currentAssignedBoxes = Number(targetItem.assigned_boxes || 0);
      const remainingBoxes =
        Number(targetItem.total_number) - currentAssignedBoxes;

      if (remainingBoxes <= 0) {
        skipped.push({ ...ass, reason: "target item already fully assigned" });
        continue;
      }

      const assignBoxes = Math.min(qtyNum, remainingBoxes);

      let assignWeight = 0;

      if (ass.assignedWeightKg != null) {
        assignWeight = Number(ass.assignedWeightKg);
        if (isNaN(assignWeight) || assignWeight < 0) {
          skipped.push({ ...ass, reason: "Invalid custom assignedWeightKg" });
          continue;
        }
      } else {
        let totalW = Number(targetItem.total_weight || 0);
        if (totalW <= 0)
          totalW = receiver ? Number(receiver.total_weight || 0) : 0;

        const alreadyAssignedWeight = Number(
          targetItem.assigned_weight_kg || 0,
        );
        const remainingWeight = totalW - alreadyAssignedWeight;
        const fraction = assignBoxes / (remainingBoxes || 1);
        assignWeight = Number((fraction * remainingWeight).toFixed(2));
      }

      if (assignBoxes <= 0) {
        skipped.push({ ...ass, reason: "no boxes remaining to assign" });
        continue;
      }

      let receiverContainers = [];
      try {
        receiverContainers = Array.isArray(receiver.containers)
          ? receiver.containers
          : JSON.parse(receiver.containers || "[]");
      } catch {
        receiverContainers = [];
      }

      const updatedContainers = [
        ...new Set([...receiverContainers, contNumber]),
      ];

      await client.query(
        `
        UPDATE receivers
        SET
          containers = $1::jsonb,
          updated_at = NOW()
        WHERE id = $2
        `,
        [JSON.stringify(updatedContainers), receiverIdNum],
      );

      const nextStatus = await moveReceiverToNextStatus(client, receiverIdNum);

      let trackingStatus = nextStatus?.order_status;
      if (!trackingStatus) {
        const currRes = await client.query(
          `SELECT status FROM receivers WHERE id = $1`,
          [receiverIdNum],
        );
        trackingStatus = currRes.rows[0]?.status || "Ready for Loading";
      }

      const { eta } = await calculateETA(client, trackingStatus);

      await createOrderTracking(client, {
        orderId: orderIdNum,
        receiverId: receiverIdNum,
        containerId: cid,
        status: trackingStatus,
        createdBy: currentUserEmail,
        itemRef,
        eta,
        etd: eta,
      });

      await client.query(
        `
        INSERT INTO container_assignment_history (
          cid, container_number, order_id, receiver_id, detail_id,
          assigned_qty, assigned_weight_kg, status, previous_status,
          action_type, created_by, updated_by, changed_by, notes, created_at, loaded_at
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,NOW(),NOW())
        `,
        [
          cid,
          contNumber,
          orderIdNum,
          receiverIdNum,
          targetItemId,
          assignBoxes,
          assignWeight,
          "Ready for Loading",
          "Created",
          "ASSIGN",
          currentUserEmail,
          currentUserEmail,
          currentUserEmail,
          `Assigned ${assignBoxes} boxes (${assignWeight} kg) via batch`,
        ],
      );

      await client.query(
        `UPDATE container_master SET status = 'Ready for Loading' WHERE cid = $1`,
        [cid],
      );

      let details =
        safeParseJsonArrayForMultiple(targetItem.container_details || "[]") ||
        [];

      let entry = details.find((e) => e?.container?.cid === cid);

      if (!entry) {
        entry = {
          status: "Ready for Loading",
          container: { cid, container_number: contNumber },
          total_number: String(targetItem.total_number || 0),
          assign_weight: "0.00",
          remaining_items: String(targetItem.total_number || 0),
          assign_total_box: "0",
        };
        details.push(entry);
      }

      entry.assign_total_box = String(
        Number(entry.assign_total_box || 0) + assignBoxes,
      );
      entry.assign_weight = (
        Number(entry.assign_weight || 0) + assignWeight
      ).toFixed(2);
      entry.remaining_items = String(
        Number(targetItem.total_number) - (currentAssignedBoxes + assignBoxes),
      );

      await client.query(
        `
        UPDATE order_items
        SET
          assigned_boxes = $1,
          assigned_weight_kg = $2,
          container_details = $3::jsonb,
          assigned_at = NOW(),
          updated_at = NOW()
        WHERE id = $4
        `,
        [
          currentAssignedBoxes + assignBoxes,
          Number(targetItem.assigned_weight_kg || 0) + assignWeight,
          JSON.stringify(details),
          targetItemId,
        ],
      );

      results.push({
        orderId: orderIdNum,
        receiverId: receiverIdNum,
        itemId: targetItemId,
        container: contNumber,
        assignedQty: assignBoxes,
        assignedWeightKg: assignWeight,
      });

      await client.query(
        `
        UPDATE receivers
        SET
          status = 'Ready for Loading',
          updated_at = NOW()
        WHERE order_id = $1
        `,
        [orderIdNum],
      );

      updatedOrders.add(orderIdNum);

      const recsInOrder = await client.query(
        "SELECT id FROM receivers WHERE order_id = $1",
        [orderIdNum],
      );
      recsInOrder.rows.forEach((r) => updatedReceivers.add(r.id));
    }

    if (results.length === 0) {
      await client.query("ROLLBACK");
      return res.status(400).json({
        success: false,
        message: "No valid assignments could be processed",
        skipped,
      });
    }

    for (const ordId of updatedOrders) {
      const orderTotal = results
        .filter((r) => r.orderId === ordId)
        .reduce((sum, r) => sum + r.assignedQty, 0);

      if (orderTotal > 0) {
        await client.query(
          `
          UPDATE orders
          SET total_assigned_qty = total_assigned_qty + $1,
              updated_at = NOW()
          WHERE id = $2
          `,
          [orderTotal, ordId],
        );
      }
    }

    await client.query("COMMIT");

    res.json({
      success: true,
      message: `Successfully processed ${results.length} assignments`,
      updatedReceivers: updatedReceivers.size,
      updatedOrders: updatedOrders.size,
      assigned: results,
      skipped: skipped.length > 0 ? skipped : undefined,
    });
  } catch (err) {
    if (client) await client.query("ROLLBACK");
    res.status(500).json({
      success: false,
      message: "Server error during batch assignment",
      details: err.message,
    });
  } finally {
    if (client) client.release();
  }
}

function safeParseJsonArray(val) {
  if (!val) return [];
  if (Array.isArray(val)) return val;

  try {
    const parsed = JSON.parse(val);
    if (Array.isArray(parsed)) return parsed;
    // If parsed value is a string, wrap in array
    if (typeof parsed === "string") return [parsed];
  } catch {
    // Fallback: treat as comma-separated string or single value
    return val
      .toString()
      .split(",")
      .map((v) => v.trim())
      .filter(Boolean);
  }

  return [];
}

export async function assignContainersToOrders(req, res) {
  let client;
  try {
    client = await pool.connect();
    await client.query("BEGIN");

    const { assignments } = req.body;
    const currentUserEmail = req.user?.email || "system-fallback";

    const changedBy = req.user?.id || req.user?.email || "system-fallback";
    if (
      !assignments ||
      typeof assignments !== "object" ||
      !Object.keys(assignments).length
    ) {
      return res
        .status(400)
        .json({ error: "Valid non-empty assignments object required" });
    }

    const orderIds = Object.keys(assignments).map(Number).filter(Boolean);
    if (!orderIds.length)
      return res.status(400).json({ error: "No valid order IDs" });

    const trackingData = [];

    for (const orderId of orderIds) {
      const orderAssign = assignments[orderId];
      let orderAssignedQty = 0;

      for (const recIdStr in orderAssign) {
        const recId = Number(recIdStr);
        const recAssign = orderAssign[recIdStr];

        const receiverRes = await client.query(
          `
          SELECT id, qty_delivered, containers, total_number
            FROM receivers
            WHERE id = $1 AND order_id = $2
          FOR UPDATE
        `,
          [recId, orderId],
        );

        if (!receiverRes.rowCount) {
          console.warn(`Receiver ${recId} not found in order ${orderId}`);
          continue;
        }

        const receiver = receiverRes.rows[0];
        let receiverContainers = safeParseJsonArray(receiver.containers);

        let receiverAssignedQty = 0;
        let receiverAssignedKg = 0;
        const newContainers = new Set();

        const trackingEntries = [];

        for (const idxStr in recAssign) {
          const assign = recAssign[idxStr];
          const itemId = Number(assign.orderItemId);
          const qty = Number(assign.qty || 0);
          const userWeight = parseFloat(assign.totalAssignedWeight || 0);
          const itemLoadingDate = assign.loadingDate || null;

          if (!itemId || qty <= 0) continue;
          let removeWeight = parseFloat(assign.totalAssignedWeight || 0);
          const itemRes = await client.query(
            `
              SELECT
                id,
                item_ref,
                assigned_boxes,
                assigned_weight_kg,
                container_details,
                total_number
              FROM order_items
              WHERE id = $1
              AND receiver_id = $2
              FOR UPDATE
            `,
            [itemId, recId],
          );

          if (!itemRes.rowCount) continue;
          const item = itemRes.rows[0];
          const itemRef = item.item_ref;

          let containerDetails = safeParseJsonArray(item.container_details);
          const currentBoxes = Number(item.assigned_boxes || 0);
          const currentKg = Number(item.assigned_weight_kg || 0);

          if (qty > item.total_number - currentBoxes) {
            throw new Error(
              `Cannot assign more than remaining on item ${itemId}`,
            );
          }

          const cids = (assign.containers || []).map(Number).filter(Boolean);
          if (!cids.length) continue;

          let assignedThisItemQty = 0;
          let assignedThisItemKg = 0;

          const qtyPer = Math.floor(qty / cids.length);
          const weightPer = Number((userWeight / cids.length).toFixed(2));

          let lastCid = null;

          for (let i = 0; i < cids.length; i++) {
            const cid = cids[i];
            const isLast = i === cids.length - 1;

            const thisQty = isLast ? qty - assignedThisItemQty : qtyPer;
            const thisWeight = isLast
              ? userWeight - assignedThisItemKg
              : weightPer;

            if (thisQty <= 0) continue;

            let entry = containerDetails.find((e) => e?.container?.cid === cid);
            if (!entry) {
              const contRes = await client.query(
                `SELECT container_number FROM container_master WHERE cid = $1`,
                [cid],
              );
              const contNumber = contRes.rows[0]?.container_number || "UNKNOWN";

              entry = {
                status: "Ready for Loading",
                container: { cid, container_number: contNumber },
                total_number: String(item.total_number || 0),
                assign_weight: "0",
                remaining_items: String(item.total_number || 0),
                assign_total_box: "0",
              };
              containerDetails.push(entry);
            }

            entry.assign_total_box = String(
              Number(entry.assign_total_box || 0) + thisQty,
            );
            entry.assign_weight = (
              Number(entry.assign_weight || 0) + thisWeight
            ).toFixed(2);

            const newTotalAssigned =
              currentBoxes + assignedThisItemQty + thisQty;

            entry.remaining_items = String(
              item.total_number - newTotalAssigned,
            );

            assignedThisItemQty += thisQty;
            assignedThisItemKg += thisWeight;

            await client.query(
              `
                INSERT INTO container_assignment_history (
                  cid, container_number, order_id, receiver_id, detail_id,
                  assigned_qty, assigned_weight_kg, status, previous_status,
                  action_type, created_by, updated_by, changed_by, notes,
                  created_at, loaded_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,NOW(),$15)
              `,
              [
                cid,
                entry.container.container_number,
                orderId,
                recId,
                item.id,
                thisQty,
                thisWeight,
                entry.status,
                "Created",
                "ASSIGN",
                currentUserEmail,
                currentUserEmail,
                currentUserEmail,
                `Assigned ${thisQty} boxes (${thisWeight.toFixed(2)} kg) - user total ${userWeight} kg`,
                itemLoadingDate ? new Date(itemLoadingDate) : null,
              ],
            );

            await client.query(
              `UPDATE container_master SET status = 'Ready for Loading' WHERE cid = $1`,
              [cid],
            );

            newContainers.add(entry.container.container_number);
            lastCid = cid;
          }

          trackingEntries.push({
            cid: lastCid,
            itemRef: item.item_ref,
          });

          const newBoxes = currentBoxes + assignedThisItemQty;
          const newKg = currentKg + assignedThisItemKg;

          await client.query(
            `
            UPDATE order_items
            SET
              assigned_boxes     = $1,
              assigned_weight_kg = $2,
              container_details  = $3::jsonb,
              assigned_at        = NOW(),
              updated_at         = NOW()
            WHERE id = $4
          `,
            [newBoxes, newKg, JSON.stringify(containerDetails), item.id],
          );

          receiverAssignedQty += assignedThisItemQty;
          receiverAssignedKg += assignedThisItemKg;
        }

        if (receiverAssignedQty > 0) {
          const updatedContainers = [
            ...new Set([...receiverContainers, ...newContainers]),
          ];

          await client.query(
            `
            UPDATE receivers
            SET
              qty_delivered = qty_delivered + $1,
              containers    = $2::jsonb,
              updated_at    = NOW()
            WHERE id = $3
            `,
            [receiverAssignedQty, JSON.stringify(updatedContainers), recId],
          );

          const nextStatus = await moveReceiverToNextStatus(client, recId);

          const trackingStatus =
            nextStatus?.order_status ||
            (
              await client.query(`SELECT status FROM receivers WHERE id = $1`, [
                recId,
              ])
            ).rows[0].status;

          const { eta: trackingEta } = await calculateETA(
            client,
            trackingStatus,
          );

          for (const tracking of trackingEntries) {
            await createOrderTracking(client, {
              orderId,
              receiverId: recId,
              containerId: tracking.cid,
              status: trackingStatus,
              createdBy: currentUserEmail,
              itemRef: tracking.itemRef,
              eta: trackingEta,
              etd: trackingEta,
            });
          }

          trackingData.push({
            receiverId: recId,
            assignedQty: receiverAssignedQty,
            assignedWeightKg: receiverAssignedKg.toFixed(2),
            containers: [...newContainers],
          });

          orderAssignedQty += receiverAssignedQty;
        }
      }

      if (orderAssignedQty > 0) {
        await client.query(
          `
          UPDATE orders
          SET total_assigned_qty = total_assigned_qty + $1,
              updated_at = NOW()
          WHERE id = $2
        `,
          [orderAssignedQty, orderId],
        );
      }
    }

    await client.query("COMMIT");

    res.json({
      success: true,
      message: "Containers assigned successfully",
      tracking: trackingData,
    });
  } catch (err) {
    if (client) await client.query("ROLLBACK");
    console.error("Assign containers error:", err.stack || err);
    res.status(400).json({
      error: "Failed to assign containers",
      details: err.message,
    });
  } finally {
    if (client) client.release();
  }
}

export async function removeContainerAssignments(req, res) {
  let client;
  try {
    client = await pool.connect();
    await client.query("BEGIN");

    const { assignments } = req.body;
    const created_by = req.user?.email || "system";

    if (
      !assignments ||
      typeof assignments !== "object" ||
      !Object.keys(assignments).length
    ) {
      return res
        .status(400)
        .json({ error: "Valid non-empty assignments object required" });
    }

    const orderIds = Object.keys(assignments).map(Number).filter(Boolean);
    if (!orderIds.length)
      return res.status(400).json({ error: "No valid order IDs" });

    const trackingData = [];

    for (const orderId of orderIds) {
      const orderRemove = assignments[orderId];
      let orderRemovedQty = 0;

      for (const recIdStr in orderRemove) {
        const recId = Number(recIdStr);
        const recRemove = orderRemove[recIdStr];

        const receiverRes = await client.query(
          `
          SELECT id, qty_delivered, containers, total_number
          FROM receivers
          WHERE id = $1 AND order_id = $2
          FOR UPDATE
        `,
          [recId, orderId],
        );

        if (!receiverRes.rowCount) continue;

        const receiver = receiverRes.rows[0];
        let receiverContainers = safeParseJsonArray(receiver.containers);

        let receiverRemovedQty = 0;
        let receiverRemovedKg = 0;
        const removedContainers = new Set();

        const isFullRemoval = !!recRemove.full;

        if (isFullRemoval) {
          const itemsRes = await client.query(
            `
            SELECT id, assigned_boxes, assigned_weight_kg, container_details
            FROM order_items
            WHERE receiver_id = $1 AND order_id = $2
            FOR UPDATE
          `,
            [recId, orderId],
          );

          for (const item of itemsRes.rows) {
            const assignedBoxes = Number(item.assigned_boxes || 0);
            const assignedKg = Number(item.assigned_weight_kg || 0);

            if (assignedBoxes <= 0) continue;

            receiverRemovedQty += assignedBoxes;
            receiverRemovedKg += assignedKg;

            const oldDetails = safeParseJsonArray(item.container_details);
            for (const cd of oldDetails) {
              const cid = Number(cd?.container?.cid);
              if (!cid) continue;

              const contRes = await client.query(
                `SELECT container_number FROM container_master WHERE cid = $1`,
                [cid],
              );
              const containerNumber =
                contRes.rows[0]?.container_number || "UNKNOWN";

              await client.query(
                `
                INSERT INTO container_assignment_history (
                  cid, container_number, order_id, receiver_id, detail_id,
                  assigned_qty, assigned_weight_kg, status, previous_status,
                  action_type, changed_by, notes, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
              `,
                [
                  cid,
                  containerNumber,
                  orderId,
                  recId,
                  item.id,
                  -Number(cd.assign_total_box || 0),
                  -Number(cd.assign_weight || 0),
                  "Unassigned",
                  "Ready for Loading",
                  "UNASSIGN",
                  created_by,
                  `Bulk removal: ${cd.assign_total_box || 0} boxes, ${cd.assign_weight || 0} kg removed`,
                ],
              );

              removedContainers.add(containerNumber);
            }

            await client.query(
              `
              UPDATE order_items
              SET
                assigned_boxes = 0,
                assigned_weight_kg = 0,
                container_details = '[]'::jsonb,
                assigned_at = NULL,
                updated_at = NOW()
              WHERE id = $1 AND order_id = $2
              `,
              [item.id, orderId],
            );

            await client.query(
              `
              UPDATE receivers r
              SET
                containers = '[]'::jsonb,
                qty_delivered = 0,
                status = 'Created',
                updated_at = NOW()
              WHERE r.id = (
                SELECT receiver_id FROM order_items WHERE id = $1 AND order_id = $2
              )
              `,
              [item.id, orderId],
            );

            const itemRefRes = await client.query(
              `SELECT item_ref FROM order_items WHERE id = $1`,
              [item.id],
            );
            const removedItemRef = itemRefRes.rows[0]?.item_ref || null;

            const { eta: revertEta } = await calculateETA(client, "Created");

            await createOrderTracking(client, {
              orderId,
              receiverId: recId,
              containerId: null,
              status: "Created",
              createdBy: created_by,
              itemRef: removedItemRef,
              eta: revertEta,
              etd: revertEta,
            });

            orderRemovedQty += receiverRemovedQty;
          }
        } else {
          for (const itemIdStr in recRemove) {
            const removeInfo = recRemove[itemIdStr];
            const itemId = Number(removeInfo.orderItemId || itemIdStr);
            const removeQty = Number(removeInfo.qty || 0);
            let removeWeight = parseFloat(removeInfo.totalAssignedWeight || 0);

            if (!itemId || removeQty <= 0) continue;

            const itemRes = await client.query(
              `
              SELECT *
              FROM order_items
              WHERE id = $1 AND receiver_id = $2
              FOR UPDATE
            `,
              [itemId, recId],
            );

            if (!itemRes.rowCount) continue;
            const item = itemRes.rows[0];

            const currentBoxes = Number(item.assigned_boxes || 0);
            const currentKg = Number(item.assigned_weight_kg || 0);
            if (isNaN(removeWeight) || removeWeight <= 0) {
              removeWeight = currentKg;
            }

            if (removeQty > currentBoxes || removeWeight > currentKg + 0.01) {
              throw new Error(
                `Cannot remove more than assigned: ${removeQty} > ${currentBoxes} boxes or ${removeWeight} > ${currentKg} kg`,
              );
            }

            let containerDetails = safeParseJsonArray(item.container_details);

            const sanitizedDetails = containerDetails
              .map((entry) => {
                if (!entry || typeof entry !== "object") return null;
                return {
                  status: String(entry.status || "Created"),
                  container:
                    entry.container && entry.container.cid
                      ? {
                          cid: Number(entry.container.cid),
                          container_number: String(
                            entry.container.container_number || "",
                          ),
                        }
                      : null,
                  total_number: Number(
                    entry.total_number ?? entry.totalNumber ?? 0,
                  ),
                  assign_weight: Number(
                    entry.assign_weight ?? entry.assignWeight ?? 0,
                  ).toFixed(2),
                  remaining_items: String(Number(entry.remaining_items ?? 0)),
                  assign_total_box: String(
                    Number(entry.assign_total_box ?? entry.assignTotalBox ?? 0),
                  ),
                };
              })
              .filter(Boolean);

            let removedThisItemQty = 0;
            let removedThisItemKg = 0;

            const cidsToRemove = (removeInfo.containers || [])
              .map(Number)
              .filter(Boolean);

            for (const cid of cidsToRemove) {
              const entryIdx = sanitizedDetails.findIndex(
                (e) => e?.container?.cid === cid,
              );
              if (entryIdx === -1) continue;

              const entry = sanitizedDetails[entryIdx];
              const availBox = Number(entry.assign_total_box || 0);
              const availKg = Number(entry.assign_weight || 0);

              if (availBox <= 0) continue;

              const takeBox = availBox;
              const takeKg = availKg;

              removedThisItemQty += takeBox;
              removedThisItemKg += takeKg;

              const contRes = await client.query(
                `SELECT container_number FROM container_master WHERE cid = $1`,
                [cid],
              );
              const containerNumber =
                contRes.rows[0]?.container_number || "UNKNOWN";

              await client.query(
                `
                INSERT INTO container_assignment_history (
                  cid, container_number, order_id, receiver_id, detail_id,
                  assigned_qty, assigned_weight_kg, status, previous_status,
                  action_type, changed_by, notes, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
              `,
                [
                  cid,
                  containerNumber,
                  orderId,
                  recId,
                  item.id,
                  -takeBox,
                  -takeKg,
                  "Unassigned",
                  "Ready for Loading",
                  "UNASSIGN",
                  created_by,
                  `Removed container ${containerNumber}: ${takeBox} boxes, ${takeKg.toFixed(2)} kg`,
                ],
              );

              removedContainers.add(containerNumber);

              sanitizedDetails.splice(entryIdx, 1);
            }

            const newBoxes = Math.max(0, currentBoxes - removedThisItemQty);
            const newKg = Math.max(0, currentKg - removedThisItemKg);

            sanitizedDetails.forEach((cd) => {
              cd.remaining_items = String(item.total_number - newBoxes);
            });

            const finalJson = JSON.stringify(
              sanitizedDetails.length ? sanitizedDetails : [],
            );

            console.debug(
              `[REMOVE] Updating item ${item.id}: new boxes=${newBoxes}, kg=${newKg}, container_details=`,
              finalJson,
            );

            await client.query(
              `
                UPDATE order_items
                SET
                  assigned_boxes     = $1,
                  assigned_weight_kg = $2,
                  container_details  = $3::jsonb,
                  assigned_at        = CASE WHEN $1 > 0 THEN assigned_at ELSE NULL END,
                  updated_at         = NOW()
                WHERE id = $4
              `,
              [newBoxes, newKg, finalJson, item.id],
            );

            const revertedItemStatus =
              newBoxes <= 0 ? "Created" : "Partially Assigned";
            const { eta: itemRevertEta } = await calculateETA(
              client,
              revertedItemStatus,
            );

            await createOrderTracking(client, {
              orderId,
              receiverId: recId,
              containerId: null,
              status: revertedItemStatus,
              createdBy: created_by,
              itemRef: item.item_ref,
              eta: itemRevertEta,
              etd: itemRevertEta,
            });

            receiverRemovedQty += removedThisItemQty;
            receiverRemovedKg += removedThisItemKg;
          }
        }

        if (receiverRemovedQty > 0 || receiverRemovedKg > 0) {
          receiverContainers = receiverContainers.filter(
            (c) => !removedContainers.has(c),
          );

          const newDelivered = Math.max(
            0,
            Number(receiver.qty_delivered || 0) - receiverRemovedQty,
          );

          await client.query(
            `
            UPDATE receivers
            SET
              qty_delivered = $1,
              containers    = $2::jsonb,
              status        = CASE 
                WHEN $1 <= 0 THEN 'Created'
                WHEN $1 < total_number THEN 'Partially Assigned'
                ELSE status 
              END,
              updated_at    = NOW()
            WHERE id = $3
          `,
            [newDelivered, JSON.stringify(receiverContainers), recId],
          );

          trackingData.push({
            receiverId: recId,
            removedQty: receiverRemovedQty,
            removedWeightKg: receiverRemovedKg.toFixed(2),
            removedContainers: [...removedContainers],
            isBulk: isFullRemoval,
          });

          orderRemovedQty += receiverRemovedQty;
        }
      }

      if (orderRemovedQty > 0) {
        await client.query(
          `
          UPDATE orders
          SET total_assigned_qty = GREATEST(0, total_assigned_qty - $1),
              updated_at = NOW()
          WHERE id = $2
        `,
          [orderRemovedQty, orderId],
        );
      }
    }

    await client.query("COMMIT");

    res.json({
      success: true,
      message: "Assignments removed successfully",
      tracking: trackingData,
    });
  } catch (err) {
    if (client) await client.query("ROLLBACK");
    console.error("Remove assignment error:", err.stack || err);
    res.status(400).json({
      error: "Failed to remove container assignments",
      details: err.message,
    });
  } finally {
    if (client) client.release();
  }
}

export async function removeOrderItem(req, res) {
  let client;

  try {
    client = await pool.connect();
    await client.query("BEGIN");

    const { orderId, itemId } = req.params;

    const itemRes = await client.query(
      `
      SELECT
        oi.id,
        oi.order_id,
        oi.receiver_id,
        COALESCE(oi.assigned_boxes, 0) AS assigned_boxes,
        COALESCE(oi.assigned_weight_kg, 0) AS assigned_weight_kg,
        COALESCE(oi.container_details, '[]'::jsonb) AS container_details,
        r.containers,
        COALESCE(r.qty_delivered, 0) AS qty_delivered
      FROM order_items oi
      JOIN receivers r ON r.id = oi.receiver_id
      WHERE oi.id = $1 AND oi.order_id = $2
      FOR UPDATE
      `,
      [itemId, orderId],
    );

    if (itemRes.rowCount === 0) {
      await client.query("ROLLBACK");
      return res.status(404).json({
        success: false,
        error: "Order item not found for this order",
      });
    }

    const item = itemRes.rows[0];

    const safeArray = (val) => {
      if (!val) return [];
      if (Array.isArray(val)) return val;
      try {
        const parsed = JSON.parse(val);
        return Array.isArray(parsed) ? parsed : [];
      } catch {
        return [];
      }
    };

    const containerDetails = safeArray(item.container_details);
    let receiverContainers = safeArray(item.containers);

    const removedQty = Number(item.assigned_boxes || 0);
    const removedWeight = Number(item.assigned_weight_kg || 0);

    const removedContainerNumbers = containerDetails
      .map((cd) => cd?.container?.container_number)
      .filter(Boolean);

    const removedContainerIds = containerDetails
      .map((cd) => Number(cd?.container?.cid))
      .filter(Boolean);

    for (const cd of containerDetails) {
      const cid = Number(cd?.container?.cid || 0);
      const containerNumber = cd?.container?.container_number || "";

      if (!cid) continue;

      await client.query(
        `
        INSERT INTO container_assignment_history (
          cid,
          container_number,
          order_id,
          receiver_id,
          detail_id,
          assigned_qty,
          assigned_weight_kg,
          status,
          previous_status,
          action_type,
          changed_by,
          notes,
          created_at
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NOW())
        `,
        [
          cid,
          containerNumber,
          Number(orderId),
          item.receiver_id,
          Number(itemId),
          -Number(cd.assign_total_box || 0),
          -Number(cd.assign_weight || 0),
          "Unassigned",
          "Ready for Loading",
          "UNASSIGN",
          req.user?.id || req.user?.email || "system",
          `Removed shipping item ${itemId}; container assignment cleared`,
        ],
      );
    }

    // Receiver summary containers clean
    receiverContainers = receiverContainers.filter((c) => {
      const num =
        typeof c === "string"
          ? c
          : c?.container_number || c?.container?.container_number || "";

      const cid =
        typeof c === "object" ? Number(c?.cid || c?.container?.cid || 0) : 0;

      return (
        !removedContainerNumbers.includes(num) &&
        !removedContainerIds.includes(cid)
      );
    });

    const newQtyDelivered = Math.max(
      0,
      Number(item.qty_delivered || 0) - removedQty,
    );

    await client.query(
      `
      UPDATE receivers
      SET
        qty_delivered = $1,
        containers = $2::jsonb,
        status = CASE
          WHEN $1 <= 0 THEN 'Created'
          ELSE status
        END,
        updated_at = NOW()
      WHERE id = $3
      `,
      [newQtyDelivered, JSON.stringify(receiverContainers), item.receiver_id],
    );

    await client.query(
      `
      UPDATE orders
      SET
        total_assigned_qty = GREATEST(0, COALESCE(total_assigned_qty, 0) - $1),
        updated_at = NOW()
      WHERE id = $2
      `,
      [removedQty, orderId],
    );

    await client.query(
      `DELETE FROM order_items WHERE id = $1 AND order_id = $2`,
      [itemId, orderId],
    );

    await client.query("COMMIT");

    return res.status(200).json({
      success: true,
      message:
        "Order item, assigned boxes, and container summary removed successfully",
      itemId: Number(itemId),
      removedQty,
      removedWeight,
      removedContainers: removedContainerNumbers,
    });
  } catch (error) {
    if (client) await client.query("ROLLBACK");

    console.error("[removeOrderItem] Error:", error);

    return res.status(500).json({
      success: false,
      error: "Failed to remove order item",
      message: error.message,
    });
  } finally {
    if (client) client.release();
  }
}
export async function removeReceiver(req, res) {
  let client;
  try {
    client = await pool.connect();
    await client.query("BEGIN");

    const { orderId, receiverId } = req.params;

    console.log(
      `[removeReceiver] Removing receiver ${receiverId} from order ${orderId}`,
    );

    // ── 1. Verify receiver belongs to this order ──────────────────────────
    const receiverCheck = await client.query(
      `SELECT id FROM receivers WHERE id = $1 AND order_id = $2`,
      [receiverId, orderId],
    );

    if (receiverCheck.rowCount === 0) {
      await client.query("ROLLBACK");
      return res
        .status(404)
        .json({ error: "Receiver not found for this order" });
    }

    // ── 2. Delete child records first (FK constraints) ────────────────────
    const deletedItems = await client.query(
      `DELETE FROM order_items WHERE receiver_id = $1 RETURNING id`,
      [receiverId],
    );
    console.log(
      `[removeReceiver] Deleted ${deletedItems.rowCount} order_items`,
    );

    const deletedDropOffs = await client.query(
      `DELETE FROM drop_off_details WHERE receiver_id = $1 RETURNING id`,
      [receiverId],
    );
    console.log(
      `[removeReceiver] Deleted ${deletedDropOffs.rowCount} drop_off_details`,
    );

    // ── 3. Delete the receiver ────────────────────────────────────────────
    await client.query(`DELETE FROM receivers WHERE id = $1`, [receiverId]);
    console.log(`[removeReceiver] Receiver ${receiverId} deleted`);

    // ── 4. Update order updated_at ────────────────────────────────────────
    await client.query(`UPDATE orders SET updated_at = NOW() WHERE id = $1`, [
      orderId,
    ]);

    await client.query("COMMIT");

    // ── 5. Return updated receivers list ──────────────────────────────────
    const updatedReceivers = await client.query(
      `SELECT *, receiver_marks_and_number AS "marksAndNumber"
       FROM receivers WHERE order_id = $1 ORDER BY id`,
      [orderId],
    );

    return res.status(200).json({
      message: "Receiver removed successfully",
      receivers: updatedReceivers.rows.map((r) => ({
        ...r,
        eta: r.eta ? String(r.eta).split("T")[0] : "",
        etd: r.etd ? String(r.etd).split("T")[0] : "",
      })),
    });
  } catch (error) {
    console.error("[removeReceiver] Error:", error);
    if (client) await client.query("ROLLBACK");
    return res.status(500).json({
      error: "Failed to remove receiver",
      message: error.message,
      code: error.code || null,
      detail: error.detail || null,
    });
  } finally {
    if (client) client.release();
  }
}

export async function updateSpecificItemsStatus(req, res) {
  const { orderId } = req.params;
  const {
    itemRefs,
    receiverId,
    status,
    notifyClient = true,
    notifyParties = true,
    forceRecalcEta = false,
  } = req.body || {};

  if (!Array.isArray(itemRefs) || itemRefs.length === 0) {
    return res
      .status(400)
      .json({ error: "itemRefs must be a non-empty array" });
  }

  if (itemRefs.length > 20) {
    return res.status(400).json({ error: "Maximum 20 items per request" });
  }

  let client;
  try {
    client = await pool.connect();
    await client.query("BEGIN");

    const trimmedStatus = (status || "").trim();

    if (!trimmedStatus) {
      throw new Error("Status is required");
    }

    const statusResult = await client.query(
      `SELECT order_status
       FROM statuses
       WHERE status = true
         AND order_status ILIKE $1
       ORDER BY sorting_number ASC
       LIMIT 1`,
      [trimmedStatus],
    );

    if (statusResult.rowCount === 0) {
      throw new Error(`Invalid status: "${trimmedStatus}"`);
    }

    const normalizedStatus = statusResult.rows[0].order_status;

    let extraWhere = "";
    const queryParams = [normalizedStatus, Number(orderId), itemRefs];

    if (receiverId) {
      extraWhere = " AND receiver_id = $4";
      queryParams.push(Number(receiverId));
    }

    const updateResult = await client.query(
      `
      UPDATE order_items
         SET consignment_status = $1,
             updated_at = CURRENT_TIMESTAMP
       WHERE order_id = $2
         AND item_ref = ANY($3::text[])${extraWhere}
      RETURNING id, receiver_id, item_ref, consignment_status
    `,
      queryParams,
    );

    if (updateResult.rowCount === 0) {
      await client.query("ROLLBACK");
      return res.status(404).json({
        error: "No matching items found",
        hint: receiverId
          ? "Item(s) may not belong to this receiver"
          : "Check item references",
      });
    }

    const updatedRows = updateResult.rows;
    const affectedReceiverIds = [
      ...new Set(updatedRows.map((r) => r.receiver_id)),
    ];

    for (const rid of affectedReceiverIds) {
      const agg = await client.query(
        `
        SELECT 
          COUNT(*) AS total,
          COUNT(CASE WHEN consignment_status = 'Shipment Delivered' THEN 1 END) AS delivered
        FROM order_items
        WHERE receiver_id = $1
      `,
        [rid],
      );

      const { total, delivered } = agg.rows[0];

      if (total > 0 && delivered === total) {
        await client.query(
          `
          UPDATE receivers
             SET status = 'Shipment Delivered',
                 updated_at = CURRENT_TIMESTAMP
           WHERE id = $1
             AND status != 'Shipment Delivered'
        `,
          [rid],
        );
      }
    }

    const { eta: calculatedEta } = await calculateETA(client, normalizedStatus);
    const calculatedEtd = calculatedEta;

    if (forceRecalcEta) {
      for (const rid of affectedReceiverIds) {
        await client.query(
          `
          UPDATE receivers 
             SET eta = $1, updated_at = CURRENT_TIMESTAMP 
           WHERE id = $2
        `,
          [calculatedEta, rid],
        );
      }
    }

    const minEtaRes = await client.query(
      `SELECT MIN(eta) AS min_eta FROM receivers WHERE order_id = $1`,
      [Number(orderId)],
    );

    if (minEtaRes.rows[0]?.min_eta) {
      await client.query(
        `UPDATE orders SET eta = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2`,
        [minEtaRes.rows[0].min_eta, Number(orderId)],
      );
    }

    let createdByLabel = "system";
    if (req.user?.id) {
      const userRes = await client.query(
        `SELECT email FROM users WHERE id = $1`,
        [req.user.id],
      );
      createdByLabel = userRes.rows[0]?.email || "system";
    }

    for (const row of updatedRows) {
      const lastTrackingRes = await client.query(
        `
        SELECT sender_id, sender_ref, receiver_ref, container_id, consignment_number, status
        FROM order_tracking
        WHERE order_id = $1
          AND receiver_id = $2
          AND item_ref = $3
        ORDER BY created_time DESC
        LIMIT 1
      `,
        [Number(orderId), row.receiver_id, row.item_ref],
      );

      const last = lastTrackingRes.rows[0] || {};

      await client.query(
        `
        INSERT INTO order_tracking 
          (order_id, sender_id, sender_ref, receiver_id, receiver_ref,
           container_id, consignment_number, status, old_status,
           item_ref, eta, etd, created_by, created_time)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, CURRENT_TIMESTAMP)
      `,
        [
          Number(orderId),
          last.sender_id ?? null,
          last.sender_ref ?? null,
          row.receiver_id,
          last.receiver_ref ?? null,
          last.container_id ?? null,
          last.consignment_number ?? null,
          normalizedStatus,
          last.status ?? null,
          row.item_ref,
          calculatedEta,
          calculatedEtd,
          createdByLabel,
        ],
      );
    }

    await client.query("COMMIT");

    const orderResult = await pool.query(
      `
      SELECT 
        o.booking_ref,
        pol.name AS pol_name,
        pod.name AS pod_name,
        TO_CHAR(o.eta, 'DD Mon YYYY') AS eta_formatted,
        o.sender_email,
        o.sender_name,
        MAX(r.receiver_name) AS receiver_name
      FROM orders o
      LEFT JOIN places pol ON o.place_of_loading = pol.id
      LEFT JOIN places pod ON o.place_of_delivery = pod.id
      LEFT JOIN receivers r ON r.order_id = o.id AND r.id = ANY($2::int[])
      WHERE o.id = $1
      GROUP BY o.id, pol.name, pod.name
    `,
      [Number(orderId), affectedReceiverIds],
    );

    const order = orderResult.rows[0] || {};

    const routeDisplay =
      order.pol_name && order.pod_name
        ? `${order.pol_name} → ${order.pod_name}`
        : order.pod_name || order.pol_name || "—";

    const itemsForEmail = updatedRows.map((r) => ({
      itemRef: r.item_ref,
      receiverId: r.receiver_id,
      status: r.consignment_status,
    }));

    if (notifyClient || notifyParties) {
      setImmediate(() => {
        notifyOrderStatusUpdate(Number(orderId), {
          receiverName: order.receiver_name || "Valued Customer",
          statusLabel: normalizedStatus,
          statusMsg: getStatusMessage(normalizedStatus),
          refId: order.booking_ref || "—",
          route: routeDisplay,
          etaFormatted: order.eta_formatted || "—",
          trackLink: "https://consolidatetracking.onrender.com/",
          updatedItems: itemsForEmail,
        }).catch((err) => {
          console.error("Background notification error:", err.message);
        });
      });
    }

    // 11. Success response
    return res.status(200).json({
      success: true,
      updatedCount: updateResult.rowCount,
      updatedItems: itemsForEmail,
      eta: calculatedEta,
      etd: calculatedEtd,
      message: `Updated ${updateResult.rowCount} item(s) to "${normalizedStatus}"`,
    });
  } catch (err) {
    if (client) await client.query("ROLLBACK");
    console.error("updateSpecificItemsStatus error:", err);
    return res.status(500).json({
      error: "Failed to update items",
      details: err.message,
    });
  } finally {
    if (client) client.release();
  }
}

export async function getOrderByItemRef(req, res) {
  const { ref } = req.params;

  if (!ref || typeof ref !== "string" || ref.trim() === "") {
    return res.status(400).json({
      success: false,
      message: "Item reference is required",
    });
  }

  const pattern = `%${ref.trim().toUpperCase()}%`;

  try {
    const { rows: statusRows } = await pool.query(
      `SELECT order_status
       FROM statuses
       WHERE status = true
       ORDER BY sorting_number ASC`,
    );
    const statusSequence = statusRows.map((s) => s.order_status);

    const query = `
      SELECT 
        o.id AS order_id,
        o.booking_ref,
        o.place_of_loading,
        o.place_of_delivery,
        pol.name AS place_of_loading_name,
        pod.name AS place_of_delivery_name,
        r.id AS receiver_id,
        r.status AS receiver_base_status,
        oi.id AS item_id,
        oi.item_ref,
        oi.total_number,
        oi.weight,
        ot.id AS ot_tracking_id,
        ot.old_status AS ot_old_status,
        ot.status AS ot_new_status,
        ot.created_time AS ot_timestamp,
        ot.created_by AS ot_created_by,
        ot.eta AS ot_eta,
        ot.etd AS ot_etd,
        ot.consignment_number AS ot_consignment_number
      FROM order_items oi
      INNER JOIN orders o ON oi.order_id = o.id
      LEFT JOIN places pol ON pol.id = o.place_of_loading
      LEFT JOIN places pod ON pod.id = o.place_of_delivery
      LEFT JOIN receivers r ON oi.receiver_id = r.id
      LEFT JOIN order_tracking ot
        ON ot.receiver_id = r.id
        OR (ot.order_id = o.id AND ot.item_ref = oi.item_ref)
      WHERE oi.item_ref ILIKE $1
      ORDER BY o.created_at DESC, oi.id, ot.created_time DESC
    `;

    const { rows } = await pool.query(query, [pattern]);

    if (rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: `No items found matching reference "${ref.trim()}"`,
      });
    }

    const orderMap = {};

    rows.forEach((row) => {
      const orderId = row.order_id;

      if (!orderMap[orderId]) {
        orderMap[orderId] = {
          order_id: orderId,
          booking_ref: row.booking_ref,
          place_of_loading: row.place_of_loading_name || null,
          place_of_delivery: row.place_of_delivery_name || null,
          receivers: {},
        };
      }

      const ord = orderMap[orderId];
      const recvKey = row.receiver_id
        ? row.receiver_id
        : `no_receiver_${orderId}`;

      if (!ord.receivers[recvKey]) {
        const receiverCurrent = row.receiver_base_status;

        const currentIdx = statusSequence.indexOf(receiverCurrent);
        const remaining =
          currentIdx === -1 || currentIdx >= statusSequence.length - 1
            ? []
            : statusSequence.slice(currentIdx + 1);

        ord.receivers[recvKey] = {
          receiver_id: row.receiver_id || null,
          status: receiverCurrent,
          eta: null,
          items: {},
          current_status: receiverCurrent,
          status_history: [],
          remaining_status_steps: remaining,
        };
      }

      const recv = ord.receivers[recvKey];

      if (row.ot_tracking_id) {
        const alreadyExists = recv.status_history.some(
          (h) => h.tracking_id === row.ot_tracking_id,
        );

        if (!alreadyExists) {
          recv.status_history.push({
            tracking_id: row.ot_tracking_id,
            old_status: row.ot_old_status || null,
            status: row.ot_new_status,
            time: row.ot_timestamp,
            created_by: row.ot_created_by || null,
            eta: row.ot_eta || null,
            etd: row.ot_etd || null,
            consignment_number: row.ot_consignment_number || null,
          });
        }
      }

      if (row.item_id && !recv.items[row.item_id]) {
        recv.items[row.item_id] = {
          item_id: row.item_id,
          item_ref: row.item_ref || "—",
          total_number: Number(row.total_number) || 0,
          weight: Number(row.weight) || 0,
        };
      }
    });

    const result = Object.values(orderMap).map((order) => {
      Object.values(order.receivers).forEach((recv) => {
        recv.status_history.sort((a, b) => new Date(b.time) - new Date(a.time));

        recv.current_status = recv.status;

        if (recv.status_history.length > 0) {
          recv.latest_tracking_status = recv.status_history[0].status;
          recv.eta = recv.status_history[0].eta || null;
        } else {
          recv.latest_tracking_status = null;
        }
      });

      return {
        ...order,
        receivers: Object.values(order.receivers).map((r) => ({
          ...r,
          items: Object.values(r.items),
        })),
      };
    });

    res.json({
      success: true,
      data: result.length === 1 ? result[0] : result,
      count: result.length,
      message: `Found ${result.length} order(s) containing item reference matching "${ref.trim()}"`,
    });
  } catch (err) {
    console.error("getOrderByItemRef error:", err.stack || err);
    res.status(500).json({
      success: false,
      message: "Failed to fetch item tracking details",
      error: process.env.NODE_ENV === "development" ? err.message : undefined,
    });
  }
}

export async function getOrderByTrackingId(req, res) {
  const { id } = req.params;

  if (!id?.trim()) {
    return res
      .status(400)
      .json({ success: false, message: "Consignment number required" });
  }

  const consNumber = id.trim().toUpperCase();

  try {
    const consRes = await pool.query(
      `
      SELECT 
        c.id AS consignment_id,
        c.consignment_number,
        c.status AS consignment_status,
        c.eta AS consignment_eta,
        c.origin,
        c.destination,
        c.shipping_line,
        c.vessel,
        c.voyage,
        c.seal_no,
        c.net_weight,
        c.gross_weight,
        c.consignment_value,
        c.currency_code,
        c.containers,
        c.orders
      FROM consignments c
      WHERE c.consignment_number = $1
    `,
      [consNumber],
    );

    if (consRes.rowCount === 0) {
      return res
        .status(404)
        .json({ success: false, message: "Consignment not found" });
    }

    const cons = consRes.rows[0];

    let containers = [];
    try {
      containers =
        typeof cons.containers === "string"
          ? JSON.parse(cons.containers)
          : cons.containers || [];
    } catch (e) {
      console.warn("Invalid consignment containers JSON:", e);
    }

    let orderIds = [];
    try {
      const rawOrders =
        typeof cons.orders === "string"
          ? JSON.parse(cons.orders)
          : cons.orders || [];
      orderIds = rawOrders
        .map((v) => parseInt(v, 10))
        .filter((n) => !isNaN(n) && n > 0);
    } catch (e) {
      console.warn("Invalid orders array in consignment:", e);
    }

    let orders = [];
    let summary = {
      order_count: 0,
      total_assigned: 0,
      total_items: 0,
      progress_percent: 0,
      active_containers: [],
      latest_activity: null,
    };

    if (orderIds.length > 0) {
      const safeIntCast = (val) => `COALESCE(${val}, 0)`;
      const safeNumericCast = (val) => `COALESCE(${val}, 0)`;

      const containerDetailsSub = `
        COALESCE((
          SELECT jsonb_agg(
            jsonb_build_object(
              'status',      COALESCE(cs.availability, 'Created'),
              'container', jsonb_build_object(
                'cid',              (cd_obj->'container'->>'cid')::int,
                'container_number', COALESCE(cd_obj->'container'->>'container_number', 'Unknown')
              ),
              'total_number',    ${safeIntCast("(cd_obj->>'total_number')::int")},
              'assign_weight',   COALESCE(cd_obj->>'assign_weight', '0')::text,
              'remaining_items', COALESCE(cd_obj->>'remaining_items', '0')::text,
              'assign_total_box',COALESCE(cd_obj->>'assign_total_box', '0')::text
            ) ORDER BY (cd_obj->'container'->>'container_number')
          )
          FROM jsonb_array_elements(COALESCE(oi.container_details, '[]'::jsonb)) cd_obj
          LEFT JOIN LATERAL (
            SELECT availability
            FROM container_status
            WHERE cid = (cd_obj->'container'->>'cid')::int
            ORDER BY sid DESC LIMIT 1
          ) cs ON true
          WHERE (cd_obj->'container'->>'cid') ~ '^[0-9]+$'
        ), '[]'::jsonb)
      `;

      const shippingDetailsAgg = `
        (SELECT COALESCE(jsonb_agg(
          jsonb_build_object(
            'id',              oi.id,
            'orderId',         oi.order_id,
            'senderId',        oi.sender_id,
            'category',        COALESCE(oi.category, ''),
            'subcategory',     COALESCE(oi.subcategory, ''),
            'type',            COALESCE(oi.type, ''),
            'pickupLocation',  COALESCE(oi.pickup_location, ''),
            'deliveryAddress', COALESCE(oi.delivery_address, ''),
            'totalNumber',     ${safeIntCast("oi.total_number")},
            'weight',          ${safeNumericCast("oi.weight")},
            'totalWeight',     0,
            'itemRef',         COALESCE(oi.item_ref, ''),
            'consignmentStatus', COALESCE(oi.consignment_status, 'Created'),
            'shippingLine',    COALESCE(oi.shipping_line, ''),
            'containerDetails', ${containerDetailsSub},
            'remainingItems',  ${safeIntCast(`
              oi.total_number - (
                SELECT COALESCE(SUM((cd_obj->>'assign_total_box')::int), 0)
                FROM jsonb_array_elements(COALESCE(oi.container_details, '[]'::jsonb)) cd_obj
              )
            `)}
          ) ORDER BY oi.id
        ), '[]'::jsonb)
        FROM order_items oi 
        WHERE oi.receiver_id = r.id AND oi.order_id = o.id)
      `;

      const ordersRes = await pool.query(
        `
        SELECT 
          o.id AS order_id,
          o.booking_ref,
          o.created_at,
          o.status,
          o.eta,
          o.etd,
          o.total_assigned_qty,
          s.sender_name,
          s.sender_contact,
          s.sender_email,
          t.transport_type,
          t.collection_scope,

          (SELECT COALESCE(jsonb_agg(r_full ORDER BY r_full.id), '[]'::jsonb) 
           FROM (
             SELECT 
               r.id,
               r.order_id                    AS "orderId",
               r.receiver_name               AS "receiverName",
               r.receiver_contact            AS "receiverContact",
               r.receiver_address            AS "receiverAddress",
               r.receiver_email              AS "receiverEmail",
               ${safeIntCast("r.total_number")}       AS "totalNumber",
               ${safeNumericCast("r.total_weight")}   AS "totalWeight",
               r.receiver_ref                AS "receiverRef",
               r.remarks,
               r.containers,
               r.status,
               r.eta                         AS eta,
               r.etd                         AS etd,
               r.shipping_line               AS "shippingLine",
               r.consignment_vessel          AS "consignmentVessel",
               r.consignment_number          AS "consignmentNumber",
               r.consignment_marks           AS "consignmentMarks",
               r.consignment_voyage          AS "consignmentVoyage",
               r.full_partial                AS "fullPartial",
               ${safeIntCast("r.qty_delivered")}      AS "qtyDelivered",
               ${shippingDetailsAgg}                  AS "shippingDetails",
               ${shippingDetailsAgg.replace('"shippingDetails"', '"shippingdetails"')} AS "shippingdetails",
               COALESCE((
                 SELECT jsonb_agg(
                   jsonb_build_object(
                     'drop_method',    dod.drop_method,
                     'dropoff_name',   dod.dropoff_name,
                     'drop_off_cnic',  dod.drop_off_cnic,
                     'drop_off_mobile',dod.drop_off_mobile,
                     'plate_no',       dod.plate_no,
                     'drop_date',      TO_CHAR(dod.drop_date, 'YYYY-MM-DD')
                   ) ORDER BY dod.id
                 ) FROM drop_off_details dod 
                 WHERE dod.receiver_id = r.id
               ), '[]'::jsonb) AS "dropOffDetails"
             FROM receivers r
             WHERE r.order_id = o.id
           ) r_full) AS receivers
        FROM orders o
        LEFT JOIN senders s ON s.order_id = o.id
        LEFT JOIN transport_details t ON t.order_id = o.id
        WHERE o.id = ANY($1::int[])
        ORDER BY o.created_at DESC
      `,
        [orderIds],
      );

      const orderMap = {};
      const allContainers = new Set();

      ordersRes.rows.forEach((row) => {
        let receivers = row.receivers || "[]";
        if (typeof receivers === "string") {
          try {
            receivers = JSON.parse(receivers);
          } catch {
            receivers = [];
          }
        }

        const order = {
          order_id: row.order_id,
          booking_ref: row.booking_ref,
          created_at: row.created_at,
          status: row.status,
          eta: row.eta,
          etd: row.etd,
          collection_scope: row.collection_scope || "—",
          total_assigned_qty: row.total_assigned_qty || 0,
          sender: {
            name: row.sender_name || "—",
            contact: row.sender_contact || "—",
            email: row.sender_email || "—",
          },
          transport: {
            type: row.transport_type || "—",
            collection_scope: row.collection_scope || "Partial",
          },
          receivers,
          summary: {
            total_items: 0,
            total_weight: 0,
            total_assigned: 0,
            active_containers: new Set(),
          },
        };

        // Calculate per-order summary from shippingDetails
        receivers.forEach((r) => {
          (r.shippingDetails || []).forEach((item) => {
            const qty = Number(item.totalNumber || 0);
            const assigned = (item.containerDetails || []).reduce((sum, cd) => {
              return sum + Number(cd.assign_total_box || 0);
            }, 0);

            order.summary.total_items += qty;
            order.summary.total_weight += Number(item.weight || 0);
            order.summary.total_assigned += assigned;

            (item.containerDetails || []).forEach((cd) => {
              const cn = cd.container?.container_number;
              if (cn && cn !== "Unknown") {
                order.summary.active_containers.add(cn);
                allContainers.add(cn);
              }
            });
          });
        });

        orderMap[row.order_id] = order;
      });

      orders = Object.values(orderMap);

      // Overall summary
      let grandTotalItems = 0;
      let grandTotalAssigned = 0;

      orders.forEach((o) => {
        grandTotalItems += o.summary.total_items;
        grandTotalAssigned += o.summary.total_assigned;
        if (
          !summary.latest_activity ||
          o.created_at > summary.latest_activity
        ) {
          summary.latest_activity = o.created_at;
        }
      });

      summary = {
        order_count: orders.length,
        total_assigned: grandTotalAssigned,
        total_items: grandTotalItems,
        progress_percent:
          grandTotalItems > 0
            ? Math.round((grandTotalAssigned / grandTotalItems) * 100)
            : 0,
        active_containers: Array.from(allContainers),
        latest_activity: summary.latest_activity
          ? summary.latest_activity.toISOString()
          : null,
      };
    }

    res.json({
      success: true,
      data: {
        consignment: {
          id: cons.consignment_id,
          number: cons.consignment_number,
          status: cons.consignment_status,
          eta: cons.consignment_eta,
          origin: cons.origin,
          destination: cons.destination,
          shipping_line: cons.shipping_line,
          vessel: cons.vessel,
          voyage: cons.voyage,
          seal_no: cons.seal_no,
          net_weight: cons.net_weight,
          gross_weight: cons.gross_weight,
          value: cons.consignment_value,
          currency: cons.currency_code,
          containers,
        },
        orders,
        summary,
      },
    });
  } catch (err) {
    console.error("getOrderByTrackingId error:", err.stack || err);
    res.status(500).json({
      success: false,
      message: "Server error while tracking consignment",
      details: process.env.NODE_ENV === "development" ? err.message : undefined,
    });
  }
}

export async function getOrderByOrderId(req, res) {
  const { ref } = req.params;

  if (!ref || typeof ref !== "string" || ref.trim() === "") {
    return res.status(400).json({
      success: false,
      message: "Order reference (ID or booking ref) is required",
    });
  }

  const search = ref.trim();
  const isNumeric = !isNaN(Number(search)) && Number.isInteger(Number(search));

  try {
    const safeIntCast = (val) => `COALESCE(${val}, 0)`;
    const safeNumericCast = (val) => `COALESCE(${val}, 0)`;

    const containerDetailsSub = `
      COALESCE((
        SELECT jsonb_agg(
          jsonb_build_object(
            'status',      COALESCE(cs.availability, 'Created'),
            'container', jsonb_build_object(
              'container_number', COALESCE(cd_obj->'container'->>'container_number', 'Unknown')
            ),
            'assign_weight',   COALESCE(cd_obj->>'assign_weight', '0')::text,
            'assign_total_box',COALESCE(cd_obj->>'assign_total_box', '0')::text
          ) ORDER BY (cd_obj->'container'->>'container_number')
        )
        FROM jsonb_array_elements(COALESCE(oi.container_details, '[]'::jsonb)) cd_obj
        LEFT JOIN LATERAL (
          SELECT availability
          FROM container_status
          WHERE cid = (cd_obj->'container'->>'cid')::int
          ORDER BY sid DESC LIMIT 1
        ) cs ON true
        WHERE (cd_obj->'container'->>'cid') ~ '^[0-9]+$'
      ), '[]'::jsonb)
    `;

    const shippingDetailsAgg = `
      (SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
          'id',              oi.id,
          'category',        COALESCE(oi.category, ''),
          'subcategory',     COALESCE(oi.subcategory, ''),
          'type',            COALESCE(oi.type, ''),
          'totalNumber',     ${safeIntCast("oi.total_number")},
          'weight',          ${safeNumericCast("oi.weight")},
          'itemRef',         COALESCE(oi.item_ref, ''),
          'containerDetails', ${containerDetailsSub},
          'remainingItems',  ${safeIntCast(`
            oi.total_number - (
              SELECT COALESCE(SUM((cd_obj->>'assign_total_box')::int), 0)
              FROM jsonb_array_elements(COALESCE(oi.container_details, '[]'::jsonb)) cd_obj
            )
          `)}
        ) ORDER BY oi.id
      ), '[]'::jsonb)
      FROM order_items oi 
      WHERE oi.receiver_id = r.id
      AND oi.order_id = o.id)
    `;

    const query = `
      SELECT 
        o.id AS order_id,
        o.booking_ref,
        o.rgl_booking_number,
        s.sender_contact,
        s.sender_email,
        t.collection_scope,

        (SELECT COALESCE(jsonb_agg(r_full ORDER BY r_full.id), '[]'::jsonb) 
         FROM (
           SELECT 
             r.id,
             r.receiver_name               AS "receiverName",
             r.receiver_address            AS "receiverAddress",
             r.receiver_email              AS "receiverEmail",
             r.containers,
             r.status,
             r.eta                         AS eta,
             ${shippingDetailsAgg}                  AS "shippingDetails",
             COALESCE((
               SELECT jsonb_agg(
                 jsonb_build_object(
                   'drop_method',    dod.drop_method,
                   'dropoff_name',   dod.dropoff_name,
                   'drop_off_mobile',dod.drop_off_mobile,
                   'plate_no',       dod.plate_no,
                   'drop_date',      TO_CHAR(dod.drop_date, 'YYYY-MM-DD')
                 ) ORDER BY dod.id
               ) FROM drop_off_details dod 
               WHERE dod.receiver_id = r.id
             ), '[]'::jsonb) AS "dropOffDetails",
             COALESCE((
               SELECT jsonb_agg(
                 jsonb_build_object(
                   'status',      ct.new_status,
                   'time',        ct."timestamp",
                   'old_status',  ct.old_status,
                   'event_type',  ct.event_type,
                   'reason',      ct.reason,
                   'location',    ct.location,
                   'notes',       ct.details
                 ) ORDER BY ct."timestamp" DESC
               ) FROM consignment_tracking ct
               WHERE ct.consignment_id = c.id
                 AND ct.event_type IN ('status_advanced','status_updated','order_synced','status_auto_updated')
             ), '[]'::jsonb) AS "status_history"
           FROM receivers r
           LEFT JOIN consignments c ON (
             c.orders @> jsonb_build_array(o.id::text)
             OR c.orders @> jsonb_build_array(o.id)
             OR c.orders ? o.id::text
           )
           WHERE r.order_id = o.id
         ) r_full) AS receivers
      FROM orders o
      LEFT JOIN senders s ON s.order_id = o.id
      LEFT JOIN transport_details t ON t.order_id = o.id
      WHERE ${isNumeric ? "o.id = $1" : "o.booking_ref ILIKE $1"}
         OR o.booking_ref ILIKE $2
      LIMIT 5
    `;

    const params = isNumeric
      ? [Number(search), `%${search}%`]
      : [`%${search}%`, `%${search}%`];

    const { rows } = await pool.query(query, params);

    if (rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: `No order found for reference "${search}"`,
      });
    }

    const formatted = rows.map((row) => {
      let receivers = row.receivers || "[]";
      if (typeof receivers === "string") {
        try {
          receivers = JSON.parse(receivers);
        } catch {
          receivers = [];
        }
      }

      return {
        id: row.order_id,
        booking_ref: row.booking_ref,
        rgl_booking_number: row.rgl_booking_number || row.booking_ref,
        sender_contact: row.sender_contact || null,
        sender_email: row.sender_email || null,
        collection_scope: row.collection_scope || "Partial",
        receivers,
      };
    });

    const data = formatted.length === 1 ? formatted[0] : formatted;

    res.json({
      success: true,
      data,
    });
  } catch (err) {
    console.error("getOrderByOrderId error:", err.stack || err);
    res.status(500).json({
      success: false,
      message: "Failed to fetch order details",
      details: process.env.NODE_ENV === "development" ? err.message : undefined,
    });
  }
}

export async function getOrderByRglBookingNo(req, res) {
  const { rglBookingNo } = req.params;

  if (
    !rglBookingNo ||
    typeof rglBookingNo !== "string" ||
    rglBookingNo.trim() === ""
  ) {
    return res.status(400).json({
      success: false,
      message: "RGL Booking Number is required",
    });
  }

  const search = rglBookingNo.trim();

  try {
    const safeIntCast = (val) => `COALESCE(${val}, 0)`;
    const safeNumericCast = (val) => `COALESCE(${val}, 0)`;

    const containerDetailsSub = `
      COALESCE((
        SELECT jsonb_agg(
          jsonb_build_object(
            'status',      COALESCE(cs.availability, 'Created'),
            'container', jsonb_build_object(
              'container_number', COALESCE(cd_obj->'container'->>'container_number', 'Unknown')
            ),
            'assign_weight',   COALESCE(cd_obj->>'assign_weight', '0')::text,
            'assign_total_box',COALESCE(cd_obj->>'assign_total_box', '0')::text
          ) ORDER BY (cd_obj->'container'->>'container_number')
        )
        FROM jsonb_array_elements(COALESCE(oi.container_details, '[]'::jsonb)) cd_obj
        LEFT JOIN LATERAL (
          SELECT availability
          FROM container_status
          WHERE cid = (cd_obj->'container'->>'cid')::int
          ORDER BY sid DESC LIMIT 1
        ) cs ON true
        WHERE (cd_obj->'container'->>'cid') ~ '^[0-9]+$'
      ), '[]'::jsonb)
    `;

    const shippingDetailsAgg = `
      (SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
          'id',              oi.id,
          'category',        COALESCE(oi.category, ''),
          'subcategory',     COALESCE(oi.subcategory, ''),
          'type',            COALESCE(oi.type, ''),
          'totalNumber',     ${safeIntCast("oi.total_number")},
          'weight',          ${safeNumericCast("oi.weight")},
          'itemRef',         COALESCE(oi.item_ref, ''),
          'containerDetails', ${containerDetailsSub},
          'remainingItems',  ${safeIntCast(`
            oi.total_number - (
              SELECT COALESCE(SUM((cd_obj->>'assign_total_box')::int), 0)
              FROM jsonb_array_elements(COALESCE(oi.container_details, '[]'::jsonb)) cd_obj
            )
          `)}
        ) ORDER BY oi.id
      ), '[]'::jsonb)
      FROM order_items oi 
      WHERE oi.receiver_id = r.id
      AND oi.order_id = o.id)
    `;

    const query = `
      SELECT 
        o.id AS order_id,
        o.booking_ref,
        o.rgl_booking_number,
        s.sender_contact,
        s.sender_email,
        t.collection_scope,

        (SELECT COALESCE(jsonb_agg(r_full ORDER BY r_full.id), '[]'::jsonb) 
         FROM (
           SELECT 
             r.id,
             r.receiver_name               AS "receiverName",
             r.receiver_address            AS "receiverAddress",
             r.receiver_email              AS "receiverEmail",
             r.containers,
             r.status,
             r.eta                         AS eta,
             ${shippingDetailsAgg}                  AS "shippingDetails",
             COALESCE((
               SELECT jsonb_agg(
                 jsonb_build_object(
                   'drop_method',    dod.drop_method,
                   'dropoff_name',   dod.dropoff_name,
                   'drop_off_mobile',dod.drop_off_mobile,
                   'plate_no',       dod.plate_no,
                   'drop_date',      TO_CHAR(dod.drop_date, 'YYYY-MM-DD')
                 ) ORDER BY dod.id
               ) FROM drop_off_details dod 
               WHERE dod.receiver_id = r.id
             ), '[]'::jsonb) AS "dropOffDetails",
             COALESCE((
               SELECT jsonb_agg(
                 jsonb_build_object(
                   'status',      ct.new_status,
                   'time',        ct."timestamp",
                   'old_status',  ct.old_status,
                   'event_type',  ct.event_type,
                   'reason',      ct.reason,
                   'location',    ct.location,
                   'notes',       ct.details
                 ) ORDER BY ct."timestamp" DESC
               ) FROM consignment_tracking ct
               WHERE ct.consignment_id = c.id
                 AND ct.event_type IN ('status_advanced','status_updated','order_synced','status_auto_updated')
             ), '[]'::jsonb) AS "status_history"
           FROM receivers r
           LEFT JOIN consignments c ON (
             c.orders @> jsonb_build_array(o.id::text)
             OR c.orders @> jsonb_build_array(o.id)
             OR c.orders ? o.id::text
           )
           WHERE r.order_id = o.id
         ) r_full) AS receivers
      FROM orders o
      LEFT JOIN senders s ON s.order_id = o.id
      LEFT JOIN transport_details t ON t.order_id = o.id
      WHERE o.rgl_booking_number ILIKE $1
      LIMIT 5
    `;

    const { rows } = await pool.query(query, [`%${search}%`]);

    if (rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: `No order found for RGL Booking Number "${search}"`,
      });
    }

    const formatted = rows.map((row) => {
      let receivers = row.receivers || "[]";
      if (typeof receivers === "string") {
        try {
          receivers = JSON.parse(receivers);
        } catch (e) {
          console.warn(`Failed to parse receivers for order ${row.order_id}`);
          receivers = [];
        }
      }

      return {
        id: row.order_id,
        booking_ref: row.booking_ref,
        rgl_booking_number: row.rgl_booking_number,
        sender_contact: row.sender_contact || null,
        sender_email: row.sender_email || null,
        collection_scope: row.collection_scope || "Partial",
        receivers,
      };
    });

    const data = formatted.length === 1 ? formatted[0] : formatted;

    res.json({
      success: true,
      data,
    });
  } catch (err) {
    console.error("getOrderByRglBookingNo error:", err.stack || err);
    res.status(500).json({
      success: false,
      message: "Failed to fetch order details",
      details: process.env.NODE_ENV === "development" ? err.message : undefined,
    });
  }
}

export async function getOrderUsageHistory(req, res) {
  try {
    const { order_id } = req.params || req.query;

    if (!order_id) {
      return res.status(400).json({ error: "order_id is required" });
    }

    const query = `
      SELECT 
        ot.created_time AS usage_time,
        ot.status AS usage_status,
        ot.created_by AS updated_by,
        t.driver_name, t.truck_number, t.drop_date AS transport_date,
        r.receiver_name, r.consignment_number
      FROM order_tracking ot
      LEFT JOIN transport_details t ON ot.order_id = t.order_id
      LEFT JOIN receivers r ON ot.receiver_id = r.id
      WHERE ot.order_id = $1
      ORDER BY ot.created_time ASC
    `;

    const result = await pool.query(query, [order_id]);
    const history = result.rows;

    res.json({ history });
  } catch (err) {
    console.error("Error fetching order usage history:", err.message);
    res.status(500).json({
      error: "Failed to fetch order usage history",
      details: err.message,
    });
  }
}

export const getAssignedOrderById = async (req, res) => {
  let client;

  try {
    const { id } = req.params;
    const numericId = parseInt(id, 10);

    if (!numericId || numericId <= 0) {
      return res.status(400).json({
        error: "Invalid order ID",
      });
    }

    client = await pool.connect();

    const orderResult = await client.query(
      `
      SELECT
        id,
        booking_ref,
        place_of_loading
      FROM orders
      WHERE id = $1
      `,
      [numericId],
    );

    if (!orderResult.rowCount) {
      return res.status(404).json({
        error: "Order not found",
      });
    }

    const containerDetailsSub = `
      COALESCE(
        (
          SELECT json_agg(
            json_build_object(
              'assign_total_box',
              COALESCE(elem->>'assign_total_box', '0'),

              'assign_weight',
              COALESCE(elem->>'assign_weight', '0'),

              'container',
              json_build_object(
                'cid',
                (elem->'container'->>'cid')::int,

                'container_number',
                COALESCE(
                  cm.container_number,
                  elem->'container'->>'container_number',
                  ''
                )
              )
            )
          )
          FROM jsonb_array_elements(
            COALESCE(oi.container_details, '[]'::jsonb)
          ) elem

          LEFT JOIN container_master cm
            ON cm.cid = (elem->'container'->>'cid')::int

          WHERE (elem->'container'->>'cid') ~ '^[0-9]+$'
        ),
        '[]'::json
      )
    `;

    const receiversResult = await client.query(
      `
      SELECT
        r.id,
        r.receiver_name,
        r.receiver_contact,
        r.receiver_email,

        COALESCE(r.total_number,0)::int AS total_number,

        (
          SELECT json_agg(
            json_build_object(
              'id', oi.id,

              'category',
              COALESCE(oi.category,''),

              'deliveryAddress',
              COALESCE(oi.delivery_address,''),

              'totalNumber',
              COALESCE(oi.total_number,0)::int,

              'weight',
              COALESCE(oi.weight,0)::numeric,

              'remainingItems',
              GREATEST(
                0,
                COALESCE(oi.total_number,0)::int -
                COALESCE(
                  (
                    SELECT SUM(
                      (cd->>'assign_total_box')::int
                    )
                    FROM jsonb_array_elements(
                      COALESCE(
                        oi.container_details,
                        '[]'::jsonb
                      )
                    ) cd
                  ),
                  0
                )
              ),

              'containerDetails',
              ${containerDetailsSub}
            )
            ORDER BY oi.id
          )
          FROM order_items oi
          WHERE oi.receiver_id = r.id
        ) AS shippingdetails

      FROM receivers r
      WHERE r.order_id = $1
      ORDER BY r.id
      `,
      [numericId],
    );

    return res.status(200).json({
      id: orderResult.rows[0].id,
      booking_ref: orderResult.rows[0].booking_ref,
      place_of_loading: orderResult.rows[0].place_of_loading,
      receivers: receiversResult.rows.map((r) => ({
        ...r,
        shippingdetails: r.shippingdetails || [],
      })),
    });
  } catch (err) {
    console.error("getAssignedOrderById:", err);

    return res.status(500).json({
      error: "Failed to fetch assigned order",
      details: err.message,
    });
  } finally {
    if (client) client.release();
  }
};
