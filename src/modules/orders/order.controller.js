import pool from "../../db/pool.js";
import sendOrderEmail from "../../middleware/nodeMailer.js";
import { withUserAudit } from "../../middleware/dbAudit.js";
import logger from "../../services/logger.js";
import {
  calculateETA,
  computeDaysUntilEta,
} from "../../services/calculateEta.js";
import { moveReceiverToNextStatus } from "../../services/moveReceiverToNextStatus.js";
import { createOrderTracking } from "../../services/createOrderTracking.js";

function normalizeDate(dateStr) {
  if (!dateStr) return null;
  const date = new Date(dateStr);
  return isNaN(date.getTime()) ? null : date.toISOString().split("T")[0];
}

function isValidDate(dateStr) {
  return !isNaN(Date.parse(dateStr));
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

      const recResult = await withUserAudit(
        req,
        `INSERT INTO receivers (
          order_id, receiver_name, receiver_contact, receiver_address, receiver_email,
          receiver_marks_and_number, eta, etd, shipping_line,
          consignment_vessel, consignment_number, consignment_marks, consignment_voyage,
          total_number, total_weight, remarks, containers, status, full_partial, qty_delivered
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
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
          req.user?.username || req.user?.email || req.user?.id || "system",
          itemRef,
        ]);
      }

      for (const row of trackingRows) {
        await client.query(
          `INSERT INTO order_tracking (order_id, sender_id, sender_ref, receiver_id, status, created_by, item_ref)
           VALUES ($1,$2,$3,$4,$5,$6,$7)`,
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

    console.log("[updateOrder] Starting update for order ID:", id);
    console.log("[updateOrder] Received body keys:", Object.keys(updates));

    // ── 1. Fetch current state ─────────────────────────────────────
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

    // ── 2. Parse incoming data ─────────────────────────────────────
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

    // ── 3. Attachments Handling ───────────────────────────────────
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

    // ── 4. Gatepass Handling ──────────────────────────────────────
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

    // ── 5. Upsert Receivers + Order Items ─────────────────────────
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

      // Order Items for this receiver
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

    // ── 6. Drop-off Details (Clear old + Insert new) ─────────────────────
    if (incomingDropOffs.length > 0) {
      hasAnyChange = true;

      // First, delete old drop-off details for this order (recommended)
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

    // ── 7. Update Transport Details ─────────────────────────────────────
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

    // ── 8. Update Orders Table ─────────────────────────────────────
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
      // sender_ref, sender_remarks etc. can be added here if needed
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

    // ── 9. COMMIT ─────────────────────────────────────────────────
    await client.query("COMMIT");

    // Refetch updated data
    const updatedOrder = await client.query(
      "SELECT * FROM orders WHERE id = $1",
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

        COALESCE(receiver_data.total_items, 0) AS total_items,
        COALESCE(receiver_data.total_weight, 0) AS total_weight

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

                      'containerDetails',
                      COALESCE(
                        (
                          SELECT jsonb_agg(
                            jsonb_build_object(
                              'container',
                              jsonb_build_object(
                                'cid',
                                (cd->'container'->>'cid')::int,

                                'container_number',
                                cd->'container'->>'container_number'
                              ),

                              'status',
                              COALESCE(cs.availability,'Created'),

                              'assign_total_box',
                              COALESCE(cd->>'assign_total_box','0'),

                              'assign_weight',
                              COALESCE(cd->>'assign_weight','0'),

                              'remaining_items',
                              COALESCE(cd->>'remaining_items','0'),

                              'total_number',
                              COALESCE((cd->>'total_number')::int,0)
                            )
                          )
                          FROM jsonb_array_elements(
                            COALESCE(oi.container_details,'[]'::jsonb)
                          ) cd

                          LEFT JOIN LATERAL (
                            SELECT availability
                            FROM container_status
                            WHERE cid = (cd->'container'->>'cid')::int
                            ORDER BY sid DESC
                            LIMIT 1
                          ) cs ON true
                        ),
                        '[]'::jsonb
                      )
                    )
                    ORDER BY oi.id
                  )
                  FROM order_items oi
                  WHERE oi.receiver_id = r.id
                ),
                '[]'::jsonb
              )
            )
            ORDER BY r.id
          ) AS receivers,

          COALESCE(SUM(oi.total_number),0) AS total_items,
          COALESCE(SUM(oi.weight),0) AS total_weight

        FROM receivers r

        LEFT JOIN order_items oi
          ON oi.receiver_id = r.id

        WHERE r.order_id = o.id

      ) receiver_data ON true

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

export async function getMyOrdersByRef(req, res) {
  try {
    const userId = req.user.sub;
    if (!userId) {
      logger.warn("Unauthorized access attempt — no user ID in token");
      return res.status(401).json({ error: "User ID not found in token" });
    }

    const { limit = 20, offset = 0, status, search } = req.query;
    const safeLimit = Math.min(50, Math.max(1, parseInt(limit) || 20));
    const safeOffset = parseInt(offset) || 0;

    logger.debug("Fetching orders", {
      userId,
      limit: safeLimit,
      offset: safeOffset,
      status: status || null,
      hasSearch: !!search,
    });

    let whereClauses = ["o.user_id = $1"];
    let params = [userId];

    if (status) {
      whereClauses.push(`o.status = $${params.length + 1}`);
      params.push(status);
    }

    if (search && search.trim()) {
      const term = `%${search.trim()}%`;
      whereClauses.push(`(
        o.booking_ref ILIKE $${params.length + 1} OR
        o.rgl_booking_number ILIKE $${params.length + 1} OR
        EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id AND oi.item_ref ILIKE $${params.length + 1})
      )`);
      params.push(term);
    }

    const whereSql =
      whereClauses.length > 0 ? "WHERE " + whereClauses.join(" AND ") : "";

    const ordersQuery = `
      SELECT 
        o.*,
        s.sender_name, s.sender_contact, s.sender_email, s.sender_address,
        t.transport_type, t.driver_name, t.driver_contact, t.truck_number, t.drop_method, t.delivery_date,
        ot.status AS latest_tracking_status,
        ot.created_time AS latest_tracking_time,
        (SELECT json_agg(
           json_build_object(
             'id', r.id,
             'receiver_name', r.receiver_name,
             'receiver_contact', r.receiver_contact,
             'receiver_email', r.receiver_email,
             'receiver_address', r.receiver_address,
             'status', r.status,
             'eta', r.eta,
             'containers', r.containers
           )
           ORDER BY r.id
         ) FROM receivers r WHERE r.order_id = o.id) AS receivers_summary,
        (SELECT json_agg(
           json_build_object(
             'item_id', oi.id,
             'item_ref', oi.item_ref,
             'category', oi.category,
             'subcategory', oi.subcategory,
             'type', oi.type,
             'total_number', oi.total_number,
             'weight', oi.weight,
             'assigned_qty', COALESCE((
               SELECT SUM(cah.assigned_qty)
               FROM container_assignment_history cah
               WHERE cah.detail_id = oi.id
             ), 0),
             'remaining', oi.total_number - COALESCE((
               SELECT SUM(cah.assigned_qty)
               FROM container_assignment_history cah
               WHERE cah.detail_id = oi.id
             ), 0)
           )
           ORDER BY oi.id
         ) FROM order_items oi WHERE oi.order_id = o.id) AS items_summary
      FROM orders o
      LEFT JOIN senders s ON s.order_id = o.id
      LEFT JOIN transport_details t ON t.order_id = o.id
      LEFT JOIN LATERAL (
        SELECT status, created_time
        FROM order_tracking
        WHERE order_id = o.id
        ORDER BY created_time DESC LIMIT 1
      ) ot ON true
      ${whereSql}
      ORDER BY o.created_at DESC
      LIMIT $${params.length + 1} OFFSET $${params.length + 2}
    `;

    params.push(safeLimit, safeOffset);

    const ordersResult = await pool.query(ordersQuery, params);

    if (ordersResult.rowCount === 0) {
      logger.debug("No orders found", { userId, status: status || null });
      return res.json({
        success: true,
        data: [],
        message: "No orders found",
        count: 0,
      });
    }

    const enriched = ordersResult.rows.map((row) => ({
      ...row,
      overall_status: row.latest_tracking_status || row.status || "Created",
    }));

    const countQuery = `SELECT COUNT(*) FROM orders o ${whereSql}`;
    const countRes = await pool.query(countQuery, params.slice(0, -2));
    const total = parseInt(countRes.rows[0].count);

    logger.info("Orders fetched successfully", {
      userId,
      returned: enriched.length,
      total,
      limit: safeLimit,
      offset: safeOffset,
    });

    res.json({
      success: true,
      data: enriched,
      pagination: {
        total,
        limit: safeLimit,
        offset: safeOffset,
        pages: Math.ceil(total / safeLimit),
      },
    });
  } catch (err) {
    logger.error("getMyOrdersByRef failed", err);
    res.status(500).json({
      success: false,
      message: "Failed to fetch your orders",
    });
  }
}

export async function getOrderByReference(req, res) {
  try {
    const { ref, limit = 10, offset = 0, status, search } = req.query;

    if (!ref || typeof ref !== "string" || ref.trim().length < 3) {
      return res.status(400).json({
        success: false,
        message:
          "Valid booking reference (ref) is required in query parameters",
      });
    }

    const safeLimit = Math.min(20, Math.max(1, parseInt(limit) || 10)); // smaller default for public endpoint
    const safeOffset = parseInt(offset) || 0;
    const refClean = ref.trim();

    // ────────────────────────────────────────────────
    // Base WHERE clause – reference-based + optional filters
    // ────────────────────────────────────────────────
    let whereClauses = [`(o.booking_ref = $1 OR o.rgl_booking_number = $1)`];
    let params = [refClean];

    if (status) {
      whereClauses.push(`o.status = $${params.length + 1}`);
      params.push(status);
    }

    // Optional extra search (on top of ref) – e.g. item refs
    if (search && search.trim()) {
      const term = `%${search.trim()}%`;
      whereClauses.push(`(
        EXISTS (SELECT 1 FROM order_items oi 
                WHERE oi.order_id = o.id AND oi.item_ref ILIKE $${params.length + 1})
      )`);
      params.push(term);
    }

    const whereSql = "WHERE " + whereClauses.join(" AND ");

    // ────────────────────────────────────────────────
    // Main query – same rich structure as before
    // ────────────────────────────────────────────────
    const ordersQuery = `
      SELECT 
        o.*,
        s.sender_name, s.sender_contact, s.sender_email, s.sender_address,
        t.transport_type, t.driver_name, t.driver_contact, t.truck_number, t.drop_method, t.delivery_date,
        ot.status AS latest_tracking_status,
        ot.created_time AS latest_tracking_time,
        -- Receiver summary
        (SELECT json_agg(
           json_build_object(
             'id', r.id,
             'receiver_name', r.receiver_name,
             'receiver_contact', r.receiver_contact,
             'receiver_email', r.receiver_email,
             'receiver_address', r.receiver_address,
             'status', r.status,
             'eta', r.eta,
             'containers', r.containers
           )
           ORDER BY r.id
         ) FROM receivers r WHERE r.order_id = o.id) AS receivers_summary,
        -- Item summary with container assignments
        (SELECT json_agg(
           json_build_object(
             'item_id', oi.id,
             'item_ref', oi.item_ref,
             'category', oi.category,
             'subcategory', oi.subcategory,
             'type', oi.type,
             'total_number', oi.total_number,
             'weight', oi.weight,
             'assigned_qty', COALESCE((
               SELECT SUM(cah.assigned_qty)
               FROM container_assignment_history cah
               WHERE cah.detail_id = oi.id
             ), 0),
             'remaining', oi.total_number - COALESCE((
               SELECT SUM(cah.assigned_qty)
               FROM container_assignment_history cah
               WHERE cah.detail_id = oi.id
             ), 0)
           )
           ORDER BY oi.id
         ) FROM order_items oi WHERE oi.order_id = o.id) AS items_summary
      FROM orders o
      LEFT JOIN senders s ON s.order_id = o.id
      LEFT JOIN transport_details t ON t.order_id = o.id
      LEFT JOIN LATERAL (
        SELECT status, created_time
        FROM order_tracking
        WHERE order_id = o.id
        ORDER BY created_time DESC LIMIT 1
      ) ot ON true
      ${whereSql}
      ORDER BY o.created_at DESC
      LIMIT $${params.length + 1} OFFSET $${params.length + 2}
    `;

    params.push(safeLimit, safeOffset);

    const ordersResult = await pool.query(ordersQuery, params);

    const data = ordersResult.rows.map((row) => ({
      ...row,
      overall_status: row.latest_tracking_status || row.status || "Created",
    }));

    // Total count (useful if allowing multiple matches)
    const countQuery = `SELECT COUNT(*) FROM orders o ${whereSql}`;
    const countRes = await pool.query(countQuery, params.slice(0, -2));
    const total = parseInt(countRes.rows[0].count);

    res.json({
      success: true,
      data,
      pagination: {
        total,
        limit: safeLimit,
        offset: safeOffset,
        pages: Math.ceil(total / safeLimit),
      },
      message:
        data.length === 0 ? "No order found for this reference" : undefined,
    });
  } catch (err) {
    console.error("getOrderByReference error:", err);
    res.status(500).json({
      success: false,
      message: "Failed to fetch order details",
      error: err.message,
    });
  }
}

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
        params.push(parseInt(consignment_id, 10));
        whereClause += ` AND (
          o.created_at::date = CURRENT_DATE
          OR o.created_at::date = (
            SELECT cch.released_at FROM container_consignment_history cch
            WHERE cch.consignment_id = $${params.length}
              AND cch.released_at IS NOT NULL
            ORDER BY cch.id DESC LIMIT 1
          )
          OR EXISTS (
            SELECT 1 FROM consignments c
            WHERE c.id = $${params.length} AND c.orders @> to_jsonb(o.id)
          )
        )`;
      } else {
        if (pol) {
          params.push(String(pol.trim()));
          whereClause += ` AND o.place_of_loading = $${params.length}`;
        }
        if (pod) {
          params.push(String(pod.trim()));
          whereClause += ` AND o.place_of_delivery = $${params.length}`;
        }
        whereClause += ` AND EXISTS (
          SELECT 1
          FROM container_assignment_history ch
          WHERE ch.order_id = o.id
            AND COALESCE(ch.status, 'Ready for Loading') IN (
              'Ready for Loading',
              'Loaded'
            )
        )`;
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
        'totalNumber', COALESCE(oi.total_number, 0),
        'weight', COALESCE(oi.weight, 0),
        'containerDetails',${containerDetailsSub},
        'remainingItems',  COALESCE(oi.total_number, 0) - COALESCE((
            SELECT SUM((cd_obj->>'assign_total_box')::int)
            FROM jsonb_array_elements(COALESCE(oi.container_details, '[]'::jsonb)) cd_obj
          ), 0)
      ) ORDER BY oi.id), '[]'::json)
      FROM order_items oi WHERE oi.receiver_id = r.id)
    `;

    const receiversSub = `
      (SELECT COALESCE(json_agg(rf ORDER BY rf.id), '[]'::json) FROM (
        SELECT
          r.id,
          r.receiver_name  AS receivername,
          r.status,
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

    console.log("[getOrderById] Raw ID received:", id);

    const numericId = parseInt(id, 10);
    if (isNaN(numericId) || numericId <= 0) {
      console.warn(`[getOrderById] Invalid ID format: "${id}"`);
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
            'consignmentStatus', COALESCE(oi.consignment_status, ''),
            'shippingLine',      COALESCE(oi.shipping_line, ''),
            'containerDetails',  ${containerDetailsSub},
            'remainingItems',    GREATEST(0, 
              COALESCE(oi.total_number, 0)::int - COALESCE((
                SELECT SUM((cd->>'assign_total_box')::int)
                FROM jsonb_array_elements(COALESCE(oi.container_details, '[]'::jsonb)) cd
              ), 0)
            )::int
          ) ORDER BY oi.id
        ) AS shippingdetails
        FROM order_items oi
        WHERE oi.receiver_id = r.id
      ) sd_full ON true
      WHERE r.order_id = $1
      ORDER BY r.id
    `;

    const receiversResult = await client.query(receiversQuery, [numericId]);

    const STATUS_MAP = {
      "under processing": "Under Processing",
      "ready for loading": "Ready for Loading",
      "loaded into container": "Loaded Into Container",
      "shipment processing": "Shipment Processing",
      "shipment in transit": "Shipment In Transit",
      "arrived at facility": "Arrived at Facility",
      "ready for delivery": "Ready for Delivery",
      "shipment delivered": "Shipment Delivered",
      "order created": "Order Created",
      created: "Order Created",
      occupied: "",
    };

    let receivers = receiversResult.rows.map((row) => {
      console.log(
        `[getOrderById] Receiver ${row.id} - marksAndNumber from DB:`,
        row.marksAndNumber,
      );
      console.log("Receiver:", row.id);
      console.log("ETA:", row.eta);
      console.log("ETD:", row.etd);

      const normalizedStatus =
        STATUS_MAP[row.status?.toLowerCase()?.trim()] ??
        row.status ??
        "Order Created";

      return {
        ...row,
        status: normalizedStatus,
        marksAndNumber: row.marksAndNumber || "",
        receiverMarksNumber: row.marksAndNumber || "",
        shippingDetails: row.shippingdetails || [],
        containers: (() => {
          try {
            return typeof row.containers === "string"
              ? JSON.parse(row.containers)
              : row.containers || [];
          } catch (e) {
            console.warn(
              `[getOrderById] Invalid containers JSON for receiver ${row.id}:`,
              e.message,
            );
            return [];
          }
        })(),
        eta: row.eta ? new Date(row.eta).toISOString().split("T")[0] : "",

        etd: row.etd ? new Date(row.etd).toISOString().split("T")[0] : "",
      };
    });

    // Enrich container numbers (optimized - only used ones)
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
      console.warn("[getOrderById] Failed to parse attachments:", e);
    }

    let parsedGatepass = [];
    try {
      parsedGatepass =
        typeof orderRow.gatepass === "string"
          ? JSON.parse(orderRow.gatepass)
          : orderRow.gatepass || [];
    } catch (e) {
      console.warn("[getOrderById] Failed to parse gatepass:", e);
    }

    // Normalize order-level dates
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

    // Overall status calculation
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

    // Overall earliest ETA (among receivers with containers)
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

const CONSIGNMENT_TO_STATUS_MAP = {
  // Consignment Status          → Container Status          → Shipment (Receiver) Status      → ETA from DB
  "Customs Cleared": {
    container: "Shipment Processing",
    shipment: "Shipment Processing",
  }, // 7 days
  "Submitted On Vessel": {
    container: "Shipment Processing",
    shipment: "Shipment Processing",
  }, // 7 days
  Submitted: {
    container: "Shipment Processing",
    shipment: "Shipment Processing",
  }, // 7 days (if needed)
  "In Transit": { container: "In Transit", shipment: "Shipment In Transit" }, // 4 days
  "Ready for Delivery": {
    container: "Ready for Delivery",
    shipment: "Ready for Delivery",
  }, // 0 days
  "Arrived at Destination": {
    container: "Under Processing",
    shipment: "Under Processing",
  }, // 2 days
  Loaded: { container: "Loaded", shipment: "Loaded Into Container" }, // 9 days
  "Ready for loading": {
    container: "Ready for Loading",
    shipment: "Ready for Loading",
  }, // 12 days
  Created: { container: "Created", shipment: "Order Created" }, // 15 days (or 'Created' → 15)
  Arrived: {
    container: "Arrived at Facility",
    shipment: "Arrived at Facility",
  }, // 1 day
  "De-Linked": {
    container: "Arrived at Sort Facility",
    shipment: "Arrived at Sort Facility",
  }, // 1 day
  Delivered: { container: "Delivered", shipment: "Shipment Delivered" }, // 0 days
};
// Helper: Wrap in transaction
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

// Helper: Send notification (placeholder—integrate with your GAS/notifications module)
async function sendNotification(consignmentData, event = "created") {
  // e.g., await emailService.send({ to: consignmentData.consignee.email, subject: `Consignment ${consignmentData.consignment_number} ${event}` });
  console.log(
    `Notification sent for consignment ${consignmentData.consignment_number}: ${event}`,
  );
}

// Unified logging function: Handles both 'logToTracking' and 'safeLogToTracking' calls
async function logToTracking(
  client,
  consignmentId,
  eventType = "unknown",
  logData = {},
) {
  // Validate eventType (required, non-null)
  if (!eventType || typeof eventType !== "string" || eventType.trim() === "") {
    console.error(
      `Invalid eventType '${eventType}' for consignment ${consignmentId} – defaulting to 'unknown_event'`,
    );
    eventType = "unknown_event"; // Fallback to avoid NULL violation
  }

  // Validate against schema CHECK (expand as needed)
  const validEvents = [
    "status_advanced",
    "status_updated",
    "status_auto_updated",
    "updated",
    "order_synced",
  ];
  if (!validEvents.includes(eventType)) {
    console.warn(
      `Event '${eventType}' not in DB CHECK – add to constraint or use valid one`,
    );
  }

  try {
    // Normalize logData
    const {
      from: oldStatus = null,
      to: newStatus = null,
      offsetDays = 0,
      reason = null,
      ...extraDetails
    } = logData;

    const details = {
      ...extraDetails,
      old_status: oldStatus,
      new_status: newStatus,
      reason,
      action: logData.action || eventType, // Legacy: Store 'action' in details if passed
    };

    const query = `
      INSERT INTO consignment_tracking (
        consignment_id, event_type, old_status, new_status, offset_days, details
      ) VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT (consignment_id, event_type, timestamp) DO NOTHING
      RETURNING id
    `;
    const result = await client.query(query, [
      consignmentId,
      eventType.trim(), // Ensure non-null string
      oldStatus,
      newStatus,
      offsetDays,
      details,
    ]);

    if (result.rowCount > 0) {
      console.log(
        `✓ Logged '${eventType}' for ${consignmentId} (ID: ${result.rows[0].id})`,
      );
      return { success: true, id: result.rows[0].id };
    } else {
      console.log(`⚠ Duplicate '${eventType}' skipped for ${consignmentId}`);
      return { success: true, skipped: true };
    }
  } catch (error) {
    console.error(`Failed to log '${eventType}' for ${consignmentId}:`, error);
    if (error.code === "23502") {
      console.error("NOT NULL violation on event_type – ensure non-null param");
    } else if (error.code === "23514") {
      console.error(
        `CHECK violation: '${eventType}' not allowed – update DB constraint`,
      );
    }
    return { success: false, error: error.message };
    // No throw – keep tx alive
  }
}
async function safeLogToTracking(
  client,
  consignmentId,
  eventType,
  logData = {},
) {
  // Validate event_type against schema CHECK (optional, but prevents 23514 errors)
  const validEvents = [
    "status_advanced",
    "status_updated",
    "order_synced",
    "status_auto_updated",
  ]; // Sync with DB
  if (!validEvents.includes(eventType)) {
    console.warn(
      `Invalid event_type '${eventType}' – add to DB CHECK constraint`,
    );
    return { success: false, reason: "Invalid event" };
  }
  try {
    // Normalize: Use eventType as event_type; ignore/rename 'action' if present
    const {
      from: oldStatus = null,
      to: newStatus = null,
      offsetDays = 0,
      reason = null,
      action, // Ignore if passed; use eventType
      ...extraDetails
    } = logData;

    const details = {
      ...extraDetails,
      old_status: oldStatus,
      new_status: newStatus,
      reason,
      action: action || eventType, // Legacy: Store in details if needed
    };

    const query = `
      INSERT INTO consignment_tracking (
        consignment_id, event_type, old_status, new_status, offset_days, details
      ) VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT (consignment_id, event_type, timestamp) DO NOTHING
      RETURNING id
    `;
    const result = await client.query(query, [
      consignmentId,
      eventType, // Use this for event_type (e.g., 'status_auto_updated')
      oldStatus,
      newStatus,
      offsetDays,
      details,
    ]);

    if (result.rowCount > 0) {
      console.log(
        `✓ Logged '${eventType}' for ${consignmentId} (ID: ${result.rows[0].id})`,
      );
      return { success: true, id: result.rows[0].id };
    } else {
      console.log(`⚠ Duplicate '${eventType}' skipped for ${consignmentId}`);
      return { success: true, skipped: true };
    }
  } catch (error) {
    console.error(`Failed to log '${eventType}' for ${consignmentId}:`, error);
    if (error.code === "42703") {
      console.error(
        'Schema mismatch – check INSERT columns vs. table (e.g., no "action" column)',
      );
    }
    return { success: false, error: error.message };
    // No throw – non-critical
  }
}
// Helper to extract valid integer IDs from orderIds (handles array of IDs or array of objects; robust parsing)
function extractOrderIds(orderData) {
  if (!orderData || !Array.isArray(orderData)) {
    return [];
  }

  const ids = orderData
    .map((item) => {
      let id = null;
      if (typeof item === "number") {
        id = item;
      } else if (typeof item === "object" && item !== null) {
        if ("id" in item && item.id !== null && item.id !== undefined) {
          id = parseInt(item.id, 10);
        } else if ("value" in item || "key" in item) {
          const val = item.value || item.key;
          id = parseInt(val, 10);
        }
      } else if (typeof item === "string") {
        id = parseInt(item, 10);
      }
      return id;
    })
    .filter((id) => Number.isInteger(id) && id > 0); // Strict: integer and positive

  return ids;
}

// export async function advanceStatus(req, res) {
//   console.log("Advance Status Request Params:", req.params);
//   try {
//     const { id } = req.params;
//     const numericId = parseInt(id, 10);
//     if (isNaN(numericId) || numericId <= 0) {
//       return res.status(400).json({ error: 'Invalid consignment ID.' });
//     }

//     const { rows } = await pool.query('SELECT status FROM consignments WHERE id = $1', [numericId]);
//     if (rows.length === 0) {
//       return res.status(404).json({ error: 'Consignment not found' });
//     }

//     const currentStatus = rows[0].status;

//     // Your existing nextStatusMap (keep unchanged)
//     const nextStatusMap = {
//       'Drafts Cleared': 'Submitted On Vessel',
//       'Submitted On Vessel': 'Customs Cleared',
//       'Customs Cleared': 'Submitted',
//       'Submitted': 'Under Shipment Processing',
//       'Under Shipment Processing': 'In Transit',
//       'In Transit': 'Arrived at Facility',
//       'Arrived at Facility': 'Ready for Delivery',
//       'Ready for Delivery': 'Arrived at Destination',
//       'Arrived at Destination': 'Delivered',
//       // ... keep the rest
//     };

//     const nextStatus = nextStatusMap[currentStatus];
//     if (!nextStatus) {
//       return res.status(400).json({ error: `No next status from ${currentStatus}` });
//     }

//     const mapping = CONSIGNMENT_TO_STATUS_MAP[nextStatus];
//     if (!mapping) {
//       console.warn(`No mapping defined for consignment status: ${nextStatus}`);
//     }

//     let updateError = null;

//     await withTransaction(async (client) => {
//       try {
//         // 1. Update consignment status
//         let consignmentEta = null;
//         if (mapping && nextStatus !== 'Delivered') {
//           const etaResult = await calculateETA(client, mapping.shipment);
//           consignmentEta = etaResult.eta;
//         } else if (nextStatus === 'Delivered') {
//           consignmentEta = new Date().toISOString().split('T')[0];
//         }

//         await client.query(
//           'UPDATE consignments SET status = $1, eta = $2, updated_at = NOW() WHERE id = $3',
//           [nextStatus, consignmentEta, numericId]
//         );

//         // 2. Log tracking
//         await safeLogToTracking(client, numericId, 'status_advanced', {
//           from: currentStatus,
//           to: nextStatus,
//           newEta: consignmentEta,
//           reason: 'Manual advance'
//         });

//         // 3. Sync linked orders → receivers → containers
//         if (mapping) {
//           const orderIdsQuery = await client.query('SELECT orders FROM consignments WHERE id = $1', [numericId]);
//           let rawOrders = orderIdsQuery.rows[0]?.orders || [];
//           if (typeof rawOrders === 'string') rawOrders = JSON.parse(rawOrders || '[]');

//           const syncOrderIds = extractOrderIds(rawOrders)
//             .map(oid => parseInt(oid, 10))
//             .filter(oid => !isNaN(oid) && oid > 0);

//           if (syncOrderIds.length > 0) {
//             // Update orders (optional)
//             await client.query(
//               'UPDATE orders SET status = $1, updated_at = CURRENT_TIMESTAMP WHERE id = ANY($2::int[])',
//               [nextStatus, syncOrderIds]
//             );

//             // Update ALL receivers: correct shipment status
//             await client.query(
//               `UPDATE receivers
//                SET status = $1, updated_at = CURRENT_TIMESTAMP
//                WHERE order_id = ANY($2::int[])`,
//               [mapping.shipment, syncOrderIds]
//             );

//             // Recalculate ETA for each receiver using eta_config table
//             const etaResult = await calculateETA(client, mapping.shipment);

//             await client.query(
//               `UPDATE receivers
//                SET eta = $1, updated_at = CURRENT_TIMESTAMP
//                WHERE order_id = ANY($2::int[])`,
//               [etaResult.eta, syncOrderIds]
//             );

//             // Sync containers via existing function
//             const receiversResult = await client.query(
//               'SELECT id FROM receivers WHERE order_id = ANY($1::int[])',
//               [syncOrderIds]
//             );

//             for (const row of receiversResult.rows) {
//               await updateLinkedContainersStatus(client, row.id, mapping.shipment, 'system');
//             }
//           }
//         }

//         // Notification (non-critical)
//         try {
//           const updated = await client.query('SELECT * FROM consignments WHERE id = $1', [numericId]);
//           await sendNotification(updated.rows[0], `status_advanced_to_${nextStatus}`, { reason: 'Manual advance' });
//         } catch (notifErr) {
//           console.warn(`Notification failed:`, notifErr);
//         }

//       } catch (updateErr) {
//         updateError = updateErr;
//         if (updateErr.code === '22P02') {
//           console.warn(`Enum violation for '${nextStatus}'`);
//         } else {
//           throw updateErr;
//         }
//       }
//     });

//     if (updateError && updateError.code === '22P02') {
//       return res.status(409).json({ error: `Status '${nextStatus}' not in DB enum.` });
//     }

//     res.json({
//       message: `Status advanced to ${nextStatus}`,
//       data: { newStatus: nextStatus, previousStatus: currentStatus }
//     });

//   } catch (err) {
//     console.error("Error advancing status:", err);
//     res.status(500).json({ error: 'Failed to advance status' });
//   }
// }
// Enhanced calculateETA (returns { eta, daysUntil }; uses exact status match from eta_config table)
// async function calculateETA(client, status, baseDate = new Date()) {
//   // Dynamic: Use current date as base
//   try {
//     const configQuery = `SELECT days_offset FROM eta_config WHERE status = $1`; // Exact match
//     const configResult = await client.query(configQuery, [status]);
//     if (configResult.rowCount === 0) {
//       console.log(
//         `No ETA config for status: ${status}; using baseDate (0 days)`,
//       );
//       const eta = baseDate.toISOString().split("T")[0];
//       return { eta, daysUntil: 0 };
//     }
//     const days = configResult.rows[0].days_offset;
//     if (status.toLowerCase().includes("delivered")) {
//       // Simplified check
//       const eta = baseDate.toISOString().split("T")[0];
//       return { eta, daysUntil: 0 };
//     }
//     const etaDate = new Date(baseDate.getTime() + days * 86400000);
//     const eta = etaDate.toISOString().split("T")[0];
//     const daysUntil = computeDaysUntilEta(eta, baseDate);
//     console.log(
//       `[calculateETA] For status "${status}": offset=${days} days → ETA=${eta} (days until: ${daysUntil})`,
//     );
//     return { eta, daysUntil };
//   } catch (err) {
//     console.error("ETA calc error:", err);
//     const eta = new Date().toISOString().split("T")[0];
//     return { eta, daysUntil: 0 };
//   }
// }

export async function updateContainer(req, res) {
  let client;

  try {
    client = await pool.connect();
    await client.query("BEGIN");

    const { cid } = req.params;
    const updates = req.body;
    const created_by = updates.created_by || req.user?.id || "system";

    const current = await client.query(
      `
      SELECT owner_type
      FROM container_master
      WHERE cid = $1
      `,
      [cid],
    );

    if (current.rowCount === 0) {
      await client.query("ROLLBACK");

      return res.status(404).json({
        error: "Container not found",
      });
    }

    if (
      updates.owner_type &&
      updates.owner_type !== current.rows[0].owner_type
    ) {
      await client.query("ROLLBACK");

      return res.status(400).json({
        error: "Cannot change owner_type manually",
      });
    }

    await client.query(
      `
      UPDATE container_master
      SET
        container_number = COALESCE($1, container_number),
        container_size   = COALESCE($2, container_size),
        container_type   = COALESCE($3, container_type),
        remarks          = COALESCE($4, remarks),
        available_at     = COALESCE($5, available_at),
        updated_at       = NOW()
      WHERE cid = $6
      `,
      [
        updates.container_number,
        updates.container_size,
        updates.container_type,
        updates.remarks,
        updates.available_at,
        cid,
      ],
    );

    const purchaseExists = await client.query(
      `
      SELECT pid
      FROM container_purchase_details
      WHERE cid = $1
      `,
      [cid],
    );

    if (purchaseExists.rowCount > 0) {
      await client.query(
        `
        UPDATE container_purchase_details
        SET
          manufacture_date = COALESCE($1, manufacture_date),
          purchase_date    = COALESCE($2, purchase_date),
          purchase_price   = COALESCE($3, purchase_price),
          purchase_from    = COALESCE($4, purchase_from),
          owned_by         = COALESCE($5, owned_by),
          available_at     = COALESCE($6, available_at),
          currency         = COALESCE($7, currency),
          created_by       = $8
        WHERE cid = $9
        `,
        [
          updates.manufacture_date,
          updates.purchase_date,
          updates.purchase_price,
          updates.purchase_from,
          updates.owned_by,
          updates.available_at,
          updates.currency,
          created_by,
          cid,
        ],
      );
    } else {
      await client.query(
        `
        INSERT INTO container_purchase_details
        (
          cid,
          manufacture_date,
          purchase_date,
          purchase_price,
          purchase_from,
          owned_by,
          available_at,
          currency,
          created_by
        )
        VALUES
        ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        `,
        [
          cid,
          updates.manufacture_date,
          updates.purchase_date,
          updates.purchase_price,
          updates.purchase_from,
          updates.owned_by,
          updates.available_at,
          updates.currency,
          created_by,
        ],
      );
    }

    await client.query("COMMIT");

    return res.status(200).json({
      success: true,
      message: "Container updated successfully",
    });
  } catch (err) {
    if (client) {
      await client.query("ROLLBACK");
    }

    console.error("Container update failed:", err);

    return res.status(500).json({
      error: "Failed to update container",
      details: err.message,
    });
  } finally {
    if (client) {
      client.release();
    }
  }
}

// NEW: Reverse mapping for container status to receiver status (for cascades)
function mapContainerStatusToReceiverStatus(containerStatus) {
  const mapping = {
    Available: "Ready for Loading",
    Loaded: "Loaded Into Container",
    "In Transit": "Shipment In Transit",
    Arrived: "Arrived at Sort Facility",
    "De-Linked": "Ready for Delivery",
    Returned: "Shipment Delivered",
    "Under Repair": "Under Processing",
    Hired: "Loaded Into Container",
    Occupied: "Loaded Into Container",
    Cleared: "Shipment Delivered",
  };
  return mapping[containerStatus] || null; // Null if no direct map
}
// Helper function – returns a friendly, human-readable message for each shipment status
// Used in email notifications and possibly UI updates
function getStatusMessage(status) {
  // Normalize input (in case status comes in different cases or with extra spaces)
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
// Assuming getNotificationSettings is defined (from earlier)
export async function getNotificationSettings(typeCode) {
  console.log(
    `[getNotificationSettings] Starting for type_code: "${typeCode}"`,
  );

  try {
    const client = await pool.connect();
    console.log("[DB] Client connected");

    const result = await client.query(
      `
      SELECT 
        nt.type_code,
        nt.name,
        ns.enabled,
        ns.subject,
        ns.heading,
        ns.additional_content,
        ns.trigger_statuses
      FROM notification_types nt
      LEFT JOIN notification_settings ns ON ns.type_id = nt.id
      WHERE nt.type_code = $1
    `,
      [typeCode],
    );

    console.log(`[DB] Rows returned: ${result.rows.length}`);

    if (result.rows.length === 0) {
      console.log(
        `[getNotificationSettings] No matching type_code "${typeCode}" found in notification_types`,
      );
      client.release();
      return null;
    }

    const row = result.rows[0];
    console.log(`[DB] Found settings:`, {
      type_code: row.type_code,
      name: row.name,
      enabled: row.enabled,
      trigger_statuses_raw: row.trigger_statuses,
    });

    const settings = {
      ...row,
      trigger_statuses: row.trigger_statuses
        ? row.trigger_statuses.split(",").map((s) => s.trim().toLowerCase())
        : [],
    };

    client.release();
    return settings;
  } catch (err) {
    console.error(
      `[getNotificationSettings] DB error for "${typeCode}":`,
      err.message,
    );
    return null;
  }
}

export async function updateReceiverStatus(req, res) {
  let client;
  try {
    const orderId = req.params.orderId;
    const receiverId = req.params.id;
    const {
      status,
      notifyClient = true,
      notifyParties = false,
      forceRecalcEta = false,
    } = req.body || {};
    const created_by = req.user?.id || "system";
    console.log(
      "Received request to update receiver status:",
      { orderId, receiverId },
      { status, notifyClient, notifyParties, forceRecalcEta },
    );
    const validStatuses = [
      "Ready for Loading",
      "Loaded Into Container",
      "Shipment Processing",
      "Shipment In Transit",
      "Under Processing",
      "Arrived at Facility",
      "Ready for Delivery",
      "Shipment Delivered",
    ];
    // Enhanced validation with logging and case-insensitivity
    if (!orderId || isNaN(parseInt(orderId))) {
      return res.status(400).json({ error: "Valid order ID is required" });
    }
    if (!receiverId || isNaN(parseInt(receiverId))) {
      return res.status(400).json({ error: "Valid receiver ID is required" });
    }
    const trimmedStatus = (status || "").trim();
    if (!trimmedStatus || !isValidReceiverStatus(trimmedStatus)) {
      console.log("Invalid status provided:", trimmedStatus); // Debug log
      const validStatuses = [
        "Ready for Loading",
        "Loaded Into Container",
        "Shipment Processing",
        "Shipment In Transit",
        "Under Processing",
        "Arrived at Facility",
        "Ready for Delivery",
        "Shipment Delivered",
      ];
      return res.status(400).json({
        error: "Valid status is required",
        validStatuses,
        details: trimmedStatus
          ? `Received: "${trimmedStatus}" (case-insensitive match failed)`
          : "No status provided",
      });
    }
    // Normalize to exact casing
    const normalizedStatus = validStatuses.find(
      (valid) => valid.toLowerCase() === trimmedStatus.toLowerCase(),
    );

    client = await pool.connect();
    await client.query("BEGIN");

    // Fetch order, receiver, and ALL receivers (unchanged)
    const detailsQuery = `
      SELECT o.*, s.sender_email, s.sender_contact,
             r.id as receiver_id, r.receiver_name, r.receiver_email, r.receiver_contact, r.status as receiver_status, r.eta, r.total_weight, r.containers
      FROM orders o
      LEFT JOIN senders s ON o.id = s.order_id
      LEFT JOIN receivers r ON o.id = r.order_id AND r.id = $2
      WHERE o.id = $1
    `;
    const detailsResult = await client.query(detailsQuery, [
      parseInt(orderId),
      parseInt(receiverId),
    ]);
    if (detailsResult.rowCount === 0) {
      await client.query("ROLLBACK");
      return res.status(404).json({ error: "Order or Receiver not found" });
    }
    const order = detailsResult.rows[0];
    const oldStatus = order.receiver_status;

    const allReceiversQuery = `SELECT id, status, eta FROM receivers WHERE order_id = $1`;
    const allRecResult = await client.query(allReceiversQuery, [
      parseInt(orderId),
    ]);
    let allReceivers = allRecResult.rows;

    // Update receiver status (use normalized)
    const updateQuery = `
      UPDATE receivers
      SET status = $1, updated_at = CURRENT_TIMESTAMP
      WHERE id = $2 AND order_id = $3
      RETURNING id, status, eta, containers
    `;
    const updateResult = await client.query(updateQuery, [
      normalizedStatus,
      parseInt(receiverId),
      parseInt(orderId),
    ]);
    if (updateResult.rowCount === 0) {
      await client.query("ROLLBACK");
      return res
        .status(500)
        .json({ error: "Failed to update receiver status" });
    }
    let updatedReceiver = updateResult.rows[0];
    let daysUntilEta = computeDaysUntilEta(updatedReceiver.eta);
    let finalStatus = normalizedStatus;

    // Auto-upgrade if past ETA (unchanged)
    if (
      daysUntilEta !== null &&
      daysUntilEta <= 0 &&
      finalStatus !== "Shipment Delivered"
    ) {
      finalStatus = "Shipment Delivered";
      await client.query(`UPDATE receivers SET status = $1 WHERE id = $2`, [
        finalStatus,
        parseInt(receiverId),
      ]);
      console.log(
        `Auto-upgraded receiver ${receiverId} to ${finalStatus} (past ETA: ${daysUntilEta} days)`,
      );
      const refetchResult = await client.query(
        `SELECT id, status, eta, containers FROM receivers WHERE id = $1`,
        [parseInt(receiverId)],
      );
      updatedReceiver = refetchResult.rows[0];
      daysUntilEta = computeDaysUntilEta(updatedReceiver.eta);
    }

    // Dynamically fetch offsets (unchanged)
    const oldOffsetQuery = `SELECT days_offset FROM eta_config WHERE status = $1 LIMIT 1`;
    const oldOffsetResult = await client.query(oldOffsetQuery, [
      oldStatus || "In Process",
    ]);
    const oldOffset =
      oldOffsetResult.rowCount > 0
        ? oldOffsetResult.rows[0].days_offset
        : Infinity;

    const newOffsetQuery = `SELECT days_offset FROM eta_config WHERE status = $1 LIMIT 1`;
    const newOffsetResult = await client.query(newOffsetQuery, [finalStatus]);
    const newOffset =
      newOffsetResult.rowCount > 0 ? newOffsetResult.rows[0].days_offset : 0;

    const statusAdvanced = newOffset < oldOffset;

    // Recalculate ETA (unchanged)
    let newEta = updatedReceiver.eta;
    if (!updatedReceiver.eta || forceRecalcEta || statusAdvanced) {
      const etaResult = await calculateETA(client, finalStatus);
      newEta = etaResult.eta;
      if (newEta !== updatedReceiver.eta) {
        await client.query(`UPDATE receivers SET eta = $1 WHERE id = $2`, [
          newEta,
          parseInt(receiverId),
        ]);
        console.log(
          `Recalculated ETA for receiver ${receiverId} (status: ${finalStatus}): ${newEta} (days until: ${etaResult.daysUntil})`,
        );
        const refetchResult = await client.query(
          `SELECT id, status, eta, containers FROM receivers WHERE id = $1`,
          [parseInt(receiverId)],
        );
        updatedReceiver = refetchResult.rows[0];
        daysUntilEta = etaResult.daysUntil;
      }
    }

    // Update allReceivers
    allReceivers = allReceivers.map((r) =>
      r.id === parseInt(receiverId)
        ? { ...r, status: finalStatus, eta: newEta }
        : r,
    );

    // Cascade to linked containers (bidirectional)
    await updateLinkedContainersStatus(
      client,
      parseInt(receiverId),
      finalStatus,
      created_by,
    );

    // Cascade: Update order_items
    await client.query(
      `
      UPDATE order_items
      SET consignment_status = $1, updated_at = CURRENT_TIMESTAMP
      WHERE receiver_id = $2
    `,
      [finalStatus, parseInt(receiverId)],
    );

    // Cascade: Update overall order status
    await updateOrderOverallStatus(
      client,
      parseInt(orderId),
      finalStatus,
      allReceivers,
    );

    // Recalc and update order-level ETA
    const minEtaQuery = `SELECT MIN(eta) as min_eta FROM receivers WHERE order_id = $1`;
    const minEtaResult = await client.query(minEtaQuery, [parseInt(orderId)]);
    const orderNewEta = minEtaResult.rows[0].min_eta;
    if (orderNewEta && orderNewEta !== order.eta) {
      await client.query(
        `UPDATE orders SET eta = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2`,
        [orderNewEta, parseInt(orderId)],
      );
      console.log(
        `Updated order ${orderId} ETA to earliest receiver: ${orderNewEta}`,
      );
    }

    await client.query(
      `
      INSERT INTO order_tracking (order_id, receiver_id, status, old_status, created_by, created_time)
      VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
    `,
      [
        parseInt(orderId),
        parseInt(receiverId),
        finalStatus,
        oldStatus,
        created_by,
      ],
    );

    await client.query("COMMIT");

    res.status(200).json({
      success: true,
      message: `Receiver status updated to "${finalStatus}". ETA recalculated to "${newEta}". Cascades (incl. containers) and notifications triggered.`,
      updatedReceiver: {
        id: updatedReceiver.id,
        status: finalStatus,
        eta: newEta,
        days_until_eta: daysUntilEta,
        containers: updatedReceiver.containers,
      },
    });
  } catch (error) {
    console.error("Error updating receiver status:", error);
    if (client) {
      try {
        await client.query("ROLLBACK");
      } catch (rollbackErr) {
        console.error("Rollback failed:", rollbackErr);
      }
    }
    return res
      .status(500)
      .json({ error: "Internal server error", details: error.message });
  } finally {
    if (client) client.release();
  }
}

function mapReceiverStatusToContainerStatus(receiverStatus) {
  const map = {
    // Pre-shipment / ready stages
    "Order Created": "Available",
    "Ready for Loading": "Available",
    Created: "Available",

    // Loading / loaded
    "Loaded Into Container": "Loaded",
    Loaded: "Loaded",

    // In shipment / transit
    "Shipment Processing": "Occupied",
    "Under Shipment Processing": "Occupied",
    "Shipment In Transit": "In Transit",
    "Under Processing": "Occupied",
    "Submitted On Vessel": "In Transit",
    "In Transit": "In Transit",

    // Arrival / destination
    "Arrived at Facility": "Arrived",
    // 'Arrived at Facility'   : 'Arrived',
    "Arrived at Destination": "Arrived",
    "Ready for Delivery": "Arrived",
    "Customs Cleared": "Cleared",

    // Completed / returned
    Delivered: "Returned",
    "Shipment Delivered": "Returned",

    // Other / fallback
    default: "Available",
  };

  const result = map[receiverStatus] || map["default"];

  if (!map[receiverStatus]) {
    console.warn(
      `[Mapping] Unhandled receiver status "${receiverStatus}" → fallback to "${result}"`,
    );
  }

  return result;
}

async function updateLinkedContainersStatus(
  client,
  receiverId,
  newReceiverStatus,
  created_by = "system",
) {
  try {
    const receiverQuery = `SELECT containers FROM receivers WHERE id = $1`;
    const recResult = await client.query(receiverQuery, [receiverId]);

    if (recResult.rowCount === 0 || !recResult.rows[0].containers) {
      return;
    }

    let containers = [];
    const rawContainers = recResult.rows[0].containers;

    if (typeof rawContainers === "string") {
      let cleaned = rawContainers.trim();

      try {
        const parsed = JSON.parse(cleaned);
        if (Array.isArray(parsed)) {
          containers = parsed;
        }
      } catch {
        cleaned = cleaned
          .replace(/^["'\[]+|["'\]]+$/g, "")
          .replace(/["']/g, "")
          .replace(/\s+/g, " ");

        if (cleaned.includes(",")) {
          containers = cleaned
            .split(",")
            .map((s) => s.trim())
            .filter(Boolean);
        } else if (cleaned) {
          containers = [cleaned];
        }
      }
    } else if (Array.isArray(rawContainers)) {
      containers = rawContainers;
    }

    if (containers.length === 0) {
      return;
    }

    const cidQuery = `
      SELECT cid
      FROM container_master
      WHERE container_number = ANY($1)
    `;

    const cidResult = await client.query(cidQuery, [containers]);

    const cids = cidResult.rows
      .map((row) => row.cid)
      .filter((cid) => !isNaN(cid));

    if (cids.length === 0) {
      return;
    }

    console.log(
      `[Container Sync Disabled] Receiver ${receiverId} | Status "${newReceiverStatus}" | CIDs: ${cids.join(", ")}`,
    );

    return;
  } catch (err) {
    console.error(
      `Error processing linked containers for receiver ${receiverId}:`,
      err.message,
    );
  }
}

async function updateOrderOverallStatus(
  client,
  orderId,
  newReceiverStatus,
  receivers,
) {
  // Pass receivers for efficiency
  try {
    // Dynamically fetch status order from eta_config (sorted by days_offset DESC: early stages first, late last)
    const configQuery = `
      SELECT DISTINCT status, days_offset 
      FROM eta_config 
      WHERE status NOT IN ('Cancelled')  -- Exclude special cases
      ORDER BY days_offset DESC
    `;
    const configResult = await client.query(configQuery);
    const statusesFromDb = configResult.rows;

    // Build dynamic statusOrder: higher index = later stage (lower offset)
    const statusOrder = {};
    statusesFromDb.forEach((row, index) => {
      statusOrder[row.status] = index;
    });

    // Fallback for missing statuses
    const fallbackStatuses = [
      "Created",
      "Order Created",
      "In Process",
      "Submitted",
      "In Transit",
    ];
    fallbackStatuses.forEach((status, index) => {
      if (!(status in statusOrder)) {
        statusOrder[status] = fallbackStatuses.length + index; // Ensure fallbacks come after DB statuses
      }
    });

    const receiverStatuses = receivers.map((r) => r.status);
    const maxIndex = Math.max(
      ...receiverStatuses.map((s) => statusOrder[s] || 0),
    );
    let overallStatus =
      Object.keys(statusOrder).find((key) => statusOrder[key] === maxIndex) ||
      "In Process";

    // Enhanced: Eta-based auto-upgrade (all past eta → 'Shipment Delivered' if not cancelled)
    const today = new Date(); // Dynamic: Use current date
    const allPastEta = receivers.every((r) => {
      if (["Shipment Delivered", "Cancelled"].includes(r.status)) return false;
      const days = computeDaysUntilEta(r.eta, today);
      return days !== null && days <= 0;
    });
    if (allPastEta && !receiverStatuses.includes("Cancelled")) {
      overallStatus = "Shipment Delivered";
    }

    // Weighted: e.g., >50% delivered → 'Shipment In Transit' (extend as needed)
    const deliveredPct =
      (receiverStatuses.filter((s) => s === "Shipment Delivered").length /
        receivers.length) *
      100;
    if (deliveredPct > 50 && overallStatus !== "Shipment Delivered") {
      overallStatus = "Shipment In Transit";
    }

    const orderUpdateQuery = `
      UPDATE orders SET status = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2 RETURNING id, status
    `;
    const orderResult = await client.query(orderUpdateQuery, [
      overallStatus,
      orderId,
    ]);
    if (orderResult.rowCount > 0) {
      console.log(
        `Cascaded order status to: ${overallStatus} for order ${orderId} (delivered %: ${deliveredPct.toFixed(0)})`,
      );
    }
  } catch (err) {
    console.error("Error updating overall order status:", err);
    throw err; // Re-throw to handle in caller if needed
  }
}
// Stub functions (define these if missing)
async function calculateETAAll(client, status) {
  // Placeholder: Return { eta: '2025-12-23', daysUntil: 8 }
  return {
    eta: new Date(Date.now() + 8 * 24 * 60 * 60 * 1000)
      .toISOString()
      .split("T")[0],
    daysUntil: 8,
  };
}
function computeDaysUntilEtaAll(etaStr) {
  const eta = new Date(etaStr);
  const now = new Date();
  return Math.max(0, Math.ceil((eta - now) / (24 * 60 * 60 * 1000)));
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
        !["Available", "Assigned to Job"].includes(container.derived_status)
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
          AND total_number > COALESCE(assigned_boxes, 0)
        ORDER BY id
        LIMIT 1 OFFSET $2
        FOR UPDATE
        `,
        [receiverIdNum, detailIdxNum],
      );

      if (itemsRes.rowCount === 0) {
        skipped.push({
          ...ass,
          reason: "no remaining items found for this detail index",
        });
        continue;
      }

      const targetItem = itemsRes.rows[0];
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
          qty_delivered = COALESCE(qty_delivered, 0) + $1,
          containers = $2::jsonb,
          updated_at = NOW()
        WHERE id = $3
        `,
        [assignBoxes, JSON.stringify(updatedContainers), receiverIdNum],
      );

      const nextStatus = await moveReceiverToNextStatus(client, receiverIdNum);

      if (nextStatus) {
        await createOrderTracking(client, {
          orderId: orderIdNum,
          receiverId: receiverIdNum,
          containerId: cid,
          status: nextStatus.order_status,
          createdBy: currentUserEmail,
          itemRef,
        });
      }

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

export async function assignOneContainerToMultipleReceivers(req, res) {
  let client;
  try {
    client = await pool.connect();
    await client.query("BEGIN");

    const {
      orderId, // required
      containerId, // required (cid or container_number)
      receiverIds = [], // optional: specific receivers
      totalQtyToAssign = null, // optional: max total boxes to assign across all
    } = req.body;

    const created_by = req.user?.id || "system";

    if (!orderId || isNaN(Number(orderId))) {
      throw new Error("Valid orderId is required");
    }

    if (!containerId) {
      throw new Error("containerId (cid or container_number) is required");
    }

    // 1. Validate container
    const contRes = await client.query(
      `
      SELECT cm.cid, cm.container_number,
             cs.availability,
             CASE 
               WHEN cs.availability = 'Cleared' THEN 'Cleared'
               WHEN cs.availability IN ('Available', 'Assigned to Job') THEN 'Available'
               ELSE cs.availability
             END as derived_status,
             cs.location
      FROM container_master cm
      LEFT JOIN (
        SELECT DISTINCT ON (cid) cid, availability, location
        FROM container_status ORDER BY cid, sid DESC NULLS LAST
      ) cs ON cm.cid = cs.cid
      WHERE (cm.cid = $1 OR cm.container_number = $1::text)
    `,
      [containerId],
    );

    if (contRes.rowCount === 0) throw new Error("Container not found");

    const container = contRes.rows[0];
    const cid = container.cid;
    const contNumber = container.container_number;

    if (!["Available", "Assigned to Job"].includes(container.derived_status)) {
      throw new Error(
        `Container is not available (status: ${container.derived_status})`,
      );
    }

    // 2. Get receivers with remaining qty
    let receiversQuery = `
      SELECT id, receiver_name, containers, qty_delivered, total_number, total_weight
      FROM receivers
      WHERE order_id = $1
        AND total_number > COALESCE(qty_delivered, 0)
    `;
    let receiversParams = [orderId];

    if (receiverIds.length > 0) {
      receiversQuery += ` AND id = ANY($2)`;
      receiversParams.push(receiverIds);
    }

    const receiversRes = await client.query(receiversQuery, receiversParams);
    if (receiversRes.rowCount === 0) {
      throw new Error(
        "No receivers with remaining quantity found in this order",
      );
    }

    const receivers = receiversRes.rows;
    const trackingData = [];

    let totalAssigned = 0;
    let totalWeightAssigned = 0;
    let remainingCap =
      totalQtyToAssign !== null ? Number(totalQtyToAssign) : Infinity;

    for (const receiver of receivers) {
      if (remainingCap <= 0) break;

      const recId = receiver.id;
      let receiverContainers = safeParseJsonArrayForMultiple(
        receiver.containers,
      );

      const itemsRes = await client.query(
        `
        SELECT id, total_number, weight, assigned_boxes, assigned_weight_kg, container_details
        FROM order_items
        WHERE receiver_id = $1
          AND total_number > COALESCE(assigned_boxes, 0)
        ORDER BY id
      `,
        [recId],
      );

      if (itemsRes.rowCount === 0) continue;

      let recAssignedQty = 0;
      let recAssignedWeight = 0;
      const newContainers = new Set(receiverContainers);

      for (const item of itemsRes.rows) {
        if (remainingCap <= 0) break;

        const itemId = item.id;
        const remainingBoxes = item.total_number - (item.assigned_boxes || 0);
        if (remainingBoxes <= 0) continue;

        const remainingWeight =
          item.weight * (remainingBoxes / item.total_number);

        // Respect cap
        const assignBoxes = Math.min(remainingBoxes, Math.floor(remainingCap));
        const assignWeight = Number(
          (item.weight * (assignBoxes / item.total_number)).toFixed(2),
        );

        if (assignBoxes <= 0) continue;

        // Update container_details
        let containerDetails = safeParseJsonArrayForMultiple(
          item.container_details,
        );
        let entry = containerDetails.find((e) => e?.container?.cid === cid);

        if (!entry) {
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
          Number(entry.assign_total_box || 0) + assignBoxes,
        );
        entry.assign_weight = (
          Number(entry.assign_weight || 0) + assignWeight
        ).toFixed(2);
        entry.remaining_items = String(
          item.total_number - (item.assigned_boxes || 0) - assignBoxes,
        );

        // History log
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
            contNumber,
            orderId,
            recId,
            itemId,
            assignBoxes,
            assignWeight,
            "Ready for Loading",
            "Available",
            "ASSIGN",
            created_by,
            `Assigned ${assignBoxes} boxes (${assignWeight.toFixed(2)} kg) to shared container ${contNumber}`,
          ],
        );

        // Update order_item
        const newBoxes = (item.assigned_boxes || 0) + assignBoxes;
        const newKg = (item.assigned_weight_kg || 0) + assignWeight;

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
          [newBoxes, newKg, JSON.stringify(containerDetails), itemId],
        );

        recAssignedQty += assignBoxes;
        recAssignedWeight += assignWeight;
        remainingCap -= assignBoxes;
      }

      if (recAssignedQty > 0) {
        newContainers.add(contNumber);
        const updatedContainers = Array.from(newContainers);

        const newDelivered = (receiver.qty_delivered || 0) + recAssignedQty;

        await client.query(
          `
          UPDATE receivers
          SET
            qty_delivered = $1,
            containers    = $2::jsonb,
            status        = CASE 
              WHEN $1 >= total_number THEN 'Ready for Loading' 
              ELSE status 
            END,
            updated_at    = NOW()
          WHERE id = $3
        `,
          [newDelivered, JSON.stringify(updatedContainers), recId],
        );

        trackingData.push({
          receiverId: recId,
          receiverName: receiver.receiver_name,
          assignedQty: recAssignedQty,
          assignedWeightKg: recAssignedWeight.toFixed(2),
          containers: [contNumber],
        });

        totalAssigned += recAssignedQty;
        totalWeightAssigned += recAssignedWeight;
      }
    }

    if (totalAssigned === 0) {
      throw new Error("No remaining quantity could be assigned");
    }

    // Update order
    await client.query(
      `
      UPDATE orders
      SET total_assigned_qty = total_assigned_qty + $1,
          updated_at = NOW()
      WHERE id = $2
    `,
      [totalAssigned, orderId],
    );

    await client.query("COMMIT");

    res.json({
      success: true,
      message: `Container ${contNumber} assigned to ${trackingData.length} receivers in order ${orderId}`,
      totalAssignedQty: totalAssigned,
      totalAssignedWeightKg: totalWeightAssigned.toFixed(2),
      tracking: trackingData,
      capReached: remainingCap <= 0 && totalQtyToAssign !== null,
    });
  } catch (err) {
    if (client) await client.query("ROLLBACK");
    console.error("Assign one container error:", err.stack || err);
    res.status(400).json({
      error: "Failed to assign container",
      details: err.message,
    });
  } finally {
    if (client) client.release();
  }
}

function safeParseContainers(val) {
  if (!val) return [];

  // Already array
  if (Array.isArray(val)) return val;

  // Try JSON parse
  if (typeof val === "string") {
    try {
      const parsed = JSON.parse(val);
      if (Array.isArray(parsed)) return parsed;
    } catch {
      // Fallback: treat as comma-separated or single container
      return val
        .split(",")
        .map((v) => v.trim())
        .filter(Boolean);
    }
  }

  return [];
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
function bulkSanitizeContainerDetails(details) {
  if (!Array.isArray(details)) return [];
  return details.map((d) => {
    if (!d || typeof d !== "object") return d;
    [
      "total_number",
      "assign_weight",
      "remaining_items",
      "assign_total_box",
    ].forEach((f) => {
      const val = d[f];
      d[f] = (val != null ? parseFloat(val) : 0).toString();
    });
    return d;
  });
}

export async function assignContainersToOrders(req, res) {
  let client;
  try {
    client = await pool.connect();
    await client.query("BEGIN");

    const { assignments } = req.body;
    const currentUserEmail = req.user?.email || "system-fallback";

    // Get real user email for auditing
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

        // Always fetch & lock receiver first
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

            newContainers.add(entry.container.container_number);

            trackingEntries.push({
              cid,
              itemRef: item.item_ref,
            });
          }

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

          for (const tracking of trackingEntries) {
            await createOrderTracking(client, {
              orderId,
              receiverId: recId,
              containerId: tracking.cid,
              status: trackingStatus,
              createdBy: currentUserEmail,
              itemRef: tracking.itemRef,
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
    // const created_by = req.user?.id || req.user?.email || "system";
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

        // Always fetch and lock receiver first
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

async function sendUpdateToSubscribers(orderId, newStatus, oldStatus) {
  try {
    // Fetch order details (adapted for your schema; assumes 'reference_id' in receivers or orders)
    const orderQuery = `
      SELECT o.booking_ref, o.eta, o.updated_at as last_updated, o.sender_name, o.receiver_name, o.receiver_item_ref
      FROM orders o
      LEFT JOIN receivers r ON o.id = r.order_id
      WHERE o.id = $1
    `;
    const orderResult = await pool.query(orderQuery, [orderId]);
    if (orderResult.rowCount === 0) {
      return;
    }

    const order = orderResult.rows[0];
    const tz = "Asia/Dubai"; // RGSL timezone
    const now = new Date().toLocaleString("en-US", { timeZone: tz });
    const etaFormatted = order.eta
      ? new Date(order.eta).toLocaleDateString("en-GB")
      : "—"; // dd/MM/yyyy
    const route = `${order.sender_name || ""} to ${order.receiver_name || ""}`; // Full route from sender/receiver

    // Fetch subscribers from notifications table (assuming it has order_id, reference_id, email)
    const subQuery = `SELECT email FROM notifications WHERE order_id = $1 AND reference_id = $2`;
    const subResult = await pool.query(subQuery, [orderId, order.reference_id]);
    const subscribers = subResult.rows
      .map((row) => row.email)
      .filter((email) => email && email.includes("@"));
    console.log(`Found ${orderId} subscribers for order ${orderId}:`, order);
    // if (subscribers.length === 0) {
    //   console.log(`No subscribers for order ${orderId}`);
    //   return;
    // }

    // Get phase details (assume getPhase function exists; fallback if not)
    // const phase = getPhase ? getPhase(newStatus) : { label: newStatus, msg: `Updated from "${oldStatus}" to "${newStatus}".` };

    // Template data for shipment update
    const shipmentData = {
      statusLabel: "" || `Status: ${newStatus}`,
      statusMsg:
        "phase.msg" || `Updated from "${oldStatus}" to "${newStatus}".`,
      refId: order.reference_id || order.booking_ref || "—",
      orderId: order.booking_ref || "—",
      route: route,
      etaFormatted,
      lastUpdated: now,
      trackLink: `https://ordertracking.royalgulfshipping.com/?ref=${encodeURIComponent(order.reference_id || order.booking_ref)}`,
    };
    const email = "support2@royalgulfshipping.com"; // For testing, send to fixed email
    // Send to each subscriber (uses updated sendShipmentEmail)
    // for (const email of subscribers) {
    // await sendShipmentEmail(email, shipmentData);
    console.log(
      `Update email sent to ${email} for order ${orderId}: ${newStatus}`,
    );
    // }
  } catch (err) {
    console.error("Subscriber email error:", err);
  }
}

// Updated cascadeToContainers function - fetches from separate 'receivers' table (no orders.receivers column needed)
// Collects containers from all relevant receivers for the order
// Add/update this in your order.controller.js
async function cascadeToContainers(client, orderId, status, receiverId) {
  const CONTAINER_TABLE = "container_master"; // Your table (singular)

  try {
    // Step 1: Fetch containers from receivers table for the order (specific receiver or all)
    const fetchQuery = `
      SELECT id, containers
      FROM receivers
      WHERE order_id = $1 
        AND (id = $2 OR $2 IS NULL)
    `;
    const fetchParams = [orderId, receiverId];

    const fetchResult = await client.query(fetchQuery, fetchParams);

    if (fetchResult.rowCount === 0) {
      console.log(
        `No receivers found for order ${orderId}, receiver ${receiverId || "all"}`,
      );
      return;
    }

    const allContainerNumbers = new Set(); // Dedupe across receivers

    fetchResult.rows.forEach((row) => {
      // Parse receiver's containers array (assumes JSON array like ["SLUO1234521"])
      if (row.containers) {
        let containersArray;
        if (typeof row.containers === "string") {
          // Fallback: If stored as CSV string, split it
          containersArray = row.containers.split(",");
        } else if (Array.isArray(row.containers)) {
          containersArray = row.containers;
        } else {
          console.warn(
            `Unexpected containers format for receiver ${row.id}:`,
            typeof row.containers,
          );
          return;
        }
        containersArray.forEach((cn) => {
          const trimmed = (cn || "").toString().trim();
          if (trimmed) allContainerNumbers.add(trimmed);
        });
      }
    });

    const containerNumbers = Array.from(allContainerNumbers);
    if (containerNumbers.length === 0) {
      console.log(
        `No valid container numbers found for order ${orderId}, receiver ${receiverId || "all"}`,
      );
      return;
    }

    console.log(
      `Found containers to cascade from receivers table: [${containerNumbers.join(", ")}]`,
    );

    // Step 2: Derive status (direct map; customize if needed)
    let derivedStatus = status;

    // Step 3: Dynamic UPDATE on container_master
    const placeholders = containerNumbers.map((_, i) => `$${i + 2}`).join(", "); // Starts after $1
    const updateQuery = `
      UPDATE ${CONTAINER_TABLE}
      SET 
        derived_status = $1,
        updated_at = CURRENT_TIMESTAMP
      WHERE container_number IN (${placeholders})
    `;
    const updateParams = [derivedStatus, ...containerNumbers];

    const updateResult = await client.query(updateQuery, updateParams);

    console.log(
      `✅ Cascaded "${derivedStatus}" to ${updateResult.rowCount} containers in ${CONTAINER_TABLE} for order ${orderId}, receiver ${receiverId || "all"}.`,
    );
  } catch (err) {
    console.error(`❌ Cascade to ${CONTAINER_TABLE} failed:`, err.message);
    if (err.code === "42703") {
      // Column does not exist
      console.error(
        `Column error - Verify: 'containers' column exists in 'receivers' table.`,
      );
      console.error(
        `Sample query for debug: SELECT id, containers FROM receivers WHERE order_id = ${orderId};`,
      );
    } else if (err.code === "42P01") {
      console.error(`Table ${CONTAINER_TABLE} or 'receivers' missing.`);
    }
    throw err; // Isolate in main tx
  }
}
// Optional: Define route mapping here (or move to a shared config file)
const ROUTE_CITY_MAP = {
  1: "Shenzhen",
  2: "Karachi",
  3: "London",
  5: "Dubai",
  // Add more codes as your system grows
  // Default fallback: keep the code if not mapped
};
/**
 * Sends a shipment / order status update email to the receiver
 * @param {string} email - Recipient email address
 * @param {Object} shipmentData - Data from the notification logic
 * @param {string} [shipmentData.receiverName]
 * @param {string} [shipmentData.statusLabel]
 * @param {string} [shipmentData.statusMsg]
 * @param {string} [shipmentData.refId]
 * @param {string|number} [shipmentData.orderId]
 * @param {string} [shipmentData.route]
 * @param {string} [shipmentData.etaFormatted]
 * @param {string|Date} [shipmentData.lastUpdated]
 * @param {string} [shipmentData.trackLink]
 * @param {string} [shipmentData.currentStatus] - Used for status filtering
 * @param {string} [notificationType='order-status-update'] - Type code for settings lookup
 * @returns {Promise<{success: boolean, messageId?: string, error?: string, skipped?: boolean}>}
 */
export async function sendShipmentEmail(
  email,
  shipmentData,
  notificationType = "order-status-update",
) {
  if (!email || typeof email !== "string" || !email.trim().includes("@")) {
    console.warn(`Invalid or missing email: ${email}`);
    return { success: false, error: "Invalid email address" };
  }

  console.log(`Preparing ${notificationType} email to: ${email}`);

  // Clean receiver name
  const receiverName =
    String(shipmentData.receiverName || "Valued Customer")
      .trim()
      .replace(/\|.*$/, "")
      .replace(/\s+/g, " ") || "Valued Customer";

  // Safe defaults for all fields
  const safeData = {
    statusLabel: String(shipmentData.statusLabel || "Shipment Updated"),
    statusMsg: String(
      shipmentData.statusMsg || "We have an update on your shipment.",
    ),
    refId: String(shipmentData.refId || "—"),
    orderId: String(shipmentData.orderId || "—"),
    route: String(shipmentData.route || "—"),
    etaFormatted: String(shipmentData.etaFormatted || "To be confirmed"),
    trackLink: String(
      shipmentData.trackLink || "https://consolidatetracking.onrender.com/",
    ),
    receiverName,
  };

  // Handle lastUpdated
  let lastUpdatedStr = new Date().toLocaleString("en-GB");
  if (shipmentData.lastUpdated) {
    try {
      const date = new Date(shipmentData.lastUpdated);
      lastUpdatedStr = !isNaN(date.getTime())
        ? date.toLocaleString("en-GB", {
            dateStyle: "medium",
            timeStyle: "short",
          })
        : String(shipmentData.lastUpdated);
    } catch {
      lastUpdatedStr = String(shipmentData.lastUpdated);
    }
  }
  safeData.lastUpdated = lastUpdatedStr;

  // Build template data
  const templateData = {
    type: "shipment",
    ...safeData,
    updatedItems: Array.isArray(shipmentData.updatedItems)
      ? shipmentData.updatedItems
      : [],
    currentStatus: shipmentData.currentStatus || shipmentData.statusLabel || "",
  };

  try {
    // 1. Fetch settings
    const settings = await getNotificationSettings(notificationType);

    if (!settings || settings.enabled === false) {
      console.log(
        `Skipped ${notificationType} email to ${email}: disabled or not configured`,
      );
      return {
        success: false,
        skipped: true,
        message: `Notification ${notificationType} is disabled`,
      };
    }

    // ── IMPORTANT: Declare finalSubject EARLY ──────────────────────────────
    let finalSubject = `Royal Gulf Shipping – ${templateData.statusLabel} (Ref: ${templateData.refId})`;

    if (settings.subject) {
      finalSubject = settings.subject
        .replace(/{type_name}/gi, settings.name || notificationType)
        .replace(/{ref_id}/gi, templateData.refId || "—")
        .replace(/{order_number}/gi, templateData.orderId || "—")
        .replace(/{status}/gi, templateData.statusLabel || "")
        .replace(
          /{customer_name}/gi,
          templateData.receiverName || "Valued Customer",
        );
    }

    // 2. Normalize incoming status
    let incomingStatus = String(
      templateData.currentStatus || templateData.statusLabel || "",
    )
      .toLowerCase()
      .trim();

    const normalizedIncoming = incomingStatus
      .replace(/\s+/g, "-") // spaces → hyphen
      .replace(/-+/g, "-") // collapse multiple hyphens
      .replace(/^shipment-?/i, ""); // remove "shipment" prefix if present

    console.log(`Normalized incoming status: "${normalizedIncoming}"`);

    // 3. Normalize allowed statuses from DB the same way
    const allowedNormalized = (settings.trigger_statuses || []).map((s) =>
      String(s)
        .trim()
        .toLowerCase()
        .replace(/\s+/g, "-")
        .replace(/-+/g, "-")
        .replace(/^shipment-?/i, ""),
    );

    console.log("Allowed statuses (normalized):", allowedNormalized);

    // 4. Check if allowed
    const isAllowed =
      allowedNormalized.length === 0 ||
      allowedNormalized.includes(normalizedIncoming) ||
      allowedNormalized.includes(incomingStatus); // fallback for original format

    if (!isAllowed) {
      console.log(
        `Skipped: status "${incomingStatus}" (normalized "${normalizedIncoming}") ` +
          `does not match any allowed value`,
        { allowed: allowedNormalized },
      );
      return {
        success: false,
        skipped: true,
        message: `Status "${incomingStatus}" not configured for notifications`,
      };
    }

    console.log(
      `Status "${incomingStatus}" → normalized "${normalizedIncoming}" is allowed → proceeding to send`,
    );

    // ── Safe to use finalSubject here ─────────────────────────────────────
    console.log(
      `Sending ${notificationType} email to ${receiverName} (${email})`,
      {
        subject: finalSubject,
        status: templateData.statusLabel,
        currentStatusNormalized: normalizedIncoming,
        allowedStatuses: settings.trigger_statuses || "all",
      },
    );

    console.log("Email template data:", templateData);

    // Optional: pass custom subject if sendOrderEmail wants to use it
    templateData.customSubject = finalSubject;

    // 5. Actually send the email
    const result = await sendOrderEmail(email, notificationType, templateData);

    if (result.success) {
      console.log(
        `Shipment email sent successfully to ${receiverName} (${email}) → Msg ID: ${result.messageId || "—"}`,
      );
    } else {
      console.error(
        `Failed sending shipment email to ${receiverName} (${email}): ${result.error || "Unknown error"}`,
      );
    }

    return result;
  } catch (err) {
    console.error(
      `Shipment email fatal error for ${email} (${receiverName}):`,
      {
        message: err.message,
        stack: err.stack?.substring(0, 500),
        notificationType,
        email,
      },
    );
    return {
      success: false,
      error: err.message || "Unexpected error during email preparation",
    };
  }
}

export async function advanceStatus(req, res) {
  let syncOrderIds = [];
  let consignmentEta = null;

  try {
    const numericId = Number(req.params.id);

    if (!Number.isInteger(numericId) || numericId <= 0) {
      return res.status(400).json({
        error: "Invalid consignment ID.",
      });
    }

    const { rows } = await pool.query(
      `SELECT id, status FROM consignments WHERE id = $1`,
      [numericId],
    );

    if (!rows.length) {
      return res.status(404).json({
        error: "Consignment not found",
      });
    }

    const currentStatus = rows[0].status;

    const statusResult = await pool.query(
      `
      SELECT
        id,
        consignment_status,
        order_status,
        container_status,
        days_offset,
        sorting_number
      FROM statuses
      WHERE status = true
      AND consignment_status IS NOT NULL
      ORDER BY sorting_number ASC
      `,
    );

    const statuses = statusResult.rows;

    const currentIndex = statuses.findIndex(
      (s) => s.consignment_status === currentStatus,
    );

    const nextStatusRow = statuses[currentIndex + 1];

    const nextStatus = nextStatusRow.consignment_status;
    const syncedStatus = nextStatusRow.order_status;
    const containerStatus = nextStatusRow.container_status;

    if (nextStatus === "Delivered") {
      consignmentEta = new Date().toISOString().split("T")[0];
    } else {
      const etaResult = await calculateETA(pool, syncedStatus);

      consignmentEta = etaResult?.eta ?? null;
    }

    await withTransaction(async (client) => {
      await client.query(
        `
        UPDATE consignments
        SET status=$1,
            eta=$2,
            updated_at=NOW()
        WHERE id=$3
        `,
        [nextStatus, consignmentEta, numericId],
      );

      if (containerStatus) {
        await client.query(
          `
            UPDATE container_master
            SET status = $1
            WHERE cid IN (
              SELECT DISTINCT cid
              FROM container_assignment_history
              WHERE consignment_id = $2
            )
          `,
          [containerStatus, numericId],
        );

        await client.query(
          `
            UPDATE container_assignment_history
            SET status = $1
            WHERE consignment_id = $2
          `,
          [containerStatus, numericId],
        );
      }
      try {
        await client.query(
          `
          INSERT INTO order_tracking
            (
              order_id,
              sender_id,
              sender_ref,
              receiver_id,
              container_id,
              consignment_number,
              status,
              old_status,
              item_ref,
              created_by
            )
          SELECT
            cah.order_id,
            ot.sender_id,
            ot.sender_ref,
            cah.receiver_id,
            cah.cid,
            c.consignment_number,
            $1,
            $2,
            oi.item_ref,
            $3
            FROM container_assignment_history cah
            JOIN consignments c
            ON c.id = cah.consignment_id
            JOIN order_items oi
            ON oi.id = cah.detail_id
            LEFT JOIN LATERAL (
              SELECT sender_id, sender_ref
                FROM order_tracking
                WHERE item_ref = oi.item_ref
                ORDER BY created_time DESC
            LIMIT 1
            ) ot ON true
            WHERE cah.consignment_id = $4
          `,
          [
            syncedStatus,
            currentStatus,
            req.user?.username || req.user?.email || "system",
            numericId,
          ],
        );
      } catch (e) {
        throw e;
      }

      const orderIdsRes = await client.query(
        `
        SELECT jsonb_array_elements(orders::jsonb)->>'id'
        AS order_id

        FROM consignments

        WHERE id=$1
        `,
        [numericId],
      );

      syncOrderIds = orderIdsRes.rows
        .map((r) => parseInt(r.order_id, 10))
        .filter(Boolean);

      if (syncOrderIds.length) {
        await client.query(
          `
          UPDATE orders
            SET status=$1,
            updated_at=NOW()
          WHERE id = ANY($2::int[])
          `,
          [syncedStatus, syncOrderIds],
        );
        await client.query(
          `
          UPDATE receivers
            SET status=$1,
            eta=$2,
            updated_at=NOW()
          WHERE order_id = ANY($3::int[])
          `,
          [syncedStatus, consignmentEta, syncOrderIds],
        );
      }
    });

    return res.json({
      success: true,
      data: {
        previousStatus: currentStatus,
        newStatus: nextStatus,
        syncedStatus,
        eta: consignmentEta,
      },
    });
  } catch (err) {
    console.error("advanceStatus ERROR:", {
      message: err.message,
      code: err.code,
      detail: err.detail,
      hint: err.hint,
      position: err.position,
      stack: err.stack,
    });

    return res.status(500).json({
      error: "Failed",
      details: err.message,
    });
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

export async function changeConsignmentStatus(req, res) {
  let syncOrderIds = [];
  let consignmentEta = null;

  try {
    const numericId = Number(req.params.id);

    if (!Number.isInteger(numericId) || numericId <= 0) {
      return res.status(400).json({
        error: "Invalid consignment ID.",
      });
    }

    const { newStatus, reason, days_offset } = req.body;

    if (!newStatus?.trim()) {
      return res.status(400).json({
        error: "newStatus is required",
      });
    }

    if (days_offset === undefined || days_offset === null) {
      return res.status(400).json({
        error: "days_offset is required",
      });
    }

    const trimmedStatus = newStatus.trim();
    const syncedStatus = trimmedStatus;

    const { rows } = await pool.query(
      `
      SELECT
        id,
        status,
        consignment_number,
        orders
      FROM consignments
      WHERE id = $1
      `,
      [numericId],
    );

    if (!rows.length) {
      return res.status(404).json({
        error: "Consignment not found",
      });
    }

    const consignment = rows[0];
    const currentStatus = consignment.status;

    if (currentStatus === trimmedStatus) {
      return res.status(400).json({
        error: "New status is the same as current status",
      });
    }

    let rawOrders = [];

    try {
      rawOrders = Array.isArray(consignment.orders)
        ? consignment.orders
        : JSON.parse(consignment.orders || "[]");
    } catch {
      rawOrders = [];
    }

    syncOrderIds = rawOrders
      .map((id) => Number(id))
      .filter((id) => Number.isInteger(id) && id > 0);

    await withTransaction(async (client) => {
      if (trimmedStatus === "Delivered") {
        consignmentEta = new Date().toISOString().split("T")[0];
      } else {
        const days = Number(days_offset);

        consignmentEta = new Date(Date.now() + days * 24 * 60 * 60 * 1000)
          .toISOString()
          .split("T")[0];
      }

      const containerStatusResult = await client.query(
        `
        SELECT container_status, order_status
        FROM statuses
        WHERE consignment_status = $1
        `,
        [trimmedStatus],
      );

      const containerStatus =
        containerStatusResult.rows[0]?.container_status ?? null;
      const orderStatus = containerStatusResult.rows[0]?.order_status ?? null;

      await client.query(
        `
        UPDATE consignments
        SET
          status = $1,
          eta = $2,
          updated_at = NOW()
        WHERE id = $3
        `,
        [trimmedStatus, consignmentEta, numericId],
      );

      if (containerStatus) {
        await client.query(
          `
            UPDATE container_master
            SET status = $1
            WHERE cid IN (
              SELECT DISTINCT cid
              FROM container_assignment_history
              WHERE consignment_id = $2
            )
          `,
          [containerStatus, numericId],
        );

        await client.query(
          `
            UPDATE container_assignment_history
            SET status = $1
            WHERE consignment_id = $2
            `,
          [containerStatus, numericId],
        );
      }

      await client.query(
        `
        INSERT INTO order_tracking (
          order_id,
          sender_id,
          sender_ref,
          receiver_id,
          container_id,
          consignment_number,
          status,
          old_status,
          item_ref,
          created_by
        )
        SELECT
          cah.order_id,
          ot.sender_id,
          ot.sender_ref,
          cah.receiver_id,
          cah.cid,
          c.consignment_number,
          $1,
          $2,
          oi.item_ref,
          $3
        FROM container_assignment_history cah
        JOIN consignments c
        ON c.id = cah.consignment_id
        JOIN order_items oi
        ON oi.id = cah.detail_id
        LEFT JOIN LATERAL (
          SELECT
            sender_id,
            sender_ref
          FROM order_tracking ot
          WHERE ot.item_ref = oi.item_ref
          ORDER BY ot.created_time DESC
          LIMIT 1
        ) ot ON TRUE
        WHERE cah.consignment_id = $4
        `,
        [
          syncedStatus,
          currentStatus,
          req.user?.username || req.user?.email || req.user?.id || "system",
          numericId,
        ],
      );

      await client.query(
        `
        INSERT INTO consignment_tracking (
          consignment_id,
          event_type,
          old_status,
          new_status,
          timestamp,
          details,
          created_at,
          source,
          action
        )
        VALUES (
          $1,
          $2,
          COALESCE($3::varchar,'Unknown'),
          $4,
          NOW(),
          $5::jsonb,
          NOW(),
          'api',
          'status_changed'
        )
        `,
        [
          numericId,
          "status_updated",
          currentStatus,
          trimmedStatus,
          JSON.stringify({
            reason: reason || "Manual status change",
            newEta: consignmentEta,
            daysOffset: Number(days_offset),
            user: req.user?.id || "system",
          }),
        ],
      );

      if (syncOrderIds.length) {
        await client.query(
          `
          UPDATE orders
          SET
            status = $1,
            updated_at = CURRENT_TIMESTAMP
          WHERE id = ANY($2::int[])
          `,
          [syncedStatus, syncOrderIds],
        );

        await client.query(
          `
          UPDATE receivers
          SET
            status = $1,
            eta = $2,
            updated_at = CURRENT_TIMESTAMP
          WHERE order_id = ANY($3::int[])
          `,
          [orderStatus, consignmentEta, syncOrderIds],
        );

        const receiversRes = await client.query(
          `
          SELECT id
          FROM receivers
          WHERE order_id = ANY($1::int[])
          `,
          [syncOrderIds],
        );

        for (const { id: receiverId } of receiversRes.rows) {
          const itemsRes = await client.query(
            `
            SELECT
              id,
              container_details
            FROM order_items
            WHERE receiver_id = $1
            `,
            [receiverId],
          );

          for (const itemRow of itemsRes.rows) {
            const containerDetails = safeParseJsonArray(
              itemRow.container_details,
            ).map((entry) =>
              entry?.container?.cid
                ? {
                    ...entry,
                    status: syncedStatus,
                  }
                : entry,
            );

            await client.query(
              `
              UPDATE order_items
              SET
                container_details = $1::jsonb,
                updated_at = NOW()
              WHERE id = $2
              `,
              [JSON.stringify(containerDetails), itemRow.id],
            );
          }

          await updateLinkedContainersStatus(
            client,
            receiverId,
            syncedStatus,
            "system",
          );
        }
      }
    });

    try {
      const updated = await pool.query(
        "SELECT * FROM consignments WHERE id = $1",
        [numericId],
      );

      await sendNotification(
        updated.rows[0],
        `status_changed_to_${trimmedStatus}`,
        {
          reason: reason || "Manual change",
          syncedOrders: syncOrderIds.length,
          syncedStatus,
          previousStatus: currentStatus,
        },
      );
    } catch {}

    return res.json({
      success: true,
      message: `Status changed to "${trimmedStatus}"`,
      data: {
        previousStatus: currentStatus,
        newStatus: trimmedStatus,
        syncedStatus,
        newEta: consignmentEta,
        daysOffset: Number(days_offset),
        affectedOrders: syncOrderIds.length,
      },
    });
  } catch (err) {
    console.error("Error changing status:", err);

    return res.status(500).json({
      error: "Failed to change status",
      details: err.message,
    });
  }
}

export async function updateSpecificItemsStatus(req, res) {
  const { orderId } = req.params;
  const {
    itemRefs,
    receiverId, // optional – extra safety filter
    status,
    notifyClient = true,
    notifyParties = true,
    forceRecalcEta = false,
  } = req.body || {};

  const created_by = req.user?.id || "system";

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

    // 1. Validate & normalize status
    const trimmedStatus = (status || "").trim().toLowerCase();
    const validStatuses = [
      "ready for loading",
      "loaded into container",
      "shipment processing",
      "shipment in transit",
      "under processing",
      "arrived at facility",
      "ready for delivery",
      "shipment delivered",
    ];

    if (!trimmedStatus || !validStatuses.includes(trimmedStatus)) {
      throw new Error(
        `Invalid status. Allowed: ${validStatuses.map((s) => s.charAt(0).toUpperCase() + s.slice(1)).join(", ")}`,
      );
    }

    const normalizedStatus = validStatuses.find((s) => s === trimmedStatus);

    // 2. Update order_items
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

    // 3. Auto-upgrade receiver status if all items delivered
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

    // 4. Optional force ETA recalculation (placeholder)
    if (forceRecalcEta) {
      for (const rid of affectedReceiverIds) {
        // Replace with your real ETA logic when available
        const etaResult = {
          eta: new Date(Date.now() + 7 * 24 * 60 * 60 * 1000),
        };
        await client.query(
          `
          UPDATE receivers 
             SET eta = $1, updated_at = CURRENT_TIMESTAMP 
           WHERE id = $2
        `,
          [etaResult.eta, rid],
        );
      }
    }

    // 5. Update order-level min ETA
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

    // 6. Log each change
    for (const row of updatedRows) {
      await client.query(
        `
        INSERT INTO order_tracking 
          (order_id, receiver_id, item_ref, status, created_by, created_time)
        VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
      `,
        [
          Number(orderId),
          row.receiver_id,
          row.item_ref,
          normalizedStatus,
          created_by,
        ],
      );
    }

    await client.query("COMMIT");

    // 7. Fetch order info (shared for all notifications)
    const orderResult = await pool.query(
      `
      SELECT 
        booking_ref,
        place_of_loading,
        place_of_delivery,
        TO_CHAR(eta, 'DD Mon YYYY') AS eta_formatted,
        sender_email,
        sender_name
      FROM orders 
      WHERE id = $1
    `,
      [Number(orderId)],
    );

    const order = orderResult.rows[0] || {};

    const routeDisplay =
      order.place_of_loading && order.place_of_delivery
        ? `${order.place_of_loading} → ${order.place_of_delivery}`
        : order.place_of_delivery || order.place_of_loading || "—";

    const templateBase = {
      statusLabel:
        normalizedStatus.charAt(0).toUpperCase() + normalizedStatus.slice(1),
      statusMsg: getStatusMessage(normalizedStatus), // assume this exists
      refId: order.booking_ref || "—",
      orderId: orderId,
      route: routeDisplay,
      etaFormatted: order.eta_formatted || "—",
      lastUpdated: new Date(),
      trackLink: "https://consolidatetracking.onrender.com/",
      currentStatus: normalizedStatus, // used for trigger check
    };

    // 8. Fetch people to notify
    const peopleRes = await pool.query(
      `
      -- Sender
      SELECT
        'sender' AS party,
        sender_email AS email,
        sender_name  AS name
      FROM orders
      WHERE id = $1
        AND sender_email IS NOT NULL
        AND sender_email != ''

      UNION ALL

      -- Affected receivers
      SELECT
        'receiver' AS party,
        receiver_email  AS email,
        receiver_name   AS name
      FROM receivers
      WHERE id = ANY($2::int[])
        AND receiver_email IS NOT NULL
        AND receiver_email != ''
    `,
      [Number(orderId), affectedReceiverIds],
    );

    const people = peopleRes.rows;

    console.log(`Notifying ${people.length} parties for order ${orderId}`);

    // 9. Send emails conditionally
    const emailPromises = [];

    for (const person of people) {
      if (!person.email?.trim()) continue;

      const isSender = person.party === "sender";

      // Determine notification type
      const notificationType = isSender
        ? "order-status-update" // or 'order-created' for new orders
        : "order-status-update";

      const templateData = {
        ...templateBase,
        receiverName: isSender
          ? "Valued Customer"
          : person.name?.trim() || "Receiver",
      };

      console.log(
        `Preparing ${notificationType} email for ${person.party} (${person.email})`,
      );

      //     emailPromises.push(
      // //  sendShipmentEmail(person.email, templateData, notificationType)
      //         .then(result => {
      //           if (result.success) {
      //             console.log(`Email sent to ${person.party} (${person.email})`);
      //           } else {
      //             console.warn(`Email failed for ${person.party} (${person.email}): ${result.error || 'Unknown error'}`);
      //           }          })
      //         .catch(err => {
      //           console.error(`Email exception for ${person.email}: ${err.message}`);
      //         })
      //     );
    }

    // Non-blocking – fire and forget
    Promise.allSettled(emailPromises);

    // 10. Success response
    return res.status(200).json({
      success: true,
      updatedCount: updateResult.rowCount,
      updatedItems: updatedRows.map((r) => ({
        itemRef: r.item_ref,
        receiverId: r.receiver_id,
        status: r.consignment_status,
      })),
      notifiedCount: people.length,
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
async function triggerNotifications(
  order,
  status,
  notifyClient,
  notifyParties,
) {
  const {
    booking_ref,
    sender_email,
    sender_contact,
    receiver_email,
    receiver_contact,
  } = order;
  const clientEmail = "support2@royalgulfshipping.com"; // Assume from order or auth

  // Mapping: Which statuses trigger what
  const notificationRules = {
    "Received for Shipment": {
      client: true,
      parties: false,
      message: "Order received and in process.",
    },
    "Waiting for Authentication": {
      client: true,
      parties: true,
      message: "Please authenticate shipment. Click to verify.",
    },
    "Shipper Authentication Confirmed": {
      client: true,
      parties: true,
      message: "Shipper confirmed. Awaiting consignee.",
    },
    "Waiting for Consignee Authentication": {
      client: true,
      parties: true,
      message: "Receiver authentication needed.",
    },
    "Waiting for Shipper Authentication (if applicable)": {
      client: true,
      parties: true,
      message: "Shipper re-authentication required.",
    },
    "Consignee Authentication Confirmed": {
      client: true,
      parties: true,
      message: "Consignee confirmed. Proceeding.",
    },
    "In Process": {
      client: true,
      parties: false,
      message: "Shipment processing complete. Ready for next steps.",
    },
    "Ready for Loading": {
      client: true,
      parties: false,
      message: "Shipment ready for container loading.",
    },
    "Loaded into Container": {
      client: true,
      parties: false,
      message: "Loaded into container.",
    },
    "Departed for Port": {
      client: true,
      parties: false,
      message: "Vessel sailed from Karachi.",
    },
    "Offloaded at Port": {
      client: true,
      parties: false,
      message: "Arrived and offloaded at Dubai port.",
    },
    "Clearance Completed": {
      client: true,
      parties: false,
      message: "Customs cleared. Ready for collection.",
    },
    Hold: {
      client: true,
      parties: false,
      message: "Shipment on hold. Contact support.",
    },
    Cancelled: { client: true, parties: true, message: "Shipment cancelled." },
    Delivered: {
      client: true,
      parties: true,
      message: "Shipment delivered successfully!",
    },
    // 'Containers Returned (Internal only)': No notification
  };

  const rule = notificationRules[status];
  if (!rule) return;

  const baseMessage = `${rule.message} Order: ${booking_ref}.`;
  const authLink = `https://portal.royalgulf.com/auth/${order.id}`; // Dynamic link

  if (notifyClient && rule.client) {
    await sendEmail(
      clientEmail,
      `Status Update: ${status}`,
      `${baseMessage} ${status.includes("Authentication") ? `Auth link: ${authLink}` : ""}`,
    );
  }

  if (notifyParties && rule.parties) {
    // Sender
    if (sender_email)
      await sendEmail(
        sender_email,
        `Action Required: ${status}`,
        `${baseMessage} ${authLink}`,
      );
    if (sender_contact) await sendSMS(sender_contact, baseMessage); // Pseudo SMS

    // Receiver
    if (receiver_email)
      await sendEmail(
        receiver_email,
        `Action Required: ${status}`,
        `${baseMessage} ${authLink}`,
      );
    if (receiver_contact) await sendSMS(receiver_contact, baseMessage);
  }
}

// Pseudo functions (implement with nodemailer/twilio)
async function sendEmail(to, subject, body) {
  // e.g., transporter.sendMail({ to, subject, html: body });
  console.log(`Email sent to ${to}: ${subject} - ${body}`);
}

async function sendSMS(to, message) {
  // e.g., client.messages.create({ body: message, from: '+123', to });
  console.log(`SMS sent to ${to}: ${message}`);
}

// Helper: Auto-transitions (e.g., auth complete → In Process)
async function handleAutoTransitions(client, orderId, newStatus) {
  // Example: If both auth confirmed, set to 'In Process'
  const authCheckQuery = `
    SELECT COUNT(*) as auth_count
    FROM receivers r
    WHERE r.order_id = $1 AND r.status IN ('Shipper Authentication Confirmed', 'Consignee Authentication Confirmed')
    GROUP BY r.order_id
    HAVING COUNT(*) >= 2  -- Assume 2 parties
  `;
  // If conditions met, update
  // await client.query('UPDATE orders SET status = \'In Process\' WHERE id = $1', [orderId]);
  // Extend for other rules (e.g., cron for reminders)
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
        r.id AS receiver_id,
        r.status AS receiver_base_status,
        r.eta AS receiver_eta,
        oi.id AS item_id,
        oi.item_ref,
        oi.total_number,
        oi.weight,
        ct.id AS ct_tracking_id,
        ct.old_status,
        ct.new_status AS ct_new_status,
        ct."timestamp" AS ct_timestamp,
        ct.event_type AS ct_event_type,
        ct.details AS ct_details,
        ct.reason,
        ct.location
      FROM order_items oi
      INNER JOIN orders o ON oi.order_id = o.id
      LEFT JOIN receivers r ON oi.receiver_id = r.id
      LEFT JOIN consignments c ON (
           c.orders @> jsonb_build_array(o.id::text)
        OR c.orders @> jsonb_build_array(o.id)
        OR c.orders ? o.id::text
      )
      LEFT JOIN consignment_tracking ct ON ct.consignment_id = c.id
        AND ct.event_type IN (
          'status_advanced',
          'status_updated',
          'order_synced',
          'status_auto_updated'
        )
      WHERE oi.item_ref ILIKE $1
      ORDER BY o.created_at DESC, oi.id, ct."timestamp" DESC
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
          place_of_loading: row.place_of_loading || null,
          place_of_delivery: row.place_of_delivery || null,
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
          eta: row.receiver_eta || null,
          items: {},
          current_status: receiverCurrent,
          status_history: [],
          remaining_status_steps: remaining,
        };
      }

      const recv = ord.receivers[recvKey];

      // ── Status history (deduplicated by ct.id) ──
      if (row.ct_tracking_id) {
        const alreadyExists = recv.status_history.some(
          (h) => h.tracking_id === row.ct_tracking_id,
        );

        if (!alreadyExists) {
          const notes = [
            row.reason ? `Reason: ${row.reason}` : "",
            row.location ? `Location: ${row.location}` : "",
            row.ct_details?.notes ? row.ct_details.notes : "",
          ]
            .filter(Boolean)
            .join(" | ");

          recv.status_history.push({
            tracking_id: row.ct_tracking_id,
            old_status: row.old_status || null,
            status: row.ct_new_status,
            time: row.ct_timestamp,
            event_type: row.ct_event_type,
            reason: row.reason || null,
            location: row.location || null,
            notes: notes || null,
          });
        }
      }

      // ── Item (only the single item shown in the layout) ──
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
    // 1. Fetch consignment core data
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

    // Parse containers safely
    let containers = [];
    try {
      containers =
        typeof cons.containers === "string"
          ? JSON.parse(cons.containers)
          : cons.containers || [];
    } catch (e) {
      console.warn("Invalid consignment containers JSON:", e);
    }

    // Parse order IDs safely
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
      // ────────────────────────────────────────────────
      // Safe casting helpers
      // ────────────────────────────────────────────────
      const safeIntCast = (val) => `COALESCE(${val}, 0)`;
      const safeNumericCast = (val) => `COALESCE(${val}, 0)`;

      // Container details subquery (latest status)
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

      // Shipping details per receiver
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

      // Fetch orders with rich receivers
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

          -- Rich receivers (matches getOrders structure)
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

      // ────────────────────────────────────────────────
      // Post-process to build summary (like old function)
      // ────────────────────────────────────────────────
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
    // ────────────────────────────────────────────────
    // Safe casting helpers (same as getOrders)
    // ────────────────────────────────────────────────
    const safeIntCast = (val) => `COALESCE(${val}, 0)`;
    const safeNumericCast = (val) => `COALESCE(${val}, 0)`;

    // ────────────────────────────────────────────────
    // Same containerDetails subquery logic as getOrders
    // ────────────────────────────────────────────────
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

    // ────────────────────────────────────────────────
    // Shipping details per receiver (matches getOrders)
    // ────────────────────────────────────────────────
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
      WHERE oi.receiver_id = r.id
      AND oi.order_id = o.id)
    `;

    // ────────────────────────────────────────────────
    // Main query – single row expected (or few if duplicate refs)
    // ────────────────────────────────────────────────
    const query = `
      SELECT 
        o.id AS order_id,
        o.booking_ref,
        o.created_at,
        o.status,
        o.eta,
        o.etd,
        o.total_assigned_qty,
        o.place_of_loading,
        o.final_destination,
        o.place_of_delivery,
        o.consignment_remarks,
        o.order_remarks,
        o.consignment_number,
        o.consignment_vessel,
        o.consignment_voyage,
        o.associated_container,
        s.sender_name,
        s.sender_contact,
        s.sender_address,
        s.sender_email,
        t.transport_type,
        t.third_party_transport,
        t.collection_scope,

        -- Receivers array – now deeply nested like getOrders
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
             ${shippingDetailsAgg.replace("shippingDetails", "shippingdetails")} AS "shippingdetails",
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
      WHERE ${isNumeric ? "o.id = $1" : "o.booking_ref ILIKE $1"}
         OR o.booking_ref ILIKE $2
      LIMIT 5  -- safety if ref is not unique
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

    // Format response – very similar to getOrders
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
        status: row.status,
        rgl_booking_number: row.booking_ref, // assuming same
        consignment_remarks: row.consignment_remarks || null,
        place_of_loading: row.place_of_loading,
        final_destination: row.final_destination,
        place_of_delivery: row.place_of_delivery,
        order_remarks: row.order_remarks,
        associated_container: row.associated_container || null,
        consignment_number: row.consignment_number || null,
        consignment_vessel: row.consignment_vessel || null,
        consignment_voyage: row.consignment_voyage || null,
        sender_name: row.sender_name || null,
        sender_contact: row.sender_contact || null,
        sender_address: row.sender_address || null,
        sender_email: row.sender_email || null,
        eta: row.eta,
        etd: row.etd || null,
        shipping_line: null, // add if needed
        transport_type: row.transport_type || null,
        third_party_transport: row.third_party_transport || null,
        collection_scope: row.collection_scope || "Partial",
        total_assigned_qty: row.total_assigned_qty || 0,
        created_at: row.created_at?.toISOString(),
        updated_at: null, // add if you select it
        created_by: null, // add if needed
        receivers, // ← now matches getOrders shape exactly
        // defaults for frontend compatibility (same as getOrders)
        receiver_name: null,
        receiver_contact: null,
        // ... all the other null defaults you have in getOrders
        overall_status: row.status,
        color: "#E0E0E0",
        // etc.
      };
    });

    const data = formatted.length === 1 ? formatted[0] : formatted;

    res.json({
      success: true,
      data,
      message: `Found ${formatted.length} matching order(s)`,
    });
  } catch (err) {
    console.error("getOrderByOrderId error:", err.stack || err);
    res.status(500).json({
      success: false,
      message: "Failed to fetch order details",
      details: process.env.NODE_ENV === "development" ? err.message : undefined,
      code: err.code,
      position: err.position,
    });
  }
}

/**
 * Get order details by RGL Booking Number
 * Example: GET /api/orders/rgl/RGSL-17695-064
 */
export async function getOrderByRglBookingNo(req, res) {
  console.log("getOrderByRglBookingNo called with params:", req.params);
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
    // ────────────────────────────────────────────────
    // Safe casting helpers (same as before)
    // ────────────────────────────────────────────────
    const safeIntCast = (val) => `COALESCE(${val}, 0)`;
    const safeNumericCast = (val) => `COALESCE(${val}, 0)`;

    // ────────────────────────────────────────────────
    // containerDetails subquery (same as getOrders)
    // ────────────────────────────────────────────────
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

    // ────────────────────────────────────────────────
    // shippingDetails aggregation per receiver
    // ────────────────────────────────────────────────
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
      WHERE oi.receiver_id = r.id
      AND oi.order_id = o.id)
    `;

    // ────────────────────────────────────────────────
    // Main query – search by rgl_booking_number
    // ────────────────────────────────────────────────
    const query = `
      SELECT 
        o.id AS order_id,
        o.booking_ref,
        o.rgl_booking_number,
        o.created_at,
        o.status,
        o.eta,
        o.etd,
        o.total_assigned_qty,
        o.place_of_loading,
        o.final_destination,
        o.place_of_delivery,
        o.consignment_remarks,
        o.order_remarks,
        o.consignment_number,
        o.consignment_vessel,
        o.consignment_voyage,
        o.associated_container,
        s.sender_name,
        s.sender_contact,
        s.sender_address,
        s.sender_email,
        t.transport_type,
        t.third_party_transport,
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

    // Format response (same shape as getOrders / getOrderByOrderId)
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
        status: row.status,
        consignment_remarks: row.consignment_remarks || null,
        place_of_loading: row.place_of_loading,
        final_destination: row.final_destination,
        place_of_delivery: row.place_of_delivery,
        order_remarks: row.order_remarks,
        associated_container: row.associated_container || null,
        consignment_number: row.consignment_number || null,
        consignment_vessel: row.consignment_vessel || null,
        consignment_voyage: row.consignment_voyage || null,
        sender_name: row.sender_name || null,
        sender_contact: row.sender_contact || null,
        sender_address: row.sender_address || null,
        sender_email: row.sender_email || null,
        eta: row.eta,
        etd: row.etd || null,
        shipping_line: null, // fill if needed
        transport_type: row.transport_type || null,
        third_party_transport: row.third_party_transport || null,
        collection_scope: row.collection_scope || "Partial",
        total_assigned_qty: row.total_assigned_qty || 0,
        created_at: row.created_at?.toISOString(),
        updated_at: null, // add if you select it
        created_by: null, // add if needed
        receivers,
        overall_status: row.status,
        color: "#E0E0E0",
        // ... other defaults your frontend expects
      };
    });

    const data = formatted.length === 1 ? formatted[0] : formatted;

    res.json({
      success: true,
      data,
      message: `Found ${formatted.length} matching order(s) for RGL Booking Number`,
    });
  } catch (err) {
    console.error("getOrderByRglBookingNo error:", err.stack || err);
    res.status(500).json({
      success: false,
      message: "Failed to fetch order details",
      details: process.env.NODE_ENV === "development" ? err.message : undefined,
      code: err.code,
      position: err.position,
    });
  }
}

// getOrderStatuses: Updated to fetch from order_tracking (merged statuses); group by order_id for history
export async function getOrderStatuses(req, res) {
  try {
    const { order_id } = req.params || req.query; // Support both

    if (!order_id) {
      return res.status(400).json({ error: "order_id is required" });
    }

    const query = `
      SELECT ot.id, ot.order_id, ot.status, ot.created_by, ot.created_time,
        r.receiver_name, r.receiver_ref, c.container_number
      FROM order_tracking ot
      LEFT JOIN receivers r ON ot.receiver_id = r.id
      LEFT JOIN containers c ON ot.container_id = c.id
      WHERE ot.order_id = $1
      ORDER BY ot.created_time ASC
    `;

    const result = await pool.query(query, [order_id]);
    const statuses = result.rows;

    console.log(`Fetched ${statuses.length} statuses for order: ${order_id}`);
    res.json({ statuses });
  } catch (err) {
    console.error("Error fetching order statuses:", err.message);
    res
      .status(500)
      .json({ error: "Failed to fetch order statuses", details: err.message });
  }
}

// getOrderUsageHistory: Updated to join order_tracking, transport_details for usage logs; adapt filters
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

    console.log(
      `Fetched ${history.length} usage history for order: ${order_id}`,
    );
    res.json({ history });
  } catch (err) {
    console.error("Error fetching order usage history:", err.message);
    res.status(500).json({
      error: "Failed to fetch order usage history",
      details: err.message,
    });
  }
}

// cancelOrder: Updated to update orders.status and insert/update order_tracking for cancellation log
export async function cancelOrder(req, res) {
  let client;
  try {
    client = await pool.connect();
    await client.query("BEGIN");

    const { id } = req.params;
    const { reason } = req.body || {}; // Optional reason
    const updated_by = req.user?.id || "system"; // Assume user from auth

    if (!id) {
      throw new Error("Order ID is required");
    }

    // Update global order status
    const updateQuery = `
      UPDATE orders 
      SET status = 'Cancelled', updated_at = CURRENT_TIMESTAMP, updated_by = $2
      WHERE id = $1 AND status != 'Cancelled'
      RETURNING id, booking_ref
    `;
    const updateResult = await client.query(updateQuery, [id, updated_by]);
    if (updateResult.rowCount === 0) {
      await client.query("ROLLBACK");
      return res
        .status(404)
        .json({ error: "Order not found or already cancelled" });
    }

    // Log cancellation in order_tracking (for all receivers or latest)
    const trackingQuery = `
      INSERT INTO order_tracking (order_id, status, created_by, created_time)
      SELECT $1, 'Cancelled', $2, CURRENT_TIMESTAMP
      WHERE NOT EXISTS (SELECT 1 FROM order_tracking ot WHERE ot.order_id = $1 AND ot.status = 'Cancelled')
    `;
    await client.query(trackingQuery, [id, updated_by]);

    await client.query("COMMIT");
    console.log(
      `Cancelled order: ${updateResult.rows[0].booking_ref || id}, reason: ${reason || "N/A"}`,
    );
    res.json({ message: "Order cancelled successfully", order_id: id, reason });
  } catch (err) {
    if (client) {
      await client.query("ROLLBACK");
    }
    console.error("Error cancelling order:", err.message);
    res
      .status(500)
      .json({ error: "Failed to cancel order", details: err.message });
  } finally {
    if (client) {
      client.release();
    }
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
        booking_ref
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
