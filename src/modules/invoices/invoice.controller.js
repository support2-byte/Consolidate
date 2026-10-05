import pool from "../../db/pool.js";
import axios from "axios";
import { getZohoAccessToken } from "../../services/getZohoAccessToken.js";
import logger from "../../services/logger.js";
import { createOrder, getOrderStatus } from "../../services/ngeniusService.js";
import {
  zohoHeaders,
  zohoParams,
} from "../zoho-invoices/zoho-bill.controller.js";
import { getZohoDueContext } from "../zoho-invoices/zoho-invoice.service.js";
import {
  getBooksBaseUrl,
  UPSERT_SQL,
} from "../zoho-invoices/zoho-invoice.controller.js";

const INVOICE_TABLES = {
  overstayed: "overstay_invoices",
  storage: "storage_invoices",
  delivery: "delivery_invoices",
  dropoff: "dropoff_invoices",
};

const DELIVERY_OPTION_LABELS = {
  offloadingSupport: "Offloading Support",
  markingRequired: "Marking Required",
  expressDelivery: "Express Delivery",
};

const ZONE_LABELS = {
  dubai: "Dubai",
  sharjah: "Sharjah",
  ajman: "Ajman",
  abuDhabi: "Abu Dhabi",
  alAin: "Al Ain",
  northernEmirates: "Northern Emirates (RAK / Fujairah / UAQ)",
};

const REQUEST_DETAILS = {
  storage: {
    column: "storage_purchase_id",
    sql: `SELECT storage, size_value, size_unit, type, notes, duration_days,
                 to_char(required_from, 'YYYY-MM-DD') AS required_from
            FROM storage_purchase WHERE id = $1`,
    map: (r) => ({
      storageType: r.type,
      size: [r.size_value, r.size_unit].filter(Boolean).join(" "),
      quantity: r.storage,
      requiredFrom: r.required_from,
      durationDays: r.duration_days,
      notes: r.notes,
    }),
  },
  delivery: {
    column: "delivery_request_id",
    sql: `SELECT delivery_contact_number, delivery_address, delivery_options
            FROM delivery_requests WHERE id = $1`,
    map: (r) => ({
      deliveryContact: r.delivery_contact_number,
      deliveryAddress: r.delivery_address,
      deliveryOptions: Object.entries(r.delivery_options || {})
        .filter(([, selected]) => selected)
        .map(([key]) => DELIVERY_OPTION_LABELS[key] || key),
    }),
  },
  dropoff: {
    column: "drop_off_requests_id",
    sql: `SELECT contact_number, pickup_address, zone,
               to_char(pickup_date, 'YYYY-MM-DD') AS pickup_date
          FROM drop_off_requests WHERE id = $1`,
    map: (r) => ({
      contactNumber: r.contact_number,
      pickupAddress: r.pickup_address,
      pickupDate: r.pickup_date,
      zone: ZONE_LABELS[r.zone] || r.zone,
    }),
  },
};

async function findInvoice(invoiceId) {
  for (const [type, table] of Object.entries(INVOICE_TABLES)) {
    const { rows } = await pool.query(
      `SELECT * FROM ${table} WHERE invoice_id = $1`,
      [invoiceId],
    );
    if (rows.length) return { type, table, invoice: rows[0] };
  }
  return null;
}

export async function createOverstayedInvoice(req, res) {
  let client;
  try {
    client = await pool.connect();
    await client.query("BEGIN");

    const {
      invoiceId,
      orderId,
      receiverId,
      itemRef,
      overstayDays,
      baseRate,
      taxPercent,
      subtotal,
      total,
      discount,
    } = req.body;

    if (!receiverId || !itemRef) {
      await client.query("ROLLBACK");
      return res
        .status(400)
        .json({ error: "receiverId and itemRef are required" });
    }

    const orderItemRes = await client.query(
      `SELECT id, order_id, item_ref FROM order_items
       WHERE item_ref = $1 AND order_id = $2
       LIMIT 1`,
      [itemRef, orderId],
    );
    if (orderItemRes.rowCount === 0) {
      await client.query("ROLLBACK");
      return res.status(404).json({ error: "Order item not found" });
    }

    const receiverRes = await client.query(
      `SELECT id, receiver_name, receiver_contact, receiver_email, receiver_ref
       FROM receivers
       WHERE id = $1 AND order_id = $2
       LIMIT 1`,
      [receiverId, orderId],
    );
    if (receiverRes.rowCount === 0) {
      await client.query("ROLLBACK");
      return res.status(404).json({ error: "Receiver not found" });
    }
    const receiver = receiverRes.rows[0];

    if (!receiver.receiver_email) {
      await client.query("ROLLBACK");
      return res.status(400).json({
        error: "Receiver has no email on file — cannot queue notification",
      });
    }

    const zohoContext = await getZohoDueContext({
      type: "overstayed",
      itemRef,
      customerRef: receiver.receiver_ref,
    });

    const finalAmount = Number.isFinite(Number(total)) ? Number(total) : 0;
    const finalSubtotal = Number.isFinite(Number(subtotal))
      ? Number(subtotal)
      : 0;

    const invoiceRes = await client.query(
      `INSERT INTO overstay_invoices
        (invoice_id, amount, status, shipment_ref, customer_ref,
         overstay_days, tax_percent, subtotal, discount, due_amount, due_details)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
       RETURNING *`,
      [
        invoiceId,
        finalAmount + zohoContext.dueAmount,
        "pending",
        itemRef,
        receiver.receiver_ref,
        Number.isFinite(Number(overstayDays)) ? Number(overstayDays) : null,
        Number.isFinite(Number(taxPercent)) ? Number(taxPercent) : null,
        finalSubtotal,
        discount,
        zohoContext.dueAmount,
        JSON.stringify(zohoContext.dueInvoices),
      ],
    );
    const invoice = invoiceRes.rows[0];

    await client.query(
      `INSERT INTO invoice_email_queue
        (recipient_id, recipient_name, recipient_email, email_type, status, item_ref, attempts, invoice_id)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
      [
        receiver.id,
        receiver.receiver_name,
        receiver.receiver_email,
        "overstayed",
        "pending",
        itemRef,
        0,
        invoice.id,
      ],
    );

    await client.query("COMMIT");

    return res.status(201).json({
      success: true,
      message: "Invoice Created Successfully",
      invoice: {
        invoiceId: invoice.invoice_id,
        createdAt: invoice.created_at,
        dueAmount: zohoContext.dueAmount,
        dueInvoices: zohoContext.dueInvoices,
      },
    });
  } catch (error) {
    console.error("Error creating overstayed invoice:", error);
    if (client) await client.query("ROLLBACK");
    return res
      .status(500)
      .json({ error: "Internal server error", details: error.message });
  } finally {
    if (client) client.release();
  }
}

export const getInvoicePayment = async (req, res) => {
  const { invoiceId } = req.params;

  try {
    const found = await findInvoice(invoiceId);

    if (!found) {
      return res.status(404).json({ message: "Invoice not found." });
    }

    const { type, invoice: invoiceRow } = found;

    const orderItemResult = await pool.query(
      `SELECT order_id, category, subcategory
       FROM order_items
       WHERE item_ref = $1
       LIMIT 1`,
      [invoiceRow.shipment_ref],
    );

    if (orderItemResult.rows.length === 0) {
      return res
        .status(404)
        .json({ message: "Shipment not found for this invoice." });
    }

    const {
      order_id: orderId,
      category: itemCategory,
      subcategory: itemSubcategory,
    } = orderItemResult.rows[0];

    const orderResult = await pool.query(
      `SELECT booking_ref, rgl_booking_number FROM orders WHERE id = $1`,
      [orderId],
    );
    const orderRow = orderResult.rows[0] || null;

    const receiverResult =
      type === "dropoff"
        ? await pool.query(
            `SELECT sender_name AS receiver_name, sender_contact AS receiver_contact,
                    sender_email AS receiver_email
             FROM senders
             WHERE sender_ref = $1 AND order_id = $2
             LIMIT 1`,
            [invoiceRow.customer_ref, orderId],
          )
        : await pool.query(
            `SELECT receiver_name, receiver_contact, receiver_email
             FROM receivers
             WHERE receiver_ref = $1 AND order_id = $2
             LIMIT 1`,
            [invoiceRow.customer_ref, orderId],
          );

    const receiverRow = receiverResult.rows[0] || null;
    let details = null;
    const lookup = REQUEST_DETAILS[type];
    if (lookup && invoiceRow[lookup.column]) {
      const detailResult = await pool.query(lookup.sql, [
        invoiceRow[lookup.column],
      ]);
      details = detailResult.rows[0] ? lookup.map(detailResult.rows[0]) : null;
    }

    let overstayFields = {};
    if (type === "overstayed") {
      const overstayDays = invoiceRow.overstay_days;
      const subtotalVal =
        invoiceRow.subtotal !== null ? Number(invoiceRow.subtotal) : null;
      const discountVal =
        invoiceRow.discount !== null ? Number(invoiceRow.discount) : null;
      overstayFields = {
        overstayDays,
        baseRate:
          subtotalVal !== null && overstayDays
            ? Number((subtotalVal / overstayDays).toFixed(2))
            : null,
        taxPercent:
          invoiceRow.tax_percent !== null
            ? Number(invoiceRow.tax_percent)
            : null,
        subtotal: subtotalVal,
        discount: discountVal,
      };
    }

    return res.json({
      invoice: {
        invoiceId: invoiceRow.invoice_id,
        invoiceType: type,
        amount: Number(invoiceRow.amount),
        dueAmount: Number(invoiceRow.due_amount || 0),
        dueInvoices: invoiceRow.due_details || [],
        status: invoiceRow.status,
        createdAt: invoiceRow.created_at,
        shipmentId: invoiceRow.shipment_ref,
        category: itemCategory,
        subcategory: itemSubcategory,
        details,
        ...overstayFields,
      },
      order: orderRow
        ? {
            bookingRef: orderRow.booking_ref,
            formNumber: orderRow.rgl_booking_number,
          }
        : null,
      receiver: receiverRow
        ? {
            receiverName: receiverRow.receiver_name,
            receiverContact: receiverRow.receiver_contact,
            receiverEmail: receiverRow.receiver_email,
          }
        : null,
      company: null,
    });
  } catch (error) {
    console.error("getInvoicePayment error:", error);
    return res.status(500).json({ message: "Unable to load invoice." });
  }
};

export const createWebNgeniusOrder = async (req, res) => {
  const { invoiceId } = req.params;

  try {
    const found = await findInvoice(invoiceId);

    if (!found) {
      return res
        .status(404)
        .json({ success: false, message: "Invoice not found." });
    }

    const { table, invoice } = found;

    if (invoice.status?.toLowerCase().includes("paid")) {
      return res
        .status(400)
        .json({ success: false, message: "Invoice already paid." });
    }

    const { paymentUrl, orderReferenceId } = await createOrder({
      orderReference: invoice.invoice_id,
      amount: Number(invoice.amount),
      currencyCode: "AED",
      redirectUrl: `${process.env.WEB_APP_BASE_URL}/invoice-payment/${invoice.invoice_id}?paid=1`,
    });
    logger.info("createWebNgeniusOrder: order created", {
      invoiceId,
      orderReferenceId,
      paymentUrl,
    });

    const updateResult = await pool.query(
      `UPDATE ${table} SET ngenius_order_ref = $1 WHERE id = $2 RETURNING id, ngenius_order_ref`,
      [orderReferenceId, invoice.id],
    );
    logger.info("createWebNgeniusOrder: ngenius_order_ref saved", {
      invoiceId,
      updateResult: updateResult.rows,
    });

    return res.status(200).json({
      success: true,
      data: { paymentUrl, orderReferenceId },
    });
  } catch (error) {
    logger.error("Failed to create N-Genius order (web)", {
      invoiceId,
      error: error.message,
      stack: error.stack,
    });
    return res
      .status(500)
      .json({ success: false, message: "Failed to initiate payment." });
  }
};

export const confirmNgeniusPayment = async (req, res) => {
  const { invoiceId } = req.params;
  logger.info("confirmNgeniusPayment: called", { invoiceId });

  try {
    const found = await findInvoice(invoiceId);

    if (!found) {
      logger.warn("confirmNgeniusPayment: invoice not found", { invoiceId });
      return res
        .status(404)
        .json({ success: false, message: "Invoice not found." });
    }

    const { type, table, invoice } = found;

    logger.info("confirmNgeniusPayment: invoice loaded", {
      invoiceId,
      id: invoice.id,
      status: invoice.status,
      ngenius_order_ref: invoice.ngenius_order_ref,
    });

    if (invoice.status?.toLowerCase() === "paid") {
      logger.info("confirmNgeniusPayment: already paid", { invoiceId });
      return res.status(200).json({ success: true, data: { state: "PAID" } });
    }

    if (!invoice.ngenius_order_ref) {
      logger.warn("confirmNgeniusPayment: no ngenius_order_ref stored", {
        invoiceId,
      });
      return res.status(400).json({
        success: false,
        message: "No payment attempt found for this invoice.",
      });
    }

    const orderStatus = await getOrderStatus(invoice.ngenius_order_ref);
    logger.info("confirmNgeniusPayment: getOrderStatus result", {
      invoiceId,
      ngenius_order_ref: invoice.ngenius_order_ref,
      state: orderStatus.state,
      raw: JSON.stringify(orderStatus.raw),
    });

    const { state } = orderStatus;

    if (state === "CAPTURED" || state === "PURCHASED") {
      const updateResult = await pool.query(
        `UPDATE ${table} SET status = 'paid' WHERE id = $1 RETURNING id, status`,
        [invoice.id],
      );
      logger.info("confirmNgeniusPayment: status updated to paid", {
        invoiceId,
        updateResult: updateResult.rows,
      });
    } else {
      logger.warn(
        "confirmNgeniusPayment: state not captured/purchased, not marking paid",
        {
          invoiceId,
          state,
        },
      );
    }

    return res.status(200).json({ success: true, data: { state } });
  } catch (error) {
    logger.error("Failed to confirm N-Genius payment", {
      invoiceId,
      error: error.message,
      stack: error.stack,
    });
    return res
      .status(500)
      .json({ success: false, message: "Failed to confirm payment." });
  }
};

export const getOverstayInvoices = async (req, res) => {
  try {
    const { rows } = await pool.query(
      `SELECT
         oi.id,
         oi.invoice_id,
         oi.amount,
         oi.status,
         oi.created_at,
         oi.shipment_ref,
         oi.customer_ref,
         oi.overstay_days,
         oi.tax_percent,
         oi.subtotal,
         oi.due_amount,
         oi.due_details,
         oi.zoho_invoice_id,
         r.receiver_name,
         r.receiver_contact,
         r.receiver_email,
         it.category,
         it.subcategory,
         it.order_id,
         o.booking_ref,
         o.rgl_booking_number
       FROM overstay_invoices oi
       LEFT JOIN order_items it ON it.item_ref = oi.shipment_ref
       LEFT JOIN orders o ON o.id = it.order_id
       LEFT JOIN receivers r
         ON r.receiver_ref = oi.customer_ref AND r.order_id = it.order_id
       ORDER BY oi.created_at DESC`,
    );

    const invoices = rows.map((row) => ({
      id: row.id,
      invoiceId: row.invoice_id,
      amount: Number(row.amount),
      status: row.status,
      createdAt: row.created_at,
      shipmentId: row.shipment_ref,
      overstayDays: row.overstay_days,
      taxPercent: row.tax_percent !== null ? Number(row.tax_percent) : null,
      subtotal: row.subtotal !== null ? Number(row.subtotal) : null,
      dueAmount: Number(row.due_amount || 0),
      dueInvoices: row.due_details || [],
      zohoInvoiceId: row.zoho_invoice_id,
      baseRate:
        row.subtotal !== null && row.overstay_days
          ? Number((Number(row.subtotal) / row.overstay_days).toFixed(2))
          : null,
      category: row.category,
      subcategory: row.subcategory,
      receiverName: row.receiver_name,
      receiverContact: row.receiver_contact,
      receiverEmail: row.receiver_email,
      orderRef: row.rgl_booking_number || row.booking_ref,
    }));

    return res.json({ success: true, invoices });
  } catch (error) {
    console.error("getOverstayInvoices error:", error);
    return res
      .status(500)
      .json({ success: false, message: "Unable to load invoices." });
  }
};

async function listInvoices(
  table,
  res,
  { withStorageDetails = false, party = "receiver" } = {},
) {
  const partyJoin =
    party === "sender"
      ? `LEFT JOIN senders r ON r.sender_ref = inv.customer_ref AND r.order_id = oi.order_id`
      : `LEFT JOIN receivers r ON r.receiver_ref = inv.customer_ref AND r.order_id = oi.order_id`;
  const partySelect =
    party === "sender"
      ? `r.sender_name AS receiver_name, r.sender_contact AS receiver_contact, r.sender_email AS receiver_email`
      : `r.receiver_name, r.receiver_contact, r.receiver_email`;
  const storageJoin = withStorageDetails
    ? `LEFT JOIN storage_purchase sp ON sp.id = inv.storage_purchase_id`
    : "";
  const storageSelect = withStorageDetails
    ? `CONCAT_WS(' ', sp.size_value, sp.size_unit) AS size, sp.type AS storage_type,`
    : "";

  const { rows } = await pool.query(
    `SELECT
       inv.id, inv.invoice_id, inv.amount, inv.status, inv.created_at,
       inv.shipment_ref, inv.customer_ref, inv.due_amount, inv.due_details,
       inv.zoho_invoice_id,
       ${storageSelect}
       oi.category, oi.subcategory, oi.order_id,
       o.booking_ref, o.rgl_booking_number,
       ${partySelect}
     FROM ${table} inv
     LEFT JOIN order_items oi ON oi.item_ref = inv.shipment_ref
     LEFT JOIN orders o ON o.id = oi.order_id
     ${partyJoin}
     ${storageJoin}
     ORDER BY inv.created_at DESC`,
  );

  return rows.map((row) => ({
    id: row.id,
    invoiceId: row.invoice_id,
    amount: Number(row.amount),
    status: row.status,
    createdAt: row.created_at,
    shipmentId: row.shipment_ref,
    dueAmount: Number(row.due_amount || 0),
    dueInvoices: row.due_details || [],
    zohoInvoiceId: row.zoho_invoice_id,
    category: row.category,
    subcategory: row.subcategory,
    ...(withStorageDetails
      ? { size: row.size, storageType: row.storage_type }
      : {}),
    receiverName: row.receiver_name,
    receiverContact: row.receiver_contact,
    receiverEmail: row.receiver_email,
    orderRef: row.rgl_booking_number || row.booking_ref,
  }));
}

export const getStorageInvoices = async (req, res) => {
  try {
    const invoices = await listInvoices("storage_invoices", res, {
      withStorageDetails: true,
    });

    return res.json({ success: true, invoices });
  } catch (error) {
    console.error("getStorageInvoices error:", error);
    return res
      .status(500)
      .json({ success: false, message: "Unable to load invoices." });
  }
};

export const getDeliveryInvoices = async (req, res) => {
  try {
    const invoices = await listInvoices("delivery_invoices", res);
    return res.json({ success: true, invoices });
  } catch (error) {
    console.error("getDeliveryInvoices error:", error);
    return res
      .status(500)
      .json({ success: false, message: "Unable to load invoices." });
  }
};

export const getDropoffInvoices = async (req, res) => {
  try {
    const invoices = await listInvoices("dropoff_invoices", res, {
      party: "sender",
    });
    return res.json({ success: true, invoices });
  } catch (error) {
    console.error("getDropoffInvoices error:", error);
    return res
      .status(500)
      .json({ success: false, message: "Unable to load invoices." });
  }
};

const ZOHO_LINE_NAMES = {
  overstayed: "Overstay Charges",
  storage: "Storage Charges",
  delivery: "Delivery Charges",
  dropoff: "Drop-off Charges",
};

export const uploadInvoiceToZohoManual = async (req, res) => {
  try {
    const found = await findInvoice(req.params.invoiceId);
    if (!found) {
      return res
        .status(404)
        .json({ success: false, message: "Invoice not found." });
    }
    const { table, invoice } = found;

    if (invoice.zoho_invoice_id) {
      return res.status(409).json({
        success: false,
        message: "Invoice already uploaded to Zoho.",
      });
    }

    const ctx = await getZohoDueContext({
      type,
      itemRef: invoice.shipment_ref,
      customerRef: invoice.customer_ref,
    });
    if (!ctx.orderNumber) {
      return res
        .status(422)
        .json({ success: false, message: "Order number not found." });
    }
    if (!ctx.customerId) {
      return res
        .status(422)
        .json({ success: false, message: "Customer not found in Zoho." });
    }

    const consRes = await pool.query(
      `SELECT c.consignment_number
         FROM order_items oi
         JOIN container_assignment_history h ON h.detail_id = oi.id
         JOIN consignments c ON c.id = h.consignment_id
        WHERE oi.item_ref = $1
        ORDER BY h.id DESC
        LIMIT 1`,
      [invoice.shipment_ref],
    );
    const consignmentNumber =
      consRes.rows[0]?.consignment_number?.trim() ||
      ctx.consignmentNumber ||
      null;

    const { rows: paidLocal } = await pool.query(
      `SELECT invoice_number FROM zoho_invoices
        WHERE btrim(order_number) = $1
          AND btrim(invoice_number) = $2
          AND status = 'paid'
          AND ($3::text IS NULL
               OR consignment_number IS NULL
               OR btrim(consignment_number) = $3)
        LIMIT 1`,
      [ctx.orderNumber, invoice.invoice_id, consignmentNumber],
    );
    if (paidLocal.length) {
      return res.status(409).json({
        success: false,
        message: "Invoice is already paid in Zoho.",
      });
    }

    const token = await getZohoAccessToken();
    const headers = zohoHeaders(token);

    const existing = await axios.get(`${getBooksBaseUrl()}/invoices`, {
      headers,
      params: { ...zohoParams(), invoice_number: invoice.invoice_id },
    });
    const hit = (existing.data.invoices || []).find(
      (z) => z.invoice_number === invoice.invoice_id,
    );
    if (hit) {
      await pool.query(
        `UPDATE ${table} SET zoho_invoice_id = $1, zoho_uploaded_at = NOW()
          WHERE id = $2 AND zoho_invoice_id IS NULL`,
        [hit.invoice_id, invoice.id],
      );
      return res.status(409).json({
        success: false,
        message:
          hit.status === "paid"
            ? "Invoice is already paid in Zoho."
            : "Invoice number already exists in Zoho.",
      });
    }

    const dueAmount = Number(invoice.due_amount || 0);
    const taxPercent = Number(invoice.tax_percent || 0);
    const taxId = process.env.ZOHO_TAX_ID;

    const lineItem =
      type === "overstayed"
        ? {
            name: ZOHO_LINE_NAMES[type],
            description: `Shipment ${invoice.shipment_ref}`,
            rate: Number(
              (Number(invoice.subtotal) / invoice.overstay_days).toFixed(2),
            ),
            quantity: invoice.overstay_days,
            ...(taxPercent > 0 && taxId ? { tax_id: taxId } : {}),
          }
        : {
            name: ZOHO_LINE_NAMES[type],
            description: `Shipment ${invoice.shipment_ref}`,
            rate: Number((Number(invoice.amount) - dueAmount).toFixed(2)),
            quantity: 1,
          };

    const dueLines = (
      Array.isArray(invoice.due_details) ? invoice.due_details : []
    ).map((d) => ({
      name: `Previous Balance - ${d.invoiceNumber}`,
      description: d.dueDate ? `Due ${d.dueDate}` : undefined,
      rate: Number(d.balance),
      quantity: 1,
    }));

    const body = {
      customer_id: ctx.customerId,
      invoice_number: invoice.invoice_id,
      reference_number: ctx.orderNumber,
      date: new Date(invoice.created_at).toISOString().slice(0, 10),
      line_items: [lineItem, ...dueLines],
      custom_field_hash: {
        cf_order_number: ctx.orderNumber,
        ...(consignmentNumber
          ? { cf_consignment_number: consignmentNumber }
          : {}),
      },
    };

    const created = await axios.post(`${getBooksBaseUrl()}/invoices`, body, {
      headers,
      params: { ...zohoParams(), ignore_auto_number_generation: true },
    });
    if (created.data.code !== 0) {
      throw new Error(created.data.message || "Zoho error");
    }

    const zi = created.data.invoice;
    const zohoInvoiceId = zi.invoice_id;
    logger.info("Zoho invoice created", {
      invoiceNumber: zi.invoice_number,
      customFields: zi.custom_fields,
    });
    await pool.query(
      `UPDATE ${table} SET zoho_invoice_id = $1, zoho_uploaded_at = NOW()
        WHERE id = $2`,
      [zohoInvoiceId, invoice.id],
    );

    try {
      await pool.query(UPSERT_SQL, [
        zohoInvoiceId,
        zi.invoice_number,
        zi.customer_id,
        zi.customer_name,
        zi.reference_number,
        zi.status,
        zi.date,
        zi.due_date || null,
        zi.currency_code,
        zi.total ?? 0,
        zi.balance ?? zi.total ?? 0,
        ctx.orderNumber,
        consignmentNumber,
        zi.last_modified_time || null,
      ]);
    } catch (insertErr) {
      logger.warn("Zoho invoice created but local insert failed", {
        zohoInvoiceId,
        err: insertErr.message,
      });
    }

    return res.json({ success: true, zohoInvoiceId });
  } catch (error) {
    logger.error("uploadInvoiceToZoho error", {
      err: error.response?.data || error.message,
    });
    return res.status(500).json({
      success: false,
      message:
        error.response?.data?.message ||
        error.message ||
        "Unable to upload invoice to Zoho.",
    });
  }
};
