import pool from "../../../db/pool.js";
import logger from "../../../services/logger.js";
import {
  createOrder,
  getOrderStatus,
} from "../../../services/ngeniusService.js";

const INVOICE_TABLES = {
  delivery: "delivery_invoices",
  storage: "storage_invoices",
  dropoff: "dropoff_invoices",
  overstay: "overstay_invoices",
};

const INVOICES_UNION = `
  SELECT id, invoice_id, amount, status, created_at, shipment_ref AS shipment_id, customer_ref AS customer_id, ngenius_order_ref, public_ref, 'delivery' AS invoice_type FROM delivery_invoices
  UNION ALL
  SELECT id, invoice_id, amount, status, created_at, shipment_ref, customer_ref, ngenius_order_ref, public_ref, 'storage' FROM storage_invoices
  UNION ALL
  SELECT id, invoice_id, amount, status, created_at, shipment_ref, customer_ref, ngenius_order_ref, public_ref, 'dropoff' FROM dropoff_invoices
  UNION ALL
  SELECT id, invoice_id, amount, status, created_at, shipment_ref, customer_ref, ngenius_order_ref, public_ref, 'overstay' FROM overstay_invoices
`;

export const getInvoices = async (req, res) => {
  const { id } = req.params;

  try {
    const { rows } = await pool.query(
      `SELECT * FROM (${INVOICES_UNION}) inv WHERE customer_id = $1 ORDER BY created_at DESC`,
      [id],
    );

    if (rows.length === 0) {
      logger.warn(`No Invoices found for receiver ${id}`);

      return res.status(404).json({
        success: false,
        message: "No Invoices found.",
        data: [],
      });
    }

    logger.info(`Found ${rows.length} Invoices for receiver ${id}`);

    return res.status(200).json({
      success: true,
      data: rows,
    });
  } catch (error) {
    logger.error("Failed to get Invoices", {
      customer_id: id,
      error,
    });

    return res.status(500).json({
      success: false,
      message: "Something went wrong.",
    });
  }
};

export const getInvoiceDetails = async (req, res) => {
  const { invoiceId } = req.params;

  try {
    const { rows: invoiceRows } = await pool.query(
      `SELECT * FROM (${INVOICES_UNION}) inv WHERE public_ref = $1::uuid`,
      [invoiceId],
    );

    if (invoiceRows.length === 0) {
      logger.warn(`Invoice not found`, { invoiceId });
      return res.status(404).json({
        success: false,
        message: "Invoice not found.",
      });
    }

    const invoice = invoiceRows[0];
    let extra = null;

    if (invoice.invoice_type?.toLowerCase() === "storage") {
      const { rows } = await pool.query(
        `SELECT id, customer_ref AS customer_id, shipment_ref AS shipment, storage, duration_days AS days, amount, status, created_at
         FROM storage_purchase
         WHERE shipment_ref = $1`,
        [invoice.shipment_id],
      );
      extra = rows[0] ?? null;
    } else if (invoice.invoice_type === "delivery") {
      const { rows } = await pool.query(
        `SELECT id, customer_ref AS customer_id, shipment_ref AS shipment, delivery_amount AS amount, status, created_at
         FROM delivery_requests
         WHERE shipment_ref = $1`,
        [invoice.shipment_id],
      );
      extra = rows[0] ?? null;
    }

    logger.info(`Fetched invoice details`, {
      invoiceId,
      hasExtra: !!extra,
    });

    return res.status(200).json({
      success: true,
      data: {
        invoice,
        details: extra,
      },
    });
  } catch (error) {
    logger.error("Failed to get invoice details", {
      invoiceId,
      error: error.message,
      stack: error.stack,
    });
    return res.status(500).json({
      success: false,
      message: "Something went wrong.",
    });
  }
};

export const createNgeniusOrder = async (req, res) => {
  const { invoiceId } = req.params;

  try {
    const { rows } = await pool.query(
      `SELECT * FROM (${INVOICES_UNION}) inv WHERE public_ref = $1::uuid`,
      [invoiceId],
    );

    if (rows.length === 0) {
      return res
        .status(404)
        .json({ success: false, message: "Invoice not found." });
    }

    const invoice = rows[0];

    if (invoice.status?.toLowerCase().includes("paid")) {
      return res
        .status(400)
        .json({ success: false, message: "Invoice already paid." });
    }

    const { paymentUrl, orderReferenceId } = await createOrder({
      orderReference: `INV-${invoice.invoice_id}-${Date.now()}`,
      amount: Number(invoice.amount),
      currencyCode: "AED",
      redirectUrl: `${process.env.APP_BASE_URL}/api/mobile-app/payments/ngenius/redirect`,
    });

    await pool.query(
      `UPDATE ${INVOICE_TABLES[invoice.invoice_type]} SET ngenius_order_ref = $1 WHERE id = $2`,
      [orderReferenceId, invoice.id],
    );

    return res.status(200).json({
      success: true,
      data: { paymentUrl, orderReferenceId },
    });
  } catch (error) {
    logger.error("Failed to create N-Genius order", {
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
  const { orderReferenceId } = req.params;

  try {
    const { state } = await getOrderStatus(orderReferenceId);
    if (state === "CAPTURED" || state === "PURCHASED") {
      for (const table of Object.values(INVOICE_TABLES)) {
        await pool.query(
          `UPDATE ${table} SET status = 'paid' WHERE ngenius_order_ref = $1`,
          [orderReferenceId],
        );
      }
    }

    return res.status(200).json({ success: true, data: { state } });
  } catch (error) {
    logger.error("Failed to confirm N-Genius payment", {
      orderReferenceId,
      error: error.message,
      stack: error.stack,
    });
    return res
      .status(500)
      .json({ success: false, message: "Failed to confirm payment." });
  }
};
