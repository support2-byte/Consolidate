import pool from "../../../db/pool.js";
import logger from "../../../services/logger.js";
import {
  createOrder,
  getOrderStatus,
} from "../../../services/ngeniusService.js";

export const getInvoices = async (req, res) => {
  const { id } = req.params;

  try {
    const { rows } = await pool.query(
      `SELECT * FROM invoices WHERE customer_id = $1`,
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
      `SELECT * FROM invoices WHERE id = $1`,
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
        `SELECT id, customer_id, shipment, storage, days, amount, status, created_at
         FROM storage_purchase
         WHERE shipment = $1`,
        [invoice.shipment_id],
      );
      extra = rows[0] ?? null;
    } else {
      const { rows } = await pool.query(
        `SELECT id, customer_id, shipment, delivery_amount AS amount, status, created_at
         FROM app_delivery
         WHERE shipment = $1`,
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
    const { rows } = await pool.query(`SELECT * FROM invoices WHERE id = $1`, [
      invoiceId,
    ]);

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
      `UPDATE invoices SET ngenius_order_ref = $1 WHERE id = $2`,
      [orderReferenceId, invoiceId],
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
      const result = await pool.query(
        `UPDATE invoices SET status = 'paid' WHERE ngenius_order_ref = $1`,
        [orderReferenceId],
      );
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
