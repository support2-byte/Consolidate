import pool from "../../db/pool.js";
import logger from "../../services/logger.js";
import { sendConfirmationEmail } from "../../services/sendConfirmationEmail.js";
import {
  notifySingleStatusUpdate,
  sendOrderEmail,
  sendShipmentEmail,
} from "../../services/sendOrderEmail.js";
import { sendInvoiceEmail } from "../../services/sendInvoiceEmail.js";
import { generateOtp } from "../../services/generateOtp.js";
import { QUEUES, sendConfirmationById } from "../../services/queueSenders.js";

const FORM_BASE_URL = process.env.FORM_BASE_URL || "http://localhost:5174";

export const getAllNotifications = async (req, res) => {
  try {
    const { rows } = await pool.query(
      `SELECT eq.id, o.rgl_booking_number AS order_form_no, eq.tracking_id,
              COALESCE(oi.status, o.status) AS item_status,
              eq.recipient_type, eq.recipient_email, eq.recipient_name,
              eq.email_type, eq.status, eq.attempts, eq.last_error,
              eq.created_at, eq.sent_at
         FROM email_queue eq
         JOIN orders o ON o.id = eq.order_id
         LEFT JOIN order_items oi ON oi.tracking_id = eq.tracking_id
        ORDER BY eq.created_at DESC, eq.id DESC`,
    );
    if (rows.length === 0) {
      logger.warn("No Email Notificaitons found!");
      return res
        .status(404)
        .json({ success: false, message: "No Email Notificaitons found!" });
    }
    return res.status(200).json({
      success: true,
      notifications: rows,
    });
  } catch (error) {
    logger.error("Failed to fetch Notifications", error);
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
};

export const getEmailSubscriptions = async (req, res) => {
  try {
    const { rows } = await pool.query(
      `SELECT ns.id, o.rgl_booking_number AS order_form_no, ns.reference_id,
              ns.email, ns.created_at, ns.updated_at
         FROM notification_subscriptions ns
         JOIN orders o ON o.id = ns.order_id
        ORDER BY ns.created_at DESC`,
    );
    if (rows.length === 0) {
      logger.warn("No Email Subscription found!");
      return res
        .status(404)
        .json({ success: false, message: "No Email Subscription found!" });
    }
    return res.status(200).json({
      success: true,
      notifications: rows,
    });
  } catch (error) {
    logger.error("Failed to fetch Email Subscription", error);
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
};

export const resendNotification = async (req, res) => {
  const { id } = req.params;

  try {
    const { rows } = await pool.query(
      "SELECT * FROM email_queue WHERE id = $1",
      [id],
    );

    if (rows.length === 0) {
      return res
        .status(404)
        .json({ success: false, message: "Notification not found" });
    }

    const notif = rows[0];

    if (!notif.recipient_email) {
      return res
        .status(400)
        .json({ success: false, message: "No recipient email on this record" });
    }

    let result;

    result = await QUEUES.emails.send(id);

    if (!result.success) {
      const errMsg = result.error || result.message || "Unknown error";

      if (result.skipped) {
        const { rows: skippedRows } = await pool.query(
          `UPDATE email_queue SET status = 'skipped', last_error = $2
            WHERE id = $1
        RETURNING *`,
          [id, errMsg],
        );
        return res.status(200).json({
          success: false,
          skipped: true,
          message: errMsg,
          row: skippedRows[0],
        });
      }

      const { rows: failedRows } = await pool.query(
        `UPDATE email_queue
            SET status = 'failed', attempts = attempts + 1, last_error = $2
          WHERE id = $1
      RETURNING *`,
        [id, errMsg],
      );
      logger.error("Resend notification failed", { id, error: errMsg });
      return res
        .status(500)
        .json({ success: false, message: errMsg, row: failedRows[0] });
    }

    const { rows: sentRows } = await pool.query(
      `UPDATE email_queue
          SET status = 'sent', attempts = attempts + 1, sent_at = NOW(), last_error = NULL
        WHERE id = $1
    RETURNING *`,
      [id],
    );

    return res.status(200).json({
      success: true,
      message: "Email sent",
      row: sentRows[0],
    });

    await pool.query(
      `UPDATE email_queue
          SET status = 'sent', attempts = attempts + 1, sent_at = NOW(), last_error = NULL
        WHERE id = $1`,
      [id],
    );

    return res.status(200).json({ success: true, message: "Email sent" });
  } catch (error) {
    logger.error("Failed to resend notification", { id, error: error.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
};

export const deleteEmailQueue = async (req, res) => {
  try {
    const { id } = req.params;

    const result = await pool.query(
      "DELETE FROM email_queue WHERE id = $1 RETURNING id",
      [id],
    );

    if (result.rowCount === 0) {
      logger.info("Couldn't delete email queue", { emailId: id });

      return res.status(404).json({
        success: false,
        message: "Email queue not found",
      });
    }

    return res.status(200).json({
      success: true,
      message: "Email deleted from the queue",
    });
  } catch (error) {
    logger.error("Failed to delete email queue", { id, error: error.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
};

export const getConfirmationEmails = async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT
        eq.id,
        bc.form_id,
        p.party_type,
        p.name AS recipient_name,
        eq.recipient_email,
        eq.cc_emails,
        eq.bcc_emails,
        p.booking_submitted_at,
        eq.status,
        eq.last_error,
        eq.created_at,
        eq.sent_at,
        p.uuid AS party_uuid,
        bc.subject,
        bc.message,
        bc.mode,
        bc.total_qty,
        bc.total_weight,
        c.company,
        c.logo_url AS company_logo_url,
        bs.name AS sender_name,
        br.name AS receiver_name,
        COALESCE(items.items, '[]'::json) AS items
      FROM confirmation_email_queue eq
      JOIN booking_confirmations bc ON bc.id = eq.order_confirmation_id
      JOIN companies c ON c.id = bc.company_id
      JOIN LATERAL (
        SELECT 'sender' AS party_type, s.name, s.uuid, s.booking_submitted_at
          FROM booking_confirmation_senders s WHERE s.email_queue_id = eq.id
        UNION ALL
        SELECT 'receiver', r.name, r.uuid, r.booking_submitted_at
          FROM booking_confirmation_receivers r WHERE r.email_queue_id = eq.id
      ) p ON true
      LEFT JOIN booking_confirmation_senders bs ON bs.booking_form_id = bc.id
      LEFT JOIN booking_confirmation_receivers br ON br.booking_form_id = bc.id
      LEFT JOIN LATERAL (
        SELECT json_agg(json_build_object(
          'category', bci.category,
          'subcategory', bci.subcategory,
          'type', bci.type,
          'qty', bci.qty,
          'weight', bci.weight,
          'placeOfLoading', bci.place_of_loading,
          'placeOfDestination', bci.place_of_destination
        ) ORDER BY bci.id) AS items
        FROM booking_confirmation_items bci
        WHERE bci.booking_form_id = bc.id
      ) items ON true
      ORDER BY eq.created_at DESC NULLS LAST
    `);

    return res.json({ success: true, notifications: rows });
  } catch (err) {
    logger.error("getConfirmationEmails failed", err);
    if (err.code === "23503") {
      return res.status(409).json({
        success: false,
        message:
          "This email is linked to a booking form and cannot be deleted.",
      });
    }
    return res
      .status(500)
      .json({ message: "Failed to load confirmation emails" });
  }
};

export const resendConfirmationEmail = async (req, res) => {
  const { id } = req.params;

  try {
    const result = await sendConfirmationById(id);

    if (result.skipped) {
      return res.status(409).json({ success: false, message: result.message });
    }
    if (!result.success) {
      throw new Error(result.error || "Send failed");
    }

    const { rows: updatedRows } = await pool.query(
      `UPDATE confirmation_email_queue
          SET status = 'sent', sent_at = NOW(), attempts = attempts + 1, last_error = NULL
        WHERE id = $1
      RETURNING id, recipient_email, status, attempts, last_error, created_at, sent_at`,
      [id],
    );

    return res.status(200).json({ success: true, row: updatedRows[0] });
  } catch (err) {
    logger.error("Failed to resend confirmation email", {
      id,
      error: err.message,
    });

    await pool.query(
      `UPDATE confirmation_email_queue
          SET attempts = attempts + 1, last_error = $2
        WHERE id = $1`,
      [id, err.message],
    );

    return res
      .status(500)
      .json({ success: false, message: "Failed to send email" });
  }
};

export const deleteConfirmationEmail = async (req, res) => {
  const { id } = req.params;

  try {
    const { rows } = await pool.query(
      `SELECT status FROM confirmation_email_queue WHERE id = $1`,
      [id],
    );

    if (!rows.length) {
      return res
        .status(404)
        .json({ success: false, message: "Queue entry not found" });
    }

    if (String(rows[0].status).toLowerCase() === "sent") {
      return res
        .status(400)
        .json({ success: false, message: "Sent emails cannot be deleted." });
    }

    await pool.query(`DELETE FROM confirmation_email_queue WHERE id = $1`, [
      id,
    ]);

    return res
      .status(200)
      .json({ success: true, message: "Queue entry deleted" });
  } catch (err) {
    logger.error("Failed to delete confirmation email", {
      id,
      error: err.message,
    });
    return res
      .status(500)
      .json({ success: false, message: "Failed to delete queue entry" });
  }
};

const INVOICE_LOOKUP = `
  SELECT id, invoice_id, amount, status, ngenius_order_ref, public_ref, 'overstayed'::varchar
    FROM overstay_invoices
   WHERE q.email_type = 'overstayed' AND id = q.invoice_id
  UNION ALL
  SELECT id, invoice_id, amount, status, ngenius_order_ref, public_ref, 'storage'::varchar
    FROM storage_invoices
   WHERE q.email_type = 'storage' AND id = q.invoice_id
  UNION ALL
  SELECT id, invoice_id, amount, status, ngenius_order_ref, public_ref, 'delivery'::varchar
    FROM delivery_invoices
   WHERE q.email_type = 'delivery' AND id = q.invoice_id
  UNION ALL
  SELECT id, invoice_id, amount, status, ngenius_order_ref, public_ref, 'dropoff'::varchar
    FROM dropoff_invoices
   WHERE q.email_type = 'dropoff' AND id = q.invoice_id
`;

export const getAllInvoiceEmails = async (req, res) => {
  try {
    const { rows } = await pool.query(
      `SELECT
         q.id, q.recipient_id, q.recipient_name, q.recipient_email, q.email_type,
         q.status, q.item_ref, q.attempts, q.created_at, q.sent_at,
         i.invoice_id, i.amount, i.status AS invoice_status,
         i.ngenius_order_ref, i.public_ref,
         oi.category, oi.subcategory
       FROM invoice_email_queue q
       LEFT JOIN LATERAL (${INVOICE_LOOKUP}) i ON true
       LEFT JOIN order_items oi ON oi.item_ref = q.item_ref
       ORDER BY q.created_at DESC, q.id DESC`,
    );
    if (rows.length === 0) {
      logger.warn("No Invoice Emails found!");
      return res
        .status(404)
        .json({ success: false, message: "No Invoice Emails found!" });
    }
    return res.status(200).json({ success: true, notifications: rows });
  } catch (error) {
    logger.error("Failed to fetch Invoice Emails", error);
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
};

export const resendInvoiceNotification = async (req, res) => {
  const { id } = req.params;

  try {
    const { rows } = await pool.query(
      `SELECT q.*, i.invoice_id, i.amount, i.public_ref
         FROM invoice_email_queue q
         LEFT JOIN LATERAL (${INVOICE_LOOKUP}) i ON true
        WHERE q.id = $1`,
      [id],
    );

    if (rows.length === 0) {
      return res
        .status(404)
        .json({ success: false, message: "Invoice email not found" });
    }

    const notif = rows[0];

    const result = await sendInvoiceEmail({
      email: notif.recipient_email,
      recipientId: notif.recipient_id,
      itemRef: notif.item_ref,
      receiverName: notif.recipient_name || "Valued Customer",
      invoiceId: notif.invoice_id,
      amount: notif.amount,
      invoiceLink: `${FORM_BASE_URL}/invoice-payment/${encodeURIComponent(notif.public_ref || notif.invoice_id)}`,
    });

    if (!result.success) {
      const errMsg = result.error || result.message || "Unknown error";
      await pool.query(
        `UPDATE invoice_email_queue
            SET status = 'failed', attempts = attempts + 1
          WHERE id = $1`,
        [id],
      );
      logger.error("Resend invoice email failed", { id, error: errMsg });
      return res.status(500).json({ success: false, message: errMsg });
    }

    await pool.query(
      `UPDATE invoice_email_queue
        SET status = 'sent', attempts = attempts + 1, sent_at = NOW()
      WHERE id = $1`,
      [id],
    );

    return res.status(200).json({ success: true, message: "Email sent" });
  } catch (error) {
    logger.error("Failed to resend invoice email", {
      id,
      error: error.message,
    });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
};

export const deleteInvoiceEmailQueue = async (req, res) => {
  try {
    const { id } = req.params;

    const result = await pool.query(
      "DELETE FROM invoice_email_queue WHERE id = $1 RETURNING id",
      [id],
    );

    if (result.rowCount === 0) {
      return res
        .status(404)
        .json({ success: false, message: "Invoice email not found" });
    }

    return res
      .status(200)
      .json({ success: true, message: "Invoice email deleted from queue" });
  } catch (error) {
    logger.error("Failed to delete invoice email", {
      id,
      error: error.message,
    });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
};
