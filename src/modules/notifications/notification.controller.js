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

const FORM_BASE_URL = process.env.FORM_BASE_URL || "http://localhost:5174";

export const getAllNotifications = async (req, res) => {
  try {
    const { rows } = await pool.query(
      `SELECT eq.id, o.rgl_booking_number AS order_form_no, eq.item_ref, oi.status AS item_status, eq.recipient_type,
              eq.recipient_email, eq.recipient_name, eq.email_type, eq.status,
              eq.attempts, eq.last_error, eq.created_at, eq.sent_at
         FROM email_queue eq
         JOIN orders o ON o.id = eq.order_id
         LEFT JOIN order_items oi ON TRIM(oi.item_ref) ILIKE TRIM(eq.item_ref)
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

    if (notif.email_type === "order_created") {
      result = await sendShipmentEmail({
        email: notif.recipient_email,
        orderId: notif.order_id,
        itemRef: notif.item_ref,
        recipientId: notif.recipient_id,
        recipientType: notif.recipient_type,
        receiverName: notif.recipient_name || "Valued Customer",
      });
    } else {
      result = await notifySingleStatusUpdate({
        email: notif.recipient_email,
        itemRef: notif.item_ref,
        orderId: notif.order_id,
        recipientId: notif.recipient_id,
        recipientType: notif.recipient_type,
        receiverName: notif.recipient_name || "Valued Customer",
      });
    }

    if (!result.success) {
      const errMsg = result.error || result.message || "Unknown error";

      if (result.skipped) {
        await pool.query(
          `UPDATE email_queue SET status = 'skipped', last_error = $2 WHERE id = $1`,
          [id, errMsg],
        );
        return res
          .status(200)
          .json({ success: false, skipped: true, message: errMsg });
      }

      await pool.query(
        `UPDATE email_queue
            SET status = 'failed', attempts = attempts + 1, last_error = $2
          WHERE id = $1`,
        [id, errMsg],
      );
      logger.error("Resend notification failed", { id, error: errMsg });
      return res.status(500).json({ success: false, message: errMsg });
    }

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
        eq.status,
        eq.created_at,
        eq.sent_at,
        eq.recipient_email,
        eq.cc_emails,
        eq.bcc_emails,
        bc.form_id,
        bc.subject,
        bc.message,
        bc.mode,
        bc.total_qty,
        bc.total_weight,
        c.company,
        c.logo_url AS company_logo_url,
        COALESCE(r.name, s.name) AS recipient_name,
        COALESCE(r.uuid, s.uuid) AS party_uuid,
        CASE WHEN r.id IS NOT NULL THEN 'receiver' ELSE 'sender' END AS party_type,
        s.name AS sender_name,
        COALESCE(items.items, '[]'::json) AS items
      FROM confirmation_email_queue eq
      JOIN booking_confirmations bc ON bc.id = eq.order_confirmation_id
      JOIN companies c ON c.id = bc.company_id
      LEFT JOIN booking_confirmation_receivers r
        ON r.email_queue_id = eq.id
      LEFT JOIN booking_confirmation_senders s
        ON s.email_queue_id = eq.id
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
    return res
      .status(500)
      .json({ message: "Failed to load confirmation emails" });
  }
};

export const resendConfirmationEmail = async (req, res) => {
  const { id } = req.params;

  try {
    const { rows } = await pool.query(
      `SELECT
        eq.id, eq.recipient_email, eq.cc_emails, eq.bcc_emails,
        bc.form_id, bc.mode, bc.total_qty, bc.total_weight,
        c.company, c.logo_url AS company_logo_url,
        COALESCE(r.name, s.name) AS recipient_name,
        COALESCE(r.uuid, s.uuid) AS party_uuid,
        CASE WHEN r.id IS NOT NULL THEN 'receiver' ELSE 'sender' END AS party_type,
        s.name AS sender_name,
        COALESCE(items.items, '[]'::json) AS items
      FROM confirmation_email_queue eq
      JOIN booking_confirmations bc ON bc.id = eq.order_confirmation_id
      JOIN companies c ON c.id = bc.company_id
      LEFT JOIN booking_confirmation_receivers r ON r.email_queue_id = eq.id
      LEFT JOIN booking_confirmation_senders s ON s.email_queue_id = eq.id
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
      WHERE eq.id = $1`,
      [id],
    );

    if (!rows.length) {
      return res
        .status(404)
        .json({ success: false, message: "Queue entry not found" });
    }

    const entry = rows[0];
    const newOtp = generateOtp();

    if (entry.party_type === "receiver") {
      await pool.query(
        `UPDATE booking_confirmation_receivers
            SET otp = $2, otp_verified_at = NULL
          WHERE email_queue_id = $1`,
        [id, newOtp],
      );
    } else {
      await pool.query(
        `UPDATE booking_confirmation_senders
            SET otp = $2, otp_verified_at = NULL
          WHERE email_queue_id = $1`,
        [id, newOtp],
      );
    }

    const viewLink =
      entry.form_id && entry.party_uuid
        ? `${FORM_BASE_URL}/booking-confirmation/${entry.party_uuid}/${entry.party_type}`
        : "#";

    const result = await sendConfirmationEmail({
      recipientEmail: entry.recipient_email,
      recipientName: entry.recipient_name,
      senderName: entry.sender_name,
      companyName: entry.company,
      companyLogo: entry.company_logo_url,
      mode: entry.mode,
      totalQty: entry.total_qty,
      totalWeight: entry.total_weight,
      lastUpdated: entry.created_at,
      viewLink,
      ccEmails: entry.cc_emails,
      bccEmails: entry.bcc_emails,
      otp: newOtp,
      items: entry.items,
    });

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
  SELECT id, invoice_id, amount, status, ngenius_order_ref, 'overstayed'::varchar
    FROM overstay_invoices
   WHERE q.email_type = 'overstayed' AND id = q.invoice_id
  UNION ALL
  SELECT id, invoice_id, amount, status, ngenius_order_ref, 'storage'::varchar
    FROM storage_invoices
   WHERE q.email_type = 'storage' AND id = q.invoice_id
  UNION ALL
  SELECT id, invoice_id, amount, status, ngenius_order_ref, 'delivery'::varchar
    FROM delivery_invoices
   WHERE q.email_type = 'delivery' AND id = q.invoice_id
  UNION ALL
  SELECT id, invoice_id, amount, status, ngenius_order_ref, 'dropoff'::varchar
    FROM dropoff_invoices
   WHERE q.email_type = 'dropoff' AND id = q.invoice_id
`;

export const getAllInvoiceEmails = async (req, res) => {
  try {
    const { rows } = await pool.query(
      `SELECT
         q.id, q.recipient_id, q.recipient_name, q.recipient_email, q.email_type,
         q.status, q.item_ref, q.attempts, q.created_at, q.sent_at, q.otp,
         i.invoice_id, i.amount, i.status AS invoice_status,
         i.ngenius_order_ref,
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
      `SELECT q.*, i.invoice_id, i.amount
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

    const otp = generateOtp();

    const result = await sendInvoiceEmail({
      email: notif.recipient_email,
      recipientId: notif.recipient_id,
      itemRef: notif.item_ref,
      receiverName: notif.recipient_name || "Valued Customer",
      invoiceId: notif.invoice_id,
      amount: notif.amount,
      otp,
      invoiceLink: `${FORM_BASE_URL}/invoice-payment/${encodeURIComponent(notif.invoice_id)}`,
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
          SET status = 'sent', attempts = attempts + 1, sent_at = NOW(), otp = $2
        WHERE id = $1`,
      [id, otp],
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
