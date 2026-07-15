import pool from "../../db/pool.js";
import logger from "../../services/logger.js";
import {
  notifyOrderStatusUpdate,
  sendOrderEmail,
  sendShipmentEmail,
} from "../../services/sendOrderEmail.js";

export const getAllNotifications = async (req, res) => {
  try {
    const { rows } = await pool.query(
      `SELECT eq.id, o.rgl_booking_number AS order_form_no, eq.recipient_type,
              eq.recipient_email, eq.recipient_name, eq.email_type, eq.status,
              eq.attempts, eq.last_error, eq.created_at, eq.sent_at
         FROM email_queue eq
         JOIN orders o ON o.id = eq.order_id
        ORDER BY eq.created_at DESC`,
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
        recipientId: notif.recipient_id,
        recipientType: notif.recipient_type,
        receiverName: notif.recipient_name || "Valued Customer",
      });
    } else if (notif.email_type === "order_update") {
      result = await notifyOrderStatusUpdate(notif.order_id, {
        receiverName: notif.recipient_name || "Valued Customer",
      });
    } else {
      return res.status(400).json({
        success: false,
        message: `Unsupported email_type "${notif.email_type}" for resend`,
      });
    }

    if (!result.success) {
      const errMsg = result.error || result.message || "Unknown error";
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
          SET status = 'sent', sent_at = now(), attempts = attempts + 1, last_error = NULL
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
