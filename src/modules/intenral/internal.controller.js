import pool from "../../db/pool.js";
import logger from "../../services/logger.js";
import {
  notifyOrderStatusUpdate,
  sendShipmentEmail,
} from "../../services/sendOrderEmail.js";

const BATCH_SIZE = 5;
const INTERNAL_SECRET = process.env.EMAIL_QUEUE_SECRET;

export const processEmailQueue = async (req, res) => {
  const provided = req.headers["x-internal-secret"];
  if (!INTERNAL_SECRET || provided !== INTERNAL_SECRET) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  const client = await pool.connect();
  let batch;
  try {
    await client.query("BEGIN");

    const result = await client.query(
      `SELECT id, order_id, recipient_email, recipient_name, email_type
       FROM email_queue
       WHERE status = 'pending'
       ORDER BY created_at ASC
       LIMIT $1
       FOR UPDATE SKIP LOCKED`,
      [BATCH_SIZE],
    );
    batch = result.rows;

    if (batch.length > 0) {
      await client.query(
        `UPDATE email_queue SET status = 'sending' WHERE id = ANY($1)`,
        [batch.map((r) => r.id)],
      );
    }

    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    client.release();
    console.error("[process-email-queue] Failed to claim batch:", err.message);
    return res.status(500).json({ error: "Failed to claim batch" });
  }
  client.release();

  const results = [];
  for (const row of batch) {
    try {
      let result;

      if (row.email_type === "order_created") {
        result = await sendShipmentEmail({
          email: row.recipient_email,
          orderId: row.order_id,
          receiverName: row.recipient_name || "Valued Customer",
        });
      } else if (row.email_type === "order_update") {
        result = await notifyOrderStatusUpdate(row.order_id, {
          receiverName: row.recipient_name || "Valued Customer",
        });
      } else {
        throw new Error(`Unsupported email_type "${row.email_type}"`);
      }

      if (!result.success) {
        throw new Error(result.error || result.message || "Send failed");
      }

      await pool.query(
        `UPDATE email_queue SET status = 'sent', sent_at = now() WHERE id = $1`,
        [row.id],
      );
      results.push({ id: row.id, status: "sent" });
    } catch (err) {
      await pool.query(
        `UPDATE email_queue
         SET status = 'failed', attempts = attempts + 1, last_error = $2
         WHERE id = $1`,
        [row.id, err.message],
      );
      results.push({ id: row.id, status: "failed", error: err.message });
    }
  }

  return res.status(200).json({
    success: true,
    message: "Email Sent!",
    processed: results.length,
    results,
  });
};
export const verifyRecaptcha = async (req, res) => {
  const { token } = req.body;

  if (!token || typeof token !== "string") {
    return res.status(400).json({ success: false, error: "Missing token" });
  }

  const secretKey = process.env.RECAPTCHA_SECRET_KEY;
  if (!secretKey) {
    logger.error("RECAPTCHA_SECRET_KEY is not set");
    return res
      .status(500)
      .json({ success: false, error: "Server misconfiguration" });
  }

  try {
    const params = new URLSearchParams({
      secret: secretKey,
      response: token,
    });

    const googleRes = await fetch(
      "https://www.google.com/recaptcha/api/siteverify",
      {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: params.toString(),
      },
    );

    const data = await googleRes.json();

    const minScore = 0.5;
    const passed =
      data.success && (data.score === undefined || data.score >= minScore);

    if (!passed) {
      logger.info("reCAPTCHA verification failed", {
        errorCodes: data["error-codes"],
        score: data.score,
      });
    }

    return res.json({ success: passed, score: data.score ?? null });
  } catch (error) {
    logger.error("reCAPTCHA verification error", { error: error.message });
    return res
      .status(500)
      .json({ success: false, error: "Verification failed" });
  }
};
