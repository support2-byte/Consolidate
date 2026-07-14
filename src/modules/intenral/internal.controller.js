import pool from "../../db/pool";
import { sendOrderConfirmationEmail } from "../../services/sendOrderConfirmationEmail.js";

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
      await sendOrderConfirmationEmail({
        to: row.recipient_email,
        name: row.recipient_name,
        orderId: row.order_id,
      });
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
