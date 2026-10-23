import pool from "../../db/pool.js";
import logger from "../../services/logger.js";
import { QUEUES } from "../../services/queueSenders.js";
import { triggerBatchTick } from "../../services/batchRunner.js";

const validKey = (k) => Object.prototype.hasOwnProperty.call(QUEUES, k);

export const getBatchStatus = async (req, res) => {
  const { queueKey } = req.params;
  if (!validKey(queueKey))
    return res.status(400).json({ success: false, message: "Invalid queue" });

  try {
    const { table } = QUEUES[queueKey];
    const [{ rows: pend }, { rows: jobs }] = await Promise.all([
      pool.query(
        `SELECT COUNT(*)::int AS n FROM ${table} WHERE status='pending'`,
      ),
      pool.query(
        `SELECT * FROM email_batch_jobs WHERE queue_key=$1 ORDER BY id DESC LIMIT 1`,
        [queueKey],
      ),
    ]);
    return res.json({
      success: true,
      pending: pend[0].n,
      job: jobs[0] || null,
    });
  } catch (err) {
    logger.error("getBatchStatus failed", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
};

export const startBatch = async (req, res) => {
  const { queueKey } = req.params;
  if (!validKey(queueKey))
    return res.status(400).json({ success: false, message: "Invalid queue" });

  try {
    const { table } = QUEUES[queueKey];
    const { rows: pend } = await pool.query(
      `SELECT COUNT(*)::int AS n FROM ${table} WHERE status='pending'`,
    );
    if (pend[0].n === 0)
      return res
        .status(400)
        .json({ success: false, message: "No pending emails in this queue" });

    const { rows } = await pool.query(
      `INSERT INTO email_batch_jobs (queue_key) VALUES ($1)
       ON CONFLICT (queue_key) WHERE status = 'running' DO NOTHING
       RETURNING *`,
      [queueKey],
    );
    if (!rows.length)
      return res.status(409).json({
        success: false,
        message: "A batch is already running for this queue",
      });
    triggerBatchTick();
    return res.status(202).json({
      success: true,
      message: "Batch sending started",
      job: rows[0],
      pending: pend[0].n,
    });
  } catch (err) {
    logger.error("startBatch failed", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
};

export const stopBatch = async (req, res) => {
  const { queueKey } = req.params;
  if (!validKey(queueKey))
    return res.status(400).json({ success: false, message: "Invalid queue" });

  try {
    const { rowCount } = await pool.query(
      `UPDATE email_batch_jobs
          SET status='stopped', finished_at=NOW()
        WHERE queue_key=$1 AND status='running'`,
      [queueKey],
    );
    if (!rowCount)
      return res
        .status(404)
        .json({ success: false, message: "No running batch for this queue" });
    return res.json({ success: true, message: "Batch sending stopped" });
  } catch (err) {
    logger.error("stopBatch failed", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
};
