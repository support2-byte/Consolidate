import pool from "../db/pool.js";
import logger from "./logger.js";
import { QUEUES } from "./queueSenders.js";

const TICK_MS = 30 * 1000;
const STALE_PROCESSING_MIN = 5;
const JOB_LOCK_STALE_MIN = 15;
const DELAY_BETWEEN_EMAILS_MS = 1000;

let shuttingDown = false;
process.on("SIGTERM", () => {
  shuttingDown = true;
});
process.on("SIGINT", () => {
  shuttingDown = true;
});

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const claimNext = async (table) => {
  const { rows } = await pool.query(
    `UPDATE ${table}
        SET status = 'processing', claimed_at = NOW()
      WHERE id = (
        SELECT id FROM ${table}
         WHERE status = 'pending'
         ORDER BY created_at ASC, id ASC
         LIMIT 1
         FOR UPDATE SKIP LOCKED
      )
      RETURNING id`,
  );
  return rows[0]?.id ?? null;
};

const markResult = async (table, id, r) => {
  if (r.success) {
    await pool.query(
      `UPDATE ${table}
          SET status='sent', attempts=attempts+1, sent_at=NOW(),
              last_error=NULL, claimed_at=NULL
        WHERE id=$1`,
      [id],
    );
    return "sent";
  }
  if (r.skipped) {
    await pool.query(
      `UPDATE ${table} SET status='skipped', last_error=$2, claimed_at=NULL WHERE id=$1`,
      [id, r.message || r.error || "Skipped"],
    );
    return "skipped";
  }
  await pool.query(
    `UPDATE ${table}
        SET status='failed', attempts=attempts+1, last_error=$2, claimed_at=NULL
      WHERE id=$1`,
    [id, r.error || r.message || "Unknown error"],
  );
  return "failed";
};

const isJobStillRunning = async (jobId) => {
  const { rows } = await pool.query(
    "SELECT status FROM email_batch_jobs WHERE id=$1",
    [jobId],
  );
  return rows[0]?.status === "running";
};

const runBatch = async (job) => {
  const queue = QUEUES[job.queue_key];
  const counts = { sent: 0, failed: 0, skipped: 0 };
  let exhausted = false;
  let interrupted = false;

  for (let i = 0; i < job.batch_size; i++) {
    if (shuttingDown || !(await isJobStillRunning(job.id))) {
      interrupted = true;
      break;
    }

    const id = await claimNext(queue.table);
    if (!id) {
      exhausted = true;
      break;
    }

    let result;
    try {
      result = await queue.send(id);
    } catch (err) {
      logger.error("Batch send threw", {
        queue: job.queue_key,
        id,
        error: err.message,
      });
      result = { success: false, error: err.message };
    }

    counts[await markResult(queue.table, id, result)]++;
    await sleep(DELAY_BETWEEN_EMAILS_MS);
  }

  const totals = [job.id, counts.sent, counts.failed, counts.skipped];

  if (interrupted) {
    await pool.query(
      `UPDATE email_batch_jobs
          SET locked_at=NULL,
              total_sent=total_sent+$2, total_failed=total_failed+$3,
              total_skipped=total_skipped+$4
        WHERE id=$1`,
      totals,
    );
    return;
  }

  if (!exhausted) {
    const { rows } = await pool.query(
      `SELECT 1 FROM ${queue.table} WHERE status='pending' LIMIT 1`,
    );
    exhausted = rows.length === 0;
  }

  if (exhausted) {
    await pool.query(
      `UPDATE email_batch_jobs
          SET status='completed', finished_at=NOW(), locked_at=NULL,
              batches_run=batches_run+1,
              total_sent=total_sent+$2, total_failed=total_failed+$3,
              total_skipped=total_skipped+$4
        WHERE id=$1 AND status='running'`,
      totals,
    );
  } else {
    await pool.query(
      `UPDATE email_batch_jobs
          SET next_run_at = NOW() + make_interval(mins => pause_minutes),
              locked_at=NULL, batches_run=batches_run+1,
              total_sent=total_sent+$2, total_failed=total_failed+$3,
              total_skipped=total_skipped+$4
        WHERE id=$1 AND status='running'`,
      totals,
    );
  }

  logger.info("Batch finished", { queue: job.queue_key, ...counts, exhausted });
};

const claimDueJob = async () => {
  const { rows } = await pool.query(
    `UPDATE email_batch_jobs
        SET locked_at = NOW()
      WHERE id = (
        SELECT id FROM email_batch_jobs
         WHERE status='running'
           AND next_run_at <= NOW()
           AND (locked_at IS NULL OR locked_at < NOW() - make_interval(mins => $1::int))
         ORDER BY next_run_at
         LIMIT 1
         FOR UPDATE SKIP LOCKED
      )
      RETURNING *`,
    [JOB_LOCK_STALE_MIN],
  );
  return rows[0] || null;
};

/* Rows left in 'processing' after a crash: never auto-resend (may already be delivered). */
const recoverStaleRows = async () => {
  for (const [key, { table }] of Object.entries(QUEUES)) {
    try {
      const { rowCount } = await pool.query(
        `UPDATE ${table}
            SET status='failed', claimed_at=NULL,
                last_error='Interrupted by server restart - verify before resending'
          WHERE status='processing'
            AND claimed_at < NOW() - make_interval(mins => $1::int)`,
        [STALE_PROCESSING_MIN],
      );
      if (rowCount)
        logger.warn("Recovered stale processing rows", {
          queue: key,
          rowCount,
        });
    } catch (err) {
      // one bad table must never block sending
      logger.error("Stale recovery failed", { queue: key, error: err.message });
    }
  }
};

let ticking = false;
const tick = async () => {
  if (ticking || shuttingDown) return;
  ticking = true;
  try {
    await recoverStaleRows();
    let job;
    while (!shuttingDown && (job = await claimDueJob())) {
      logger.info("Batch started", { jobId: job.id, queue: job.queue_key });
      try {
        await runBatch(job);
      } catch (err) {
        logger.error("Batch run crashed", {
          jobId: job.id,
          error: err.message,
        });
        await pool.query(
          `UPDATE email_batch_jobs
              SET locked_at=NULL, next_run_at = NOW() + make_interval(mins => pause_minutes)
            WHERE id=$1`,
          [job.id],
        );
      }
    }
  } catch (err) {
    logger.error("Scheduler tick failed", { error: err.message });
  } finally {
    ticking = false;
  }
};

// Called from startBatch so sending begins immediately instead of waiting up to 30s
export const triggerBatchTick = () => {
  setImmediate(tick);
};

export const startBatchScheduler = () => {
  tick(); // run immediately on boot (resumes any running jobs)
  setInterval(tick, TICK_MS);
  logger.info("Batch email scheduler started");
};
