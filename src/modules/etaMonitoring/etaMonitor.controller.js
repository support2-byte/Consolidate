import pool from "../../db/pool.js";
import logger from "../../services/logger.js";
import {
  runEtaMonitor,
  MAX_GRACE,
  IDLE_SQL,
  CHECK_AFTER_SQL,
} from "./etaMonitor.service.js";

const actor = (req) => req.user?.email || req.user?.name || "admin";
const paging = (req) => {
  const page = Math.max(1, Number(req.query.page) || 1);
  const limit = Math.min(100, Math.max(1, Number(req.query.limit) || 25));
  return { page, limit, offset: (page - 1) * limit };
};
const fail = (res, err, msg) => {
  logger.error(msg, { error: err.message });
  return res
    .status(500)
    .json({ success: false, message: "Something went wrong!" });
};

const buildFilter = (req, statusCol, searchCols) => {
  const params = [];
  const conds = [];
  const search = (req.query.search || "").trim();
  if (search) {
    params.push(`%${search}%`);
    conds.push(
      `(${searchCols.map((c) => `${c} ILIKE $${params.length}`).join(" OR ")})`,
    );
  }
  const status = (req.query.status || "").trim();
  if (status) {
    params.push(status);
    conds.push(`${statusCol} = $${params.length}`);
  }
  return { params, where: conds.length ? `WHERE ${conds.join(" AND ")}` : "" };
};

export const getStatusOptions = async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT x.s AS status
      FROM (
        SELECT status_snapshot AS s FROM eta_monitor
        UNION
        SELECT status FROM eta_monitor_log WHERE status IS NOT NULL
      ) x
      LEFT JOIN statuses st ON st.order_status = x.s
      ORDER BY COALESCE(st.sorting_number, 9999), x.s`);
    return res.json({ success: true, statuses: rows.map((r) => r.status) });
  } catch (err) {
    return fail(res, err, "eta status options");
  }
};

export const runNow = async (req, res) => {
  try {
    return res.json({ success: true, summary: await runEtaMonitor() });
  } catch (err) {
    return fail(res, err, "eta run now");
  }
};

export const getNotifications = async (req, res) => {
  try {
    const resolved = req.query.state === "resolved";
    const { rows } = await pool.query(`
      SELECT n.id, n.status, n.idle_days, n.state, n.grace_days_added, n.resolved_by,
             n.resolved_at, n.created_at, oi.item_ref, r.receiver_name,
             o.booking_ref, o.rgl_booking_number, m.grace_used,
             eta_latest(n.order_item_id) AS eta,
             COALESCE(st.grace_days, 0) AS grace_days
      FROM eta_notifications n
      JOIN order_items oi ON oi.id = n.order_item_id
      JOIN receivers r ON r.id = oi.receiver_id
      JOIN orders o ON o.id = r.order_id
      LEFT JOIN eta_monitor m ON m.order_item_id = n.order_item_id
      LEFT JOIN statuses st ON st.order_status = n.status
      WHERE n.state ${resolved ? "<>" : "="} 'pending'
        ${resolved ? "" : `AND COALESCE(m.grace_used, 0) < ${MAX_GRACE}`}
      ORDER BY ${resolved ? "n.resolved_at DESC NULLS LAST, n.id DESC" : "n.created_at DESC, n.id DESC"}
      LIMIT 200`);
    return res.json({ success: true, rows });
  } catch (err) {
    return fail(res, err, "eta notifications get");
  }
};

const lockPending = async (client, id) => {
  const { rows } = await client.query(
    `SELECT n.*, oi.receiver_id, oi.item_ref, COALESCE(oi.status, 'Created') AS current_status
     FROM eta_notifications n JOIN order_items oi ON oi.id = n.order_item_id
     WHERE n.id = $1 FOR UPDATE OF n`,
    [id],
  );
  const n = rows[0];
  if (!n || n.state !== "pending")
    return { error: "This notification is no longer pending." };
  if (n.current_status !== n.status)
    return {
      error: "The order status has changed since this notification was raised.",
    };
  const m = (
    await client.query(
      `SELECT * FROM eta_monitor WHERE order_item_id = $1 FOR UPDATE`,
      [n.order_item_id],
    )
  ).rows[0];
  return { n, m };
};

export const addGraceDays = async (req, res) => {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const { n, m, error } = await lockPending(client, req.params.id);
    if (error) {
      await client.query("ROLLBACK");
      return res.status(409).json({ success: false, message: error });
    }
    if (m.grace_used >= MAX_GRACE) {
      await client.query("ROLLBACK");
      return res.status(409).json({
        success: false,
        message: `Grace limit (${MAX_GRACE}) reached. This order will be marked Delayed automatically.`,
      });
    }

    const cfg = await client.query(
      `SELECT COALESCE(grace_days, 0) AS grace_days FROM statuses WHERE order_status = $1 LIMIT 1`,
      [n.status],
    );
    const days = Number(cfg.rows[0]?.grace_days || 0);
    if (days < 1) {
      await client.query("ROLLBACK");
      return res.status(400).json({
        success: false,
        message: `No grace days configured for "${n.status}" in ETA Setup.`,
      });
    }

    const eta = await client.query(
      `UPDATE order_tracking SET eta = eta::date + $1::int
       WHERE id = (SELECT id FROM order_tracking WHERE receiver_id = $2 AND item_ref = $3
                   ORDER BY id DESC LIMIT 1)
       RETURNING eta`,
      [days, n.receiver_id, n.item_ref],
    );
    if (!eta.rowCount || !eta.rows[0].eta) {
      await client.query("ROLLBACK");
      return res
        .status(400)
        .json({ success: false, message: "No ETA found for this item." });
    }

    const used = m.grace_used + 1;
    await client.query(
      `UPDATE eta_monitor SET grace_used = $2, last_grace_at = now() WHERE order_item_id = $1`,
      [n.order_item_id, used],
    );
    await client.query(
      `UPDATE eta_notifications SET state = 'grace_added', grace_days_added = $2,
       resolved_by = $3, resolved_at = now() WHERE id = $1`,
      [n.id, days, actor(req)],
    );
    await client.query(
      `INSERT INTO eta_monitor_log (order_item_id, action, status, grace_used, eta, note, created_by)
       VALUES ($1, 'grace_added', $2, $3, $4, $5, $6)`,
      [
        n.order_item_id,
        n.status,
        used,
        eta.rows[0].eta,
        `Admin added ${days} grace day(s). Grace ${used}/${MAX_GRACE}.`,
        actor(req),
      ],
    );

    await client.query("COMMIT");
    return res.json({ success: true, message: `Added ${days} grace day(s).` });
  } catch (err) {
    await client.query("ROLLBACK");
    return fail(res, err, "eta add grace");
  } finally {
    client.release();
  }
};

export const markDelayed = async (req, res) => {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const { n, m, error } = await lockPending(client, req.params.id);
    if (m.grace_used >= MAX_GRACE) {
      await client.query("ROLLBACK");
      return res.status(409).json({
        success: false,
        message: `Grace limit (${MAX_GRACE}) reached. This order will be marked Delayed automatically.`,
      });
    }

    await client.query(
      `UPDATE eta_monitor SET is_delayed = true, delayed_at = now(), delayed_reason = 'admin'
       WHERE order_item_id = $1`,
      [n.order_item_id],
    );
    await client.query(
      `UPDATE eta_notifications SET state = 'delayed', resolved_by = $2, resolved_at = now() WHERE id = $1`,
      [n.id, actor(req)],
    );
    await client.query(
      `INSERT INTO eta_monitor_log (order_item_id, action, status, grace_used, eta, note, created_by)
       VALUES ($1, 'delayed', $2, $3, eta_latest($1), 'Admin marked Delayed. ETA not changed.', $4)`,
      [n.order_item_id, n.status, m.grace_used, actor(req)],
    );

    await client.query("COMMIT");
    return res.json({ success: true, message: "Marked as Delayed." });
  } catch (err) {
    await client.query("ROLLBACK");
    return fail(res, err, "eta mark delayed");
  } finally {
    client.release();
  }
};

export const getMonitorItems = async (req, res) => {
  try {
    const { page, limit, offset } = paging(req);
    const { params, where } = buildFilter(req, "m.status_snapshot", [
      "o.rgl_booking_number",
      "o.booking_ref",
      "oi.item_ref",
      "r.receiver_name",
    ]);
    const base = `FROM eta_monitor m
      JOIN order_items oi ON oi.id = m.order_item_id
      JOIN receivers r ON r.id = oi.receiver_id
      JOIN orders o ON o.id = r.order_id
      ${where}`;

    const [rows, count] = await Promise.all([
      pool.query(
        `SELECT o.rgl_booking_number, o.booking_ref, oi.item_ref, r.receiver_name,
                m.status_snapshot AS status, (CURRENT_DATE - m.status_since::date) AS days_in_status,
                ${IDLE_SQL} AS idle_days, m.grace_used, m.is_delayed, m.delayed_reason,
                ${CHECK_AFTER_SQL} AS check_after_days,
                eta_latest(m.order_item_id) AS eta
         ${base}
         ORDER BY GREATEST(m.status_since, COALESCE(m.last_grace_at, m.status_since)) DESC, m.order_item_id DESC
         LIMIT $${params.length + 1} OFFSET $${params.length + 2}`,
        [...params, limit, offset],
      ),
      pool.query(`SELECT COUNT(*) ${base}`, params),
    ]);
    return res.json({
      success: true,
      rows: rows.rows,
      total: Number(count.rows[0].count),
    });
  } catch (err) {
    return fail(res, err, "eta monitor items");
  }
};

export const getMonitorLog = async (req, res) => {
  try {
    const { page, limit, offset } = paging(req);
    const { params, where } = buildFilter(req, "l.status", [
      "o.rgl_booking_number",
      "o.booking_ref",
      "oi.item_ref",
    ]);
    const base = `FROM eta_monitor_log l
      JOIN order_items oi ON oi.id = l.order_item_id
      JOIN receivers r ON r.id = oi.receiver_id
      JOIN orders o ON o.id = r.order_id ${where}`;

    const [rows, count] = await Promise.all([
      pool.query(
        `SELECT l.id, o.rgl_booking_number, o.booking_ref, oi.item_ref, l.status, l.action,
                l.created_at, l.created_by, l.eta, l.note
         ${base} ORDER BY l.created_at DESC, l.id DESC
         LIMIT $${params.length + 1} OFFSET $${params.length + 2}`,
        [...params, limit, offset],
      ),
      pool.query(`SELECT COUNT(*) ${base}`, params),
    ]);
    return res.json({
      success: true,
      rows: rows.rows,
      total: Number(count.rows[0].count),
    });
  } catch (err) {
    return fail(res, err, "eta monitor log");
  }
};
