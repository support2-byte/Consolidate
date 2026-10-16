import pool from "../../db/pool.js";

export const MAX_GRACE = 2;
export const FINAL_STATUSES = ["Shipment Delivered", "Rejected", "Cancelled"];

export const IDLE_SQL = `(CURRENT_DATE - GREATEST(m.status_since, COALESCE(m.last_grace_at, m.status_since))::date)`;

export const CHECK_AFTER_SQL = `CEIL(st.days_offset / 2.0)::int`;

export const runEtaMonitor = async () => {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    await client.query(`
      INSERT INTO eta_monitor (order_item_id, status_snapshot)
      SELECT oi.id, COALESCE(oi.status, 'Created') FROM order_items oi
      ON CONFLICT (order_item_id) DO NOTHING`);

    const reset = await client.query(`
      WITH changed AS (
        UPDATE eta_monitor m
        SET status_snapshot = COALESCE(oi.status, 'Created'),
            status_since = now(), grace_used = 0, last_grace_at = NULL,
            is_delayed = false, delayed_at = NULL, delayed_reason = NULL
        FROM order_items oi
        WHERE oi.id = m.order_item_id
          AND m.status_snapshot IS DISTINCT FROM COALESCE(oi.status, 'Created')
        RETURNING m.order_item_id, m.status_snapshot AS status
      ), closed AS (
        UPDATE eta_notifications n SET state = 'superseded', resolved_at = now()
        FROM changed c WHERE n.order_item_id = c.order_item_id AND n.state = 'pending'
        RETURNING n.id
      )
      INSERT INTO eta_monitor_log (order_item_id, action, status, grace_used, eta, note)
      SELECT order_item_id, 'status_changed', status, 0, eta_latest(order_item_id),
             'Status changed. Grace counter reset.'
      FROM changed`);

    const auto = await client.query(
      `
      WITH due AS (
        SELECT m.order_item_id, m.status_snapshot AS status
        FROM eta_monitor m
        JOIN statuses st ON st.order_status = m.status_snapshot
        WHERE m.is_delayed = false
          AND m.grace_used >= $1::int
          AND m.status_snapshot <> ALL($2::text[])
          AND st.days_offset > 0
          AND ${IDLE_SQL} >= ${CHECK_AFTER_SQL}
      ), upd AS (
        UPDATE eta_monitor m SET is_delayed = true, delayed_at = now(), delayed_reason = 'auto'
        FROM due WHERE m.order_item_id = due.order_item_id RETURNING 1
      ), closed AS (
        UPDATE eta_notifications n SET state = 'auto_delayed', resolved_at = now()
        FROM due WHERE n.order_item_id = due.order_item_id AND n.state = 'pending' RETURNING 1
      )
      INSERT INTO eta_monitor_log (order_item_id, action, status, grace_used, eta, note)
      SELECT order_item_id, 'auto_delayed', status, $1::int, eta_latest(order_item_id),
             'No status change after ' || $1::int || ' grace extensions. Auto-marked Delayed.'
      FROM due`,
      [MAX_GRACE, FINAL_STATUSES],
    );

    const notified = await client.query(
      `
      WITH due AS (
        SELECT m.order_item_id, m.status_snapshot AS status,
               ${IDLE_SQL} AS idle_days, ${CHECK_AFTER_SQL} AS check_after
        FROM eta_monitor m
        JOIN statuses st ON st.order_status = m.status_snapshot
        WHERE m.is_delayed = false
          AND m.grace_used < $1::int
          AND m.status_snapshot <> ALL($2::text[])
          AND st.days_offset > 0
          AND ${IDLE_SQL} >= ${CHECK_AFTER_SQL}
      ), ins AS (
        INSERT INTO eta_notifications (order_item_id, status, idle_days)
        SELECT order_item_id, status, idle_days FROM due
        ON CONFLICT (order_item_id) WHERE state = 'pending' DO NOTHING
        RETURNING order_item_id, status, idle_days
      )
      INSERT INTO eta_monitor_log (order_item_id, action, status, grace_used, eta, note)
      SELECT ins.order_item_id, 'admin_notified', ins.status,
             (SELECT grace_used FROM eta_monitor WHERE order_item_id = ins.order_item_id),
             eta_latest(ins.order_item_id),
             'Idle ' || ins.idle_days || ' days in "' || ins.status || '" (check point: '
               || due.check_after || ' days). Admin notified.'
      FROM ins JOIN due ON due.order_item_id = ins.order_item_id`,
      [MAX_GRACE, FINAL_STATUSES],
    );

    await client.query("COMMIT");
    return {
      statusChanges: reset.rowCount,
      autoDelayed: auto.rowCount,
      notified: notified.rowCount,
    };
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
};
