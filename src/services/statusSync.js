import logger from "./logger.js";

const DELIVERED = "Shipment Delivered";
const LEVEL_COLUMN = {
  order: "order_status",
  container: "container_status",
  consignment: "consignment_status",
};
const ASSIGN_COLUMN = { consignment: "consignment_id", container: "cid" };

export const ETA_MAX_DAYS = 30;
const LOCKED_STATUSES = ["Delivered", "Shipment Delivered", "Cancelled"];

export const isStatusLocked = (status) =>
  LOCKED_STATUSES.includes(String(status ?? "").trim());

export function httpError(status, code, message) {
  const err = new Error(message);
  err.status = status;
  err.code = code;
  return err;
}

const rankSql = (expr) => `COALESCE((
  SELECT s.sorting_number FROM statuses s
   WHERE s.status = true
     AND LOWER(${expr}::text) IN (
       LOWER(s.order_status), LOWER(s.container_status), LOWER(s.consignment_status)
     )
   ORDER BY s.sorting_number ASC LIMIT 1
), 0)`;

export async function getStatusRank(db, value) {
  const v = String(value ?? "").trim();
  if (!v) return 0;
  const r = await db.query(`SELECT ${rankSql("$1")} AS rank`, [v]);
  return Number(r.rows[0]?.rank) || 0;
}

export async function assertNotBackward(db, { entity, current, target }) {
  const [cur, next] = await Promise.all([
    getStatusRank(db, current),
    getStatusRank(db, target),
  ]);
  if (cur > 0 && next > 0 && next < cur) {
    throw httpError(
      409,
      "STATUS_BACKWARD",
      `Cannot move ${entity} back from "${current}" to "${target}".`,
    );
  }
}

export async function getEtaWindow(db, currentEta) {
  const r = await db.query(
    `SELECT GREATEST(CURRENT_DATE, $1::date)::text AS min,
            (GREATEST(CURRENT_DATE, $1::date) + ${ETA_MAX_DAYS})::text AS max`,
    [currentEta || null],
  );
  return r.rows[0];
}

export async function assertEtaInWindow(db, currentEta, nextEta) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(nextEta ?? ""))) {
    throw httpError(400, "ETA_INVALID", "ETA must be in YYYY-MM-DD format.");
  }
  const { min, max } = await getEtaWindow(db, currentEta);
  if (nextEta < min || nextEta > max) {
    throw httpError(
      400,
      "ETA_OUT_OF_RANGE",
      `ETA must be between ${min} and ${max}. It can only move forward, up to ${ETA_MAX_DAYS} days.`,
    );
  }
  return { min, max };
}

export async function getActorLabel(db, req) {
  if (req.user?.email) return req.user.email;
  if (req.user?.id) {
    try {
      const r = await db.query(`SELECT email FROM users WHERE id = $1`, [
        req.user.id,
      ]);
      if (r.rows[0]?.email) return r.rows[0].email;
    } catch (err) {
      logger.warn("[statusSync] actor lookup failed:", err.message);
    }
  }
  return req.user?.username || "system";
}

export async function getStatusRow(db, level, value) {
  const col = LEVEL_COLUMN[level];
  const v = (value || "").trim();
  if (!col || !v) return null;
  const r = await db.query(
    `SELECT id, order_status, container_status, consignment_status,
            days_offset, sorting_number, send_email
       FROM statuses
      WHERE status = true AND LOWER(${col}) = LOWER($1)
      ORDER BY sorting_number ASC
      LIMIT 1`,
    [v],
  );
  return r.rows[0] ?? null;
}

export async function getAssignedItems(db, level, value) {
  const col = ASSIGN_COLUMN[level];
  if (!col || value == null) return [];
  const r = await db.query(
    `SELECT la.id, la.detail_id, la.cid, la.consignment_id, la.status
       FROM (
         SELECT DISTINCT ON (detail_id, cid, consignment_id)
                id, detail_id, cid, consignment_id, status, action_type
           FROM container_assignment_history
          WHERE ${col} = $1
          ORDER BY detail_id, cid, consignment_id, id DESC
       ) la
      WHERE la.action_type = 'ASSIGN'`,
    [value],
  );
  return r.rows;
}

async function logConsignmentChange(
  client,
  { consignmentId, oldStatus, newStatus, details },
) {
  await client.query(
    `INSERT INTO consignment_tracking
       (consignment_id, event_type, old_status, new_status, timestamp, details, created_at, source, action)
     VALUES ($1, 'status_updated', COALESCE($2::varchar,'Unknown'), $3, NOW(), $4::jsonb, NOW(), 'api', 'status_changed')`,
    [consignmentId, oldStatus, newStatus, JSON.stringify(details)],
  );
}

async function syncParents(client, rows, receiverEta) {
  const receiverIds = [
    ...new Set(rows.map((r) => r.receiver_id).filter(Boolean)),
  ];
  const orderIds = [...new Set(rows.map((r) => r.order_id).filter(Boolean))];

  if (receiverIds.length) {
    await client.query(
      `UPDATE receivers r
          SET status = agg.status,
              eta = COALESCE($2::timestamp, r.eta),
              updated_at = CURRENT_TIMESTAMP
         FROM (
           SELECT DISTINCT ON (oi.receiver_id) oi.receiver_id, oi.status
             FROM order_items oi
             JOIN statuses s ON s.order_status = oi.status AND s.status = true
            WHERE oi.receiver_id = ANY($1::int[])
            ORDER BY oi.receiver_id, s.sorting_number ASC
         ) agg
        WHERE r.id = agg.receiver_id
          AND (r.status IS DISTINCT FROM agg.status OR $2::timestamp IS NOT NULL)`,
      [receiverIds, receiverEta],
    );
  }

  if (orderIds.length) {
    await client.query(
      `UPDATE orders o
          SET status = agg.status, updated_at = CURRENT_TIMESTAMP
         FROM (
           SELECT DISTINCT ON (oi.order_id) oi.order_id, oi.status
             FROM order_items oi
             JOIN statuses s ON s.order_status = oi.status AND s.status = true
            WHERE oi.order_id = ANY($1::int[])
            ORDER BY oi.order_id, s.sorting_number ASC
         ) agg
        WHERE o.id = agg.order_id AND o.status IS DISTINCT FROM agg.status`,
      [orderIds],
    );
    await client.query(
      `UPDATE orders o
          SET eta = m.min_eta, updated_at = CURRENT_TIMESTAMP
         FROM (SELECT order_id, MIN(eta) AS min_eta FROM receivers
                WHERE order_id = ANY($1::int[]) GROUP BY order_id) m
        WHERE o.id = m.order_id AND m.min_eta IS NOT NULL
          AND o.eta IS DISTINCT FROM m.min_eta`,
      [orderIds],
    );
  }
}

// ETA-only parent sync: receivers/orders ETA never move backwards
async function syncParentEta(client, rows, eta) {
  const receiverIds = [
    ...new Set(rows.map((r) => r.receiver_id).filter(Boolean)),
  ];
  const orderIds = [...new Set(rows.map((r) => r.order_id).filter(Boolean))];

  if (receiverIds.length) {
    await client.query(
      `UPDATE receivers
          SET eta = $2::timestamp, updated_at = CURRENT_TIMESTAMP
        WHERE id = ANY($1::int[])
          AND (eta IS NULL OR eta < $2::timestamp)`,
      [receiverIds, eta],
    );
  }
  if (orderIds.length) {
    await client.query(
      `UPDATE orders o
          SET eta = m.min_eta, updated_at = CURRENT_TIMESTAMP
         FROM (SELECT order_id, MIN(eta) AS min_eta FROM receivers
                WHERE order_id = ANY($1::int[]) GROUP BY order_id) m
        WHERE o.id = m.order_id AND m.min_eta IS NOT NULL
          AND o.eta IS DISTINCT FROM m.min_eta`,
      [orderIds],
    );
  }
}

export async function countDeliveredSlots(db, assigned) {
  const r = await db.query(
    `SELECT COUNT(*)::int AS total,
            COUNT(*) FILTER (WHERE lt.status = $3)::int AS delivered
       FROM unnest($1::int[], $2::int[]) AS u(detail_id, cid)
       JOIN order_items oi ON oi.id = u.detail_id
       LEFT JOIN LATERAL (
         SELECT t.status FROM order_tracking t
          WHERE t.order_id = oi.order_id AND t.item_ref = oi.item_ref
            AND (t.container_id = u.cid OR t.container_id IS NULL)
          ORDER BY (t.container_id IS NOT NULL) DESC, t.created_time DESC, t.id DESC
          LIMIT 1
       ) lt ON TRUE`,
    [assigned.map((a) => a.detail_id), assigned.map((a) => a.cid), DELIVERED],
  );
  return r.rows[0];
}

export async function rollupDelivered(client, itemIds) {
  const links = await client.query(
    `SELECT DISTINCT ON (h.detail_id, h.cid, h.consignment_id)
            h.cid, h.consignment_id, h.action_type
       FROM container_assignment_history h
      WHERE h.detail_id = ANY($1::int[])
      ORDER BY h.detail_id, h.cid, h.consignment_id, h.id DESC`,
    [itemIds],
  );
  const active = links.rows.filter((r) => r.action_type === "ASSIGN");
  const consignmentIds = [
    ...new Set(active.map((r) => r.consignment_id).filter(Boolean)),
  ];
  const cids = [...new Set(active.map((r) => r.cid).filter(Boolean))];

  const count = (assigned) => countDeliveredSlots(client, assigned);

  for (const consId of consignmentIds) {
    const assigned = await getAssignedItems(client, "consignment", consId);
    if (!assigned.length) continue;
    const { total, delivered } = await count(assigned);
    const next =
      total > 0 && delivered === total
        ? "Delivered"
        : delivered > 0
          ? "Partially Delivered"
          : null;
    if (!next) continue;

    const upd = await client.query(
      `UPDATE consignments c
        SET status = $2::consignment_status,
            eta = CASE WHEN $2::consignment_status = 'Delivered' THEN CURRENT_DATE ELSE c.eta END,
            updated_at = NOW()
        FROM (SELECT id, status FROM consignments WHERE id = $1 FOR UPDATE) old
        WHERE c.id = old.id AND old.status IS DISTINCT FROM $2::consignment_status
        RETURNING old.status AS old_status`,
      [consId, next],
    );
    if (upd.rowCount) {
      await logConsignmentChange(client, {
        consignmentId: consId,
        oldStatus: upd.rows[0].old_status,
        newStatus: next,
        details: {
          reason: "Item delivery rollup",
          delivered,
          total,
          user: "system",
        },
      });
    }
  }

  for (const cid of cids) {
    const assigned = await getAssignedItems(client, "container", cid);
    if (!assigned.length) continue;
    const { total, delivered } = await count(assigned);
    if (total > 0 && delivered === total) {
      await client.query(
        `UPDATE container_assignment_history
            SET status = $2
          WHERE id = ANY($1::int[]) AND status IS DISTINCT FROM $2`,
        [assigned.map((a) => a.id), DELIVERED],
      );
    }
  }
}

export async function queueStatusEmails(
  db,
  { itemIds, emailType = "order_status_update" },
) {
  const ids = [...new Set((itemIds || []).filter(Boolean))];
  if (!ids.length) return 0;

  const r = await db.query(
    `WITH items AS (
       SELECT oi.id, oi.order_id, oi.sender_id, oi.receiver_id,
              oi.tracking_id AS item_tracking_id
         FROM order_items oi WHERE oi.id = ANY($1::int[])
     ),
     rc AS (
       SELECT order_id, COUNT(*)::int AS n FROM receivers
        WHERE order_id IN (SELECT order_id FROM items) GROUP BY order_id
     )
     INSERT INTO email_queue
       (order_id, recipient_id, recipient_type, recipient_email, recipient_name,
        email_type, status, tracking_id)
     SELECT DISTINCT i.order_id, s.id, 'sender', s.sender_email, s.sender_name,
            $2::varchar, 'pending', o.tracking_id
       FROM items i
       JOIN orders o  ON o.id = i.order_id
       JOIN senders s ON s.id = i.sender_id
      WHERE COALESCE(s.sender_email,'') <> '' AND s.email_opt_in IS NOT FALSE
     UNION ALL
     SELECT DISTINCT i.order_id, r.id, 'receiver', r.receiver_email, r.receiver_name,
            $2::varchar, 'pending',
            CASE WHEN rc.n > 1 THEN COALESCE(r.tracking_id, i.item_tracking_id) ELSE o.tracking_id END
       FROM items i
       JOIN orders o    ON o.id = i.order_id
       JOIN receivers r ON r.id = i.receiver_id
       JOIN rc          ON rc.order_id = i.order_id
      WHERE COALESCE(r.receiver_email,'') <> '' AND r.email_opt_in IS NOT FALSE`,
    [ids, emailType],
  );
  return r.rowCount;
}

const SLOTS_CTE = `
  cur AS (
    SELECT DISTINCT ON (h.detail_id, h.cid, h.consignment_id)
           h.detail_id, h.cid, h.consignment_id, h.action_type
      FROM container_assignment_history h
     WHERE h.detail_id = ANY($1::int[])
     ORDER BY h.detail_id, h.cid, h.consignment_id, h.id DESC
  ),
  slots AS (
    SELECT oi.id AS item_id, a.cid, a.consignment_id
      FROM order_items oi
      LEFT JOIN cur a
        ON a.detail_id = oi.id AND a.action_type = 'ASSIGN'
       AND ($2::int[] IS NULL OR a.cid = ANY($2::int[]))
     WHERE oi.id = ANY($1::int[])
  )`;

const SLOT_LATEST = `
  SELECT t.status
    FROM order_tracking t
   WHERE t.order_id = oi.order_id
     AND t.item_ref = oi.item_ref
     AND (t.container_id = sl.cid OR t.container_id IS NULL)
   ORDER BY (t.container_id IS NOT NULL) DESC, t.created_time DESC, t.id DESC
   LIMIT 1`;

export async function applyItemStatus(
  client,
  {
    itemIds,
    containerId = null,
    containerIds = null,
    statusRow,
    eta = null,
    receiverEta = null,
    createdBy = "system",
    moduleId,
    protectDelivered = true,
    // "skip": silently leave items that are already further along (cascades)
    // "throw": reject the request (explicit user action on a single item)
    onBackward = "skip",
  },
) {
  const orderStatus = statusRow?.order_status;
  const ids = [...new Set((itemIds || []).filter(Boolean))];
  if (!orderStatus || !ids.length) return [];

  const cidList = containerIds?.length
    ? containerIds.map(Number)
    : containerId
      ? [Number(containerId)]
      : null;

  const slotRes = await client.query(
    `WITH ${SLOTS_CTE}
     SELECT sl.item_id, sl.cid, sl.consignment_id, oi.order_id, oi.receiver_id,
            oi.item_ref, lt.status AS cur_status
       FROM slots sl
       JOIN order_items oi ON oi.id = sl.item_id
       LEFT JOIN LATERAL (${SLOT_LATEST}) lt ON TRUE
      FOR UPDATE OF oi`,
    [ids, cidList],
  );

  const rankRes = await client.query(
    `SELECT order_status, sorting_number FROM statuses
      WHERE status = true AND order_status IS NOT NULL`,
  );
  const rankMap = new Map(
    rankRes.rows.map((r) => [r.order_status.toLowerCase(), r.sorting_number]),
  );
  const rankOf = (v) => rankMap.get(String(v ?? "").toLowerCase()) ?? 0;
  const targetRank = rankOf(orderStatus);
  const isBackward = (s) =>
    s.cur_status !== orderStatus &&
    targetRank > 0 &&
    rankOf(s.cur_status) > targetRank;

  if (onBackward === "throw") {
    const bad = slotRes.rows.find(isBackward);
    if (bad) {
      throw httpError(
        409,
        "STATUS_BACKWARD",
        `Cannot move item back from "${bad.cur_status}" to "${orderStatus}".`,
      );
    }
  }

  const toWrite = slotRes.rows
    .map((s) => ({
      ...s,
      cid: s.cid ?? (cidList?.length === 1 ? cidList[0] : null),
    }))
    .filter(
      (s) =>
        s.cur_status !== orderStatus &&
        !isBackward(s) &&
        !(protectDelivered && s.cur_status === DELIVERED),
    );

  const insertRows = [
    ...new Map(
      toWrite.map((s) => [`${s.order_id}|${s.item_ref}|${s.cid ?? ""}`, s]),
    ).values(),
  ];

  if (insertRows.length) {
    await client.query(
      `INSERT INTO order_tracking
         (order_id, sender_id, sender_ref, receiver_id, receiver_ref, container_id,
          consignment_id, status, old_status, item_ref, eta, etd, created_by, created_time, module_id)
       SELECT oi.order_id, s.id, s.sender_ref, oi.receiver_id, r.receiver_ref,
              u.cid, u.consignment_id, $2::varchar, COALESCE(u.old_status, 'Created'), oi.item_ref,
              $3::timestamp, $3::timestamp, $4::varchar, NOW(), $5::int
         FROM unnest($1::int[], $6::int[], $7::int[], $8::varchar[])
              AS u(item_id, cid, consignment_id, old_status)
         JOIN order_items oi ON oi.id = u.item_id
         LEFT JOIN senders s ON s.id = COALESCE(
           oi.sender_id,
           (SELECT MIN(id) FROM senders WHERE order_id = oi.order_id)
         )
         LEFT JOIN receivers r ON r.id = oi.receiver_id`,
      [
        toWrite.map((s) => s.item_id),
        orderStatus,
        eta,
        createdBy,
        moduleId,
        toWrite.map((s) => s.cid),
        toWrite.map((s) => s.consignment_id),
        toWrite.map((s) => s.cur_status),
      ],
    );
  }

  const changed = await client.query(
    `WITH ${SLOTS_CTE},
     agg AS (
       SELECT DISTINCT ON (sl.item_id) sl.item_id, lt.status
         FROM slots sl
         JOIN order_items oi ON oi.id = sl.item_id
         JOIN LATERAL (${SLOT_LATEST}) lt ON TRUE
         JOIN statuses st ON st.order_status = lt.status AND st.status = true
        ORDER BY sl.item_id, st.sorting_number ASC
     )
     UPDATE order_items oi
        SET status = agg.status, updated_at = NOW()
       FROM agg
      WHERE oi.id = agg.item_id AND oi.status IS DISTINCT FROM agg.status
  RETURNING oi.id, oi.order_id, oi.receiver_id`,
    [ids, null],
  );

  const byId = new Map();
  toWrite.forEach((s) =>
    byId.set(s.item_id, {
      id: s.item_id,
      order_id: s.order_id,
      receiver_id: s.receiver_id,
    }),
  );
  changed.rows.forEach((r) => byId.set(r.id, r));
  const rows = [...byId.values()];
  if (!rows.length) return [];

  const touched = rows.map((r) => r.id);

  if (statusRow.send_email === true) {
    await queueStatusEmails(client, { itemIds: touched });
  }

  await syncParents(client, rows, receiverEta);

  if (orderStatus === DELIVERED) {
    await rollupDelivered(client, touched);
  }
  return rows;
}

export async function applyContainerStatus(
  client,
  { cid, currentStatus, statusRow, eta, createdBy },
) {
  const dbRes = await client.query(
    `SELECT status::text AS status FROM container_master WHERE cid = $1 FOR UPDATE`,
    [cid],
  );
  const dbStatus = dbRes.rows[0]?.status || "";
  const [dbRank, uiRank, nextRank] = await Promise.all([
    getStatusRank(client, dbStatus),
    getStatusRank(client, currentStatus),
    getStatusRank(client, statusRow.container_status),
  ]);
  if (nextRank > 0 && nextRank < Math.max(dbRank, uiRank)) {
    throw httpError(
      409,
      "STATUS_BACKWARD",
      `Cannot move container back from "${dbRank >= uiRank ? dbStatus : currentStatus}" to "${statusRow.container_status}".`,
    );
  }

  await client.query(
    `UPDATE container_master
        SET status = $1::text,
            eta = CASE WHEN LOWER(status::text) IS DISTINCT FROM LOWER($1::text)
                       THEN COALESCE($3::date, eta) ELSE eta END
      WHERE cid = $2`,
    [statusRow.container_status, cid, eta || null],
  );

  const assigned = await getAssignedItems(client, "container", cid);
  if (!assigned.length) return [];

  await client.query(
    `UPDATE container_assignment_history h
        SET status = $1::text
      WHERE h.id = ANY($2::int[])
        AND ($3::int = 0 OR ${rankSql("h.status")} < $3::int)`,
    [statusRow.container_status, assigned.map((a) => a.id), nextRank],
  );

  if (!statusRow.order_status) {
    logger.warn(
      `[statusSync] container status "${statusRow.container_status}" has no linked order status; items not changed`,
    );
    return [];
  }
  return applyItemStatus(client, {
    itemIds: assigned.map((a) => a.detail_id),
    containerId: cid,
    statusRow,
    eta,
    receiverEta: eta,
    createdBy,
    moduleId: 4,
  });
}

export async function applyConsignmentStatus(
  client,
  {
    consignmentId,
    statusRow,
    previousStatus,
    eta,
    createdBy,
    userId,
    reason,
    daysOffset = null,
  },
) {
  await client.query(
    `UPDATE consignments SET status = $1, eta = $2, updated_at = NOW() WHERE id = $3`,
    [statusRow.consignment_status, eta, consignmentId],
  );

  const assigned = await getAssignedItems(client, "consignment", consignmentId);

  const cids = [...new Set(assigned.map((a) => a.cid).filter(Boolean))];
  const targetRank =
    Number(statusRow.sorting_number) ||
    (await getStatusRank(client, statusRow.consignment_status));

  if (statusRow.container_status && assigned.length) {
    if (cids.length) {
      await client.query(
        `UPDATE container_master cm
            SET status = $1, eta = COALESCE($4::date, cm.eta)
          WHERE cm.cid = ANY($2::int[]) AND ${rankSql("cm.status")} < $3`,
        [statusRow.container_status, cids, targetRank, eta || null],
      );
    }
    await client.query(
      `UPDATE container_assignment_history h
          SET status = $1
        WHERE h.id = ANY($2::int[]) AND ${rankSql("h.status")} < $3`,
      [statusRow.container_status, assigned.map((a) => a.id), targetRank],
    );
  } else if (eta && cids.length) {
    await client.query(
      `UPDATE container_master SET eta = $2::date WHERE cid = ANY($1::int[])`,
      [cids, eta],
    );
  }

  let updatedItems = [];
  if (statusRow.order_status && assigned.length) {
    const byCid = new Map();
    for (const a of assigned) {
      if (!byCid.has(a.cid)) byCid.set(a.cid, []);
      byCid.get(a.cid).push(a.detail_id);
    }
    for (const [cid, detailIds] of byCid) {
      const rows = await applyItemStatus(client, {
        itemIds: detailIds,
        containerId: cid,
        statusRow,
        eta,
        receiverEta: eta,
        createdBy,
        moduleId: 6,
      });
      updatedItems.push(...rows);
    }
  } else if (!statusRow.order_status) {
    logger.warn(
      `[statusSync] consignment status "${statusRow.consignment_status}" has no linked order status; items not changed`,
    );
  }

  await logConsignmentChange(client, {
    consignmentId,
    oldStatus: previousStatus,
    newStatus: statusRow.consignment_status,
    details: { reason, newEta: eta, daysOffset, user: userId || "system" },
  });

  return updatedItems;
}

export async function applyItemEta(client, { itemIds, cids = null, eta }) {
  const ids = [...new Set((itemIds || []).filter(Boolean))];
  if (!ids.length || !eta) return [];
  const cidList = cids?.length ? cids.map(Number) : null;

  const LATEST = `WITH ${SLOTS_CTE},
    latest AS (
      SELECT DISTINCT lt.id, lt.container_id, sl.cid
        FROM slots sl
        JOIN order_items oi ON oi.id = sl.item_id
        JOIN LATERAL (
          SELECT t.id, t.container_id
            FROM order_tracking t
           WHERE t.order_id = oi.order_id AND t.item_ref = oi.item_ref
             AND (t.container_id = sl.cid OR t.container_id IS NULL)
           ORDER BY (t.container_id IS NOT NULL) DESC, t.created_time DESC, t.id DESC
           LIMIT 1
        ) lt ON TRUE
       WHERE sl.cid IS NOT NULL
    )`;

  await client.query(
    `${LATEST}
     INSERT INTO order_tracking
     SELECT (jsonb_populate_record(
       null::order_tracking,
       to_jsonb(t) || jsonb_build_object(
         'id', nextval(pg_get_serial_sequence('order_tracking','id')),
         'container_id', l.cid,
         'eta', $3::date::text,
         'created_time', now()
       )
     )).*
       FROM latest l
       JOIN order_tracking t ON t.id = l.id
      WHERE l.container_id IS NULL
        AND (t.eta IS NULL OR t.eta::date < $3::date)`,
    [ids, cidList, eta],
  );

  await client.query(
    `${LATEST}
     UPDATE order_tracking ot
        SET eta = $3::date
       FROM latest l
      WHERE ot.id = l.id AND l.container_id IS NOT NULL
        AND (ot.eta IS NULL OR ot.eta::date < $3::date)`,
    [ids, cidList, eta],
  );

  const items = await client.query(
    `SELECT id, order_id, receiver_id FROM order_items WHERE id = ANY($1::int[])`,
    [ids],
  );
  await syncParentEta(client, items.rows, eta);
  return items.rows;
}

export async function applyContainerEta(client, { cid, eta }) {
  const cur = await client.query(
    `SELECT eta::date::text AS eta FROM container_master WHERE cid = $1 FOR UPDATE`,
    [cid],
  );
  if (!cur.rowCount) throw httpError(404, "NOT_FOUND", "Container not found");

  await assertEtaInWindow(client, cur.rows[0].eta, eta);

  await client.query(
    `UPDATE container_master SET eta = $1::date WHERE cid = $2`,
    [eta, cid],
  );
  const assigned = await getAssignedItems(client, "container", cid);
  await applyItemEta(client, {
    itemIds: assigned.map((a) => a.detail_id),
    cids: [cid],
    eta,
  });
  return { eta, affectedItems: new Set(assigned.map((a) => a.detail_id)).size };
}

export async function applyConsignmentEta(
  client,
  { consignmentId, eta, skipGuards = false },
) {
  const cur = await client.query(
    `SELECT status::text AS status, eta::date::text AS eta
       FROM consignments WHERE id = $1 FOR UPDATE`,
    [consignmentId],
  );
  if (!cur.rowCount) {
    throw httpError(404, "NOT_FOUND", "Consignment not found");
  }
  const { status, eta: currentEta } = cur.rows[0];
  if (!skipGuards) {
    if (isStatusLocked(status)) {
      throw httpError(
        409,
        "STATUS_LOCKED",
        `This consignment is ${status}, so its ETA can no longer be changed.`,
      );
    }
    await assertEtaInWindow(client, currentEta, eta);

    await client.query(
      `UPDATE consignments SET eta = $1::date, updated_at = NOW() WHERE id = $2`,
      [eta, consignmentId],
    );
  }

  const assigned = await getAssignedItems(client, "consignment", consignmentId);
  const cids = [...new Set(assigned.map((a) => a.cid).filter(Boolean))];
  if (cids.length) {
    await client.query(
      `UPDATE container_master SET eta = $2::date
        WHERE cid = ANY($1::int[]) AND (eta IS NULL OR eta::date < $2::date)`,
      [cids, eta],
    );
  }
  await applyItemEta(client, {
    itemIds: assigned.map((a) => a.detail_id),
    cids,
    eta,
  });
  return {
    eta,
    affectedContainers: cids.length,
    affectedItems: new Set(assigned.map((a) => a.detail_id)).size,
  };
}
