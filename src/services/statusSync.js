import logger from "./logger.js";

const DELIVERED = "Shipment Delivered";
const LEVEL_COLUMN = {
  order: "order_status",
  container: "container_status",
  consignment: "consignment_status",
};
const ASSIGN_COLUMN = { consignment: "consignment_id", container: "cid" };

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
            CASE WHEN rc.n > 1 THEN i.item_tracking_id ELSE o.tracking_id END
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
            lt.status AS cur_status
       FROM slots sl
       JOIN order_items oi ON oi.id = sl.item_id
       LEFT JOIN LATERAL (${SLOT_LATEST}) lt ON TRUE
      FOR UPDATE OF oi`,
    [ids, cidList],
  );

  const toWrite = slotRes.rows
    .map((s) => ({
      ...s,
      cid: s.cid ?? (cidList?.length === 1 ? cidList[0] : null),
    }))
    .filter(
      (s) =>
        s.cur_status !== orderStatus &&
        !(protectDelivered && s.cur_status === DELIVERED),
    );

  if (toWrite.length) {
    await client.query(
      `INSERT INTO order_tracking
         (order_id, sender_id, sender_ref, receiver_id, receiver_ref, container_id,
          consignment_id, status, old_status, item_ref, eta, etd, created_by, created_time, module_id)
       SELECT oi.order_id, oi.sender_id, s.sender_ref, oi.receiver_id, r.receiver_ref,
              u.cid, u.consignment_id, $2::varchar, COALESCE(u.old_status, 'Created'), oi.item_ref,
              $3::timestamp, $3::timestamp, $4::varchar, NOW(), $5::int
         FROM unnest($1::int[], $6::int[], $7::int[], $8::varchar[])
              AS u(item_id, cid, consignment_id, old_status)
         JOIN order_items oi ON oi.id = u.item_id
         LEFT JOIN senders s ON s.id = oi.sender_id
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

  // item status = least advanced across ALL its containers (always recomputed)
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
  await client.query(`UPDATE container_master SET status = $1 WHERE cid = $2`, [
    statusRow.container_status,
    cid,
  ]);

  const assigned = (await getAssignedItems(client, "container", cid)).filter(
    (a) => a.status === currentStatus,
  );
  if (!assigned.length) return [];

  await client.query(
    `UPDATE container_assignment_history SET status = $1 WHERE id = ANY($2::int[])`,
    [statusRow.container_status, assigned.map((a) => a.id)],
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

  if (statusRow.container_status && assigned.length) {
    const cids = [...new Set(assigned.map((a) => a.cid).filter(Boolean))];
    if (cids.length) {
      await client.query(
        `UPDATE container_master SET status = $1 WHERE cid = ANY($2)`,
        [statusRow.container_status, cids],
      );
    }
    await client.query(
      `UPDATE container_assignment_history SET status = $1 WHERE id = ANY($2::int[])`,
      [statusRow.container_status, assigned.map((a) => a.id)],
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
