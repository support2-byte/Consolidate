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
         SELECT DISTINCT ON (detail_id) id, detail_id, cid, consignment_id, status
           FROM container_assignment_history
          WHERE detail_id IN (
                  SELECT detail_id FROM container_assignment_history WHERE ${col} = $1
                )
          ORDER BY detail_id, created_at DESC, id DESC
       ) la
      WHERE la.${col} = $1`,
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

export async function rollupDelivered(client, itemIds) {
  const links = await client.query(
    `SELECT DISTINCT ON (detail_id) detail_id, cid, consignment_id
       FROM container_assignment_history
      WHERE detail_id = ANY($1::int[])
      ORDER BY detail_id, created_at DESC, id DESC`,
    [itemIds],
  );
  const consignmentIds = [
    ...new Set(links.rows.map((r) => r.consignment_id).filter(Boolean)),
  ];
  const cids = [...new Set(links.rows.map((r) => r.cid).filter(Boolean))];

  const count = async (assigned) => {
    const r = await client.query(
      `SELECT COUNT(*)::int AS total,
              COUNT(*) FILTER (WHERE status = $2)::int AS delivered
         FROM order_items WHERE id = ANY($1::int[])`,
      [assigned.map((a) => a.detail_id), DELIVERED],
    );
    return r.rows[0];
  };

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

export async function applyItemStatus(
  client,
  {
    itemIds,
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

  const upd = await client.query(
    `WITH target AS (
       SELECT id, status AS old_status
         FROM order_items
        WHERE id = ANY($1::int[])
          AND status IS DISTINCT FROM $2::varchar
          AND ($3::boolean = false OR status IS DISTINCT FROM $4::varchar)
        FOR UPDATE
     )
     UPDATE order_items oi
        SET status = $2::varchar, updated_at = NOW()
       FROM target t
      WHERE oi.id = t.id
  RETURNING oi.id, oi.order_id, oi.sender_id, oi.receiver_id, oi.item_ref, t.old_status`,
    [ids, orderStatus, protectDelivered, DELIVERED],
  );
  const rows = upd.rows;
  if (!rows.length) return [];

  await client.query(
    `INSERT INTO order_tracking
       (order_id, sender_id, sender_ref, receiver_id, receiver_ref, container_id,
        consignment_id, status, old_status, item_ref, eta, etd, created_by, created_time, module_id)
     SELECT oi.order_id, oi.sender_id, s.sender_ref, oi.receiver_id, r.receiver_ref,
            la.cid, la.consignment_id, $2::varchar, u.old_status, oi.item_ref,
            $3::timestamp, $3::timestamp, $4::varchar, NOW(), $5::int
       FROM unnest($1::int[], $6::varchar[]) AS u(id, old_status)
       JOIN order_items oi ON oi.id = u.id
       LEFT JOIN senders s ON s.id = oi.sender_id
       LEFT JOIN receivers r ON r.id = oi.receiver_id
       LEFT JOIN LATERAL (
         SELECT cid, consignment_id FROM container_assignment_history
          WHERE detail_id = oi.id ORDER BY created_at DESC, id DESC LIMIT 1
       ) la ON TRUE`,
    [
      rows.map((r) => r.id),
      orderStatus,
      eta,
      createdBy,
      moduleId,
      rows.map((r) => r.old_status),
    ],
  );

  if (statusRow.send_email === true) {
    await client.query(
      `INSERT INTO email_queue
         (order_id, recipient_id, recipient_type, recipient_email, recipient_name, email_type, status, item_ref)
       SELECT oi.order_id, r.id, 'receiver', r.receiver_email, r.receiver_name,
              'order_status_update', 'pending', oi.item_ref
         FROM order_items oi JOIN receivers r ON r.id = oi.receiver_id
        WHERE oi.id = ANY($1::int[]) AND COALESCE(r.receiver_email,'') <> ''
       UNION ALL
       SELECT oi.order_id, s.id, 'sender', s.sender_email, s.sender_name,
              'order_status_update', 'pending', oi.item_ref
         FROM order_items oi JOIN senders s ON s.id = oi.sender_id
        WHERE oi.id = ANY($1::int[]) AND COALESCE(s.sender_email,'') <> ''`,
      [rows.map((r) => r.id)],
    );
  }

  await syncParents(client, rows, receiverEta);

  if (orderStatus === DELIVERED) {
    await rollupDelivered(
      client,
      rows.map((r) => r.id),
    );
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
    updatedItems = await applyItemStatus(client, {
      itemIds: assigned.map((a) => a.detail_id),
      statusRow,
      eta,
      receiverEta: eta,
      createdBy,
      moduleId: 6,
    });
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
