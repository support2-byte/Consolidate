export async function createOrderTracking(
  client,
  { orderId, receiverId, containerId, status, createdBy, itemRef = null },
) {
  const orderRes = await client.query(
    `
      SELECT
        o.id,
        s.id AS sender_id,
        s.sender_ref,
        r.receiver_ref
      FROM orders o
      LEFT JOIN senders s
        ON s.order_id = o.id
      LEFT JOIN receivers r
        ON r.id = $2
      WHERE o.id = $1
      LIMIT 1
    `,
    [orderId, receiverId],
  );

  if (!orderRes.rowCount) return;

  const order = orderRes.rows[0];

  await client.query(
    `
    INSERT INTO order_tracking (
      order_id,
      sender_id,
      sender_ref,
      receiver_id,
      receiver_ref,
      container_id,
      status,
      old_status,
      created_by,
      item_ref
    )
    VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10
    )
  `,
    [
      orderId,
      order.sender_id,
      order.sender_ref,
      receiverId,
      order.receiver_ref,
      containerId,
      status,
      "Created",
      createdBy,
      itemRef,
    ],
  );
}
