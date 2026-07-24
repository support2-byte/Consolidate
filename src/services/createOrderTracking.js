export async function createOrderTracking(
  client,
  {
    orderId,
    receiverId,
    containerId,
    status,
    createdBy,
    itemRef = null,
    eta = null,
    etd = null,
    consignmentId = null,
    moduleId = null,
    updatedBy = null,
  },
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

  let resolvedConsignmentId = consignmentId;
  if (resolvedConsignmentId == null && containerId) {
    const cahRes = await client.query(
      `
      SELECT consignment_id
      FROM container_assignment_history
      WHERE cid = $1
        AND order_id = $2
        AND receiver_id = $3
      ORDER BY created_at DESC
      LIMIT 1
      `,
      [containerId, orderId, receiverId],
    );
    resolvedConsignmentId = cahRes.rows[0]?.consignment_id ?? null;
  }

  await client.query(
    `
    INSERT INTO order_tracking (
      order_id,
      sender_id,
      sender_ref,
      receiver_id,
      receiver_ref,
      container_id,
      consignment_id,
      status,
      old_status,
      created_by,
      item_ref,
      eta,
      etd,
      module_id
    )
    VALUES (
      $1, $2, $3, $4, $5, $6, $7,
      $8, $9, $10, $11, $12, $13, $14
    )
  `,
    [
      orderId,
      order.sender_id,
      order.sender_ref,
      receiverId,
      order.receiver_ref,
      containerId,
      resolvedConsignmentId,
      status,
      "Created",
      createdBy,
      itemRef,
      eta,
      etd,
      moduleId,
    ],
  );
}
