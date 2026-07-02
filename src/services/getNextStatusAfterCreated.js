export async function getNextStatusAfterCreated(client) {
  const currentStatus = await client.query(
    `
    SELECT sorting_number
    FROM statuses
    WHERE order_status = 'Created'
    LIMIT 1
  `,
  );

  if (!currentStatus.rowCount) {
    return null;
  }

  const nextStatus = await client.query(
    `
    SELECT order_status, days_offset
    FROM statuses
    WHERE sorting_number >
      (
        SELECT sorting_number
        FROM statuses
        WHERE order_status = 'Created'
        LIMIT 1
      )
      AND status = true
    ORDER BY sorting_number
    LIMIT 1
  `,
  );

  return nextStatus.rows[0] || null;
}
