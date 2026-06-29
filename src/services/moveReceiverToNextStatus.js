import { calculateETA } from "./calculateEta.js";
import { getNextStatusAfterCreated } from "./getNextStatusAfterCreated.js";

export async function moveReceiverToNextStatus(client, receiverId) {
  const receiverRes = await client.query(
    `
    SELECT status
      FROM receivers
    WHERE id = $1
    `,
    [receiverId],
  );

  if (!receiverRes.rowCount) return null;

  const currentStatus = receiverRes.rows[0].status;

  if (currentStatus !== "Created") {
    return null;
  }

  const nextStatus = await getNextStatusAfterCreated(client);

  if (!nextStatus) return null;

  const { eta } = await calculateETA(client, nextStatus.order_status);

  await client.query(
    `
    UPDATE receivers
    SET
      status = $1,
      eta = $2,
      updated_at = NOW()
    WHERE id = $3
  `,
    [nextStatus.order_status, eta, receiverId],
  );

  return nextStatus;
}
