import logger from "./logger.js";

export function computeDaysUntilEta(etaDateStr, today = new Date()) {
  if (!etaDateStr) return null;
  const etaDate = new Date(etaDateStr);
  if (isNaN(etaDate.getTime())) return null;
  const diffTime = etaDate - today;
  const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
  return diffDays;
}

export const calculateETA = async (client, status, baseDate = new Date()) => {
  if (!status) {
    return { eta: baseDate.toISOString().split("T")[0], daysUntil: 0 };
  }

  try {
    const configResult = await client.query(
      `SELECT days_offset, order_status, container_status, consignment_status
       FROM statuses
       WHERE status = true
         AND (
           order_status = $1
           OR container_status = $1
           OR consignment_status = $1
         )
       ORDER BY sorting_number ASC
       LIMIT 1`,
      [status],
    );

    if (configResult.rowCount === 0) {
      logger.warn("No ETA configuration found", {
        status,
      });
      return { eta: baseDate.toISOString().split("T")[0], daysUntil: 0 };
    }

    const { days_offset: days, order_status } = configResult.rows[0];

    if (order_status?.toLowerCase().includes("delivered")) {
      return { eta: baseDate.toISOString().split("T")[0], daysUntil: 0 };
    }

    const etaDate = new Date(baseDate.getTime() + days * 86400000);
    const eta = etaDate.toISOString().split("T")[0];
    const daysUntil = computeDaysUntilEta(eta, baseDate);

    logger.debug("ETA calculated", {
      status,
      orderStatus: order_status,
      offsetDays: days,
      eta,
    });

    return { eta, daysUntil };
  } catch (err) {
    logger.error("Failed to calculate ETA", {
      status,
      error: err,
    });
    return { eta: new Date().toISOString().split("T")[0], daysUntil: 0 };
  }
};
