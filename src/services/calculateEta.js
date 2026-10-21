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
  const today = baseDate.toISOString().split("T")[0];
  if (!status) return { eta: today, daysUntil: 0 };

  try {
    const configResult = await client.query(
      `SELECT days_offset, order_status
         FROM statuses
        WHERE status = true AND LOWER(order_status) = LOWER($1)
        ORDER BY sorting_number ASC
        LIMIT 1`,
      [status],
    );

    if (configResult.rowCount === 0) {
      logger.warn("No ETA configuration found", { status });
      return { eta: null, daysUntil: null };
    }

    const { days_offset, order_status } = configResult.rows[0];

    if (order_status?.toLowerCase().includes("delivered")) {
      return { eta: today, daysUntil: 0 };
    }

    const days = Number(days_offset);
    if (days_offset === null || !Number.isFinite(days)) {
      logger.warn("days_offset missing for status", { status });
      return { eta: null, daysUntil: null };
    }

    const eta = new Date(baseDate.getTime() + days * 86400000)
      .toISOString()
      .split("T")[0];

    return { eta, daysUntil: computeDaysUntilEta(eta, baseDate) };
  } catch (err) {
    logger.error("Failed to calculate ETA", { status, error: err });
    return { eta: null, daysUntil: null };
  }
};
