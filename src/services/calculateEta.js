export function computeDaysUntilEta(etaDateStr, today = new Date()) {
  if (!etaDateStr) return null;
  const etaDate = new Date(etaDateStr);
  if (isNaN(etaDate.getTime())) return null;
  const diffTime = etaDate - today;
  const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
  return diffDays;
}

export const calculateETA = async (client, status, baseDate = new Date()) => {
  try {
    const configResult = await client.query(
      `SELECT days_offset FROM statuses WHERE order_status = $1 AND status = true LIMIT 1`,
      [status],
    );
    if (configResult.rowCount === 0) {
      console.log(
        `No ETA config for status: ${status}; using baseDate (0 days)`,
      );
      return { eta: baseDate.toISOString().split("T")[0], daysUntil: 0 };
    }
    const days = configResult.rows[0].days_offset;
    if (status.toLowerCase().includes("delivered")) {
      return { eta: baseDate.toISOString().split("T")[0], daysUntil: 0 };
    }
    const etaDate = new Date(baseDate.getTime() + days * 86400000);
    const eta = etaDate.toISOString().split("T")[0];
    const daysUntil = computeDaysUntilEta(eta, baseDate);
    console.log(
      `[calculateETA] For status "${status}": offset=${days} days → ETA=${eta} (days until: ${daysUntil})`,
    );
    return { eta, daysUntil };
  } catch (err) {
    console.error("ETA calc error:", err);
    return { eta: new Date().toISOString().split("T")[0], daysUntil: 0 };
  }
};
