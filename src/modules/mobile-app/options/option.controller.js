import pool from "../../../db/pool.js";
import logger from "../../../services/logger.js";

export const getRates = async (req, res) => {
  try {
    const { rows } = await pool.query(
      `SELECT key, label, value, category
       FROM system_settings
      WHERE category IN ('Rates', 'Storage Type', 'Storage Size', 'Pickup Zone')
       ORDER BY sort_order, id`,
    );

    const rate = (key) =>
      Number(
        rows.find((r) => r.category === "Rates" && r.key === key)?.value ?? 0,
      );

    const options = (category) =>
      rows
        .filter((r) => r.category === category)
        .map((r) => ({ key: r.key, label: r.label, value: Number(r.value) }));

    return res.status(200).json({
      success: true,
      rates: {
        delivery_rate: rate("delivery_rate"),
        storage_rate: rate("storage_rate"),
        tax: rate("tax"),
        storage_types: options("Storage Type"),
        storage_sizes: options("Storage Size"),
        pickup_zones: options("Pickup Zone"),
      },
    });
  } catch (error) {
    logger.error("Failed to get rates", error);
    return res.status(500).json({
      success: false,
      message: "Something went wrong!",
    });
  }
};
