import pool from "../../../db/pool.js";
import logger from "../../../services/logger.js";

export const getRates = async (req, res) => {
  try {
    const { rows } = await pool.query("SELECT * FROM rates");
    if (rows.length) {
      logger.info("Found rates with lenght: ", rows.length);
      return res.status(200).json({
        success: true,
        rates: rows,
      });
    }
    logger.warn("No rates found!");
    return res.status(200).json({
      success: true,
      message: "No Rates Found!",
    });
  } catch (error) {
    logger.error("Failed to get rates", error);
    return res.status(500).json({
      success: false,
      message: "Something went wrong!",
    });
  }
};
