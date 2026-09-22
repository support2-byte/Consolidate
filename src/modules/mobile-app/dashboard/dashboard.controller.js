import pool from "../../../db/pool.js";
import logger from "../../../services/logger.js";

export const getDashboardData = async (req, res) => {
  const { id } = req.params;

  try {
    const { rows } = await pool.query(
      `
      WITH recent_orders AS (
        SELECT
          o.rgl_booking_number,
          ot.eta,
          oi.status,
          oi.item_ref,
          pl.name AS loading_place_name,
          pd.name AS destination_place_name,
          o.created_at
        FROM receivers r

        JOIN order_items oi
          ON oi.item_ref = r.item_ref

        JOIN orders o
          ON o.id = oi.order_id

        LEFT JOIN LATERAL (
          SELECT
            ot.eta
          FROM order_tracking ot
          WHERE ot.item_ref = oi.item_ref
            AND ot.eta IS NOT NULL
          ORDER BY ot.created_time DESC
          LIMIT 1
        ) ot ON true

        LEFT JOIN places pl
          ON pl.id = o.place_of_loading

        LEFT JOIN places pd
          ON pd.id::text = o.final_destination

        WHERE r.receiver_ref = $1

        ORDER BY o.created_at DESC
        LIMIT 3
      ),

      recent_invoices AS (
        SELECT
          i.id,
          i.invoice_id AS invoice_id,
          i.created_at,
          i.status,
          i.amount
        FROM invoices i
        WHERE i.customer_id = $1
        ORDER BY i.created_at DESC
        LIMIT 3
      )

      SELECT
        COALESCE(
          (
            SELECT json_agg(recent_orders)
            FROM recent_orders
          ),
          '[]'::json
        ) AS recent_orders,

        COALESCE(
          (
            SELECT json_agg(recent_invoices)
            FROM recent_invoices
          ),
          '[]'::json
        ) AS recent_invoices;`,
      [id],
    );

    const data = rows[0];

    return res.status(200).json({
      success: true,
      data,
    });
  } catch (error) {
    logger.error("Failed to get receiver dashboard data", { error });

    return res.status(500).json({
      success: false,
      message: "Something went wrong.",
    });
  }
};

export const getRates = async (req, res) => {
  try {
    const { rows } = await pool.query(
      "SELECT delivery_rate, storage_rate FROM rates",
    );

    const rates = rows.map((row) => ({
      delivery_rate: Number(row.delivery_rate),
      storage_rate: Number(row.storage_rate),
    }));

    return res.status(200).json({
      success: true,
      rates: rates[0],
    });
  } catch (error) {
    logger.error("Failed to get receiver dashboard data", { error });

    return res.status(500).json({
      success: false,
      message: "Something went wrong.",
    });
  }
};
