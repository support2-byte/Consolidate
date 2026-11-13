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
          oi.tracking_id AS item_tracking_id,
          o.tracking_id AS order_tracking_id,
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
          i.public_ref AS id,
          i.invoice_id,
          i.created_at,
          i.status,
          i.amount,
          i.invoice_type
        FROM (
          SELECT public_ref, invoice_id, created_at, status, amount, customer_ref, 'delivery' AS invoice_type FROM delivery_invoices
          UNION ALL
          SELECT public_ref, invoice_id, created_at, status, amount, customer_ref, 'storage' FROM storage_invoices
          UNION ALL
          SELECT public_ref, invoice_id, created_at, status, amount, customer_ref, 'dropoff' FROM dropoff_invoices
          UNION ALL
          SELECT public_ref, invoice_id, created_at, status, amount, customer_ref, 'overstay' FROM overstay_invoices
        ) i
        WHERE i.customer_ref = $1
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
      `SELECT key, label, value, category
       FROM system_settings
       WHERE category IN ('Rates', 'Storage Type', 'Storage Size')
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
      },
    });
  } catch (error) {
    logger.error("Failed to get receiver dashboard data", { error });

    return res.status(500).json({
      success: false,
      message: "Something went wrong.",
    });
  }
};
