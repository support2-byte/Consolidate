import pool from "../../../db/pool.js";
import logger from "../../../services/logger.js";

export const getShipments = async (req, res) => {
  const { id } = req.params;

  try {
    const { rows } = await pool.query(
      `
        SELECT
          o.id AS order_id,
          o.rgl_booking_number,
          oi.id AS order_item_id,
          oi.item_ref,
          oi.status,
          oi.total_number,
          oi.weight,
          ot.eta,
          pl.name AS loading_place_name,
          pd.name AS destination_place_name,
          o.created_at
        FROM receivers r
        JOIN order_items oi
          ON oi.receiver_id = r.id
        JOIN orders o
          ON o.id = oi.order_id

        LEFT JOIN places pl ON pl.id = o.place_of_loading
        LEFT JOIN places pd ON pd.id = o.final_destination::integer

        LEFT JOIN LATERAL (
          SELECT ot.eta
          FROM order_tracking ot
          WHERE ot.item_ref = oi.item_ref
          ORDER BY ot.created_time DESC
          LIMIT 1
        ) ot ON true

        WHERE r.receiver_ref = $1
        ORDER BY o.created_at DESC
      `,
      [id],
    );

    if (rows.length === 0) {
      logger.warn(`No shipments found for receiver_ref ${id}`);

      return res.status(404).json({
        success: false,
        message: "No shipments found.",
        data: [],
      });
    }

    logger.info(`Found ${rows.length} shipments for receiver_ref ${id}`);

    return res.status(200).json({
      success: true,
      data: rows,
    });
  } catch (error) {
    logger.error("Failed to get shipments", {
      receiver_ref: id,
      error: {
        message: error.message,
        code: error.code,
        detail: error.detail,
        hint: error.hint,
      },
    });

    return res.status(500).json({
      success: false,
      message: "Something went wrong.",
    });
  }
};

export const getShipment = async (req, res) => {
  const { id } = req.params;
  const { customer_id } = req.query;

  try {
    const { rows } = await pool.query(
      `
        SELECT
          o.id AS order_id,
          o.rgl_booking_number,
          o.created_at,
          pl.name AS loading_place_name,
          pd.name AS destination_place_name,

          r.receiver_name,
          r.receiver_contact,
          r.receiver_address,
          r.status AS receiver_status,
          rc.contact_name AS receiver_customer_name,
          rc.phone_number AS receiver_customer_phone,
          rc.address AS receiver_customer_address,

          oi.id AS order_item_id,
          oi.item_ref,
          oi.status,
          oi.category,
          oi.subcategory,
          oi.type,
          oi.total_number,
          oi.weight,
          oi.total_weight,
          oi.delivered_qty,
          oi.remaining_qty,

          s.sender_name,
          s.sender_contact,
          s.sender_address,
          sc.contact_name AS sender_customer_name,
          sc.phone_number AS sender_customer_phone,
          sc.address AS sender_customer_address,

          ot.eta,

          ad.id AS delivery_id,
          ad.status AS delivery_status,
          ad.delivery_amount,
          ad.delivery_address AS app_delivery_address,

          asp.id AS storage_purchase_id,
          asp.status AS storage_status,
          asp.storage,
          asp.days,
          asp.amount AS storage_amount

        FROM orders o
        JOIN receivers r
          ON r.order_id = o.id
         AND r.receiver_ref = $2
        JOIN order_items oi
          ON oi.order_id = o.id
        LEFT JOIN senders s
          ON s.id = oi.sender_id
        LEFT JOIN customers rc
          ON rc.zoho_id = r.receiver_ref
        LEFT JOIN customers sc
          ON sc.zoho_id = s.sender_ref

        LEFT JOIN places pl ON pl.id = o.place_of_loading
        LEFT JOIN places pd ON pd.id = o.final_destination::integer

        LEFT JOIN LATERAL (
          SELECT ot.eta
          FROM order_tracking ot
          WHERE ot.item_ref = oi.item_ref
          ORDER BY ot.created_time DESC
          LIMIT 1
        ) ot ON true

        LEFT JOIN LATERAL (
          SELECT ad.*
          FROM app_delivery ad
          WHERE ad.shipment = oi.item_ref
            AND ad.customer_id = $2
          ORDER BY ad.created_at DESC
          LIMIT 1
        ) ad ON true

        LEFT JOIN LATERAL (
          SELECT asp.*
          FROM storage_purchase asp
          WHERE asp.shipment = oi.item_ref
            AND asp.customer_id = $2
          ORDER BY asp.created_at DESC
          LIMIT 1
        ) asp ON true

        WHERE o.rgl_booking_number = $1

        ORDER BY oi.id ASC
      `,
      [id, customer_id],
    );

    logger.info("getShipment: query result", {
      rgl_booking_number: id,
      customer_id,
      rowCount: rows.length,
    });

    if (rows.length === 0) {
      logger.warn("getShipment: no shipment found", {
        rgl_booking_number: id,
        customer_id,
      });
      return res.status(404).json({
        success: false,
        message: "No shipments found.",
        data: [],
      });
    }

    const first = rows[0];

    const items = rows.map((row) => ({
      order_item_id: row.order_item_id,
      item_ref: row.item_ref,
      status: row.status,
      category: row.category,
      subcategory: row.subcategory,
      type: row.type,
      total_number: row.total_number,
      weight: row.weight,
      total_weight: row.total_weight,
      delivered_qty: row.delivered_qty,
      remaining_qty: row.remaining_qty,
      eta: row.eta,
      sender: row.sender_name
        ? {
            sender_name: row.sender_customer_name || row.sender_name,
            sender_contact: row.sender_customer_phone || row.sender_contact,
            sender_address: row.sender_customer_address || row.sender_address,
          }
        : null,
    }));

    const delivery = rows
      .filter((row) => row.delivery_id)
      .map((row) => ({
        id: row.delivery_id,
        item_ref: row.item_ref,
        status: row.delivery_status,
        delivery_amount: row.delivery_amount,
        delivery_address: row.app_delivery_address,
      }));

    const storage = rows
      .filter((row) => row.storage_purchase_id)
      .map((row) => ({
        id: row.storage_purchase_id,
        item_ref: row.item_ref,
        status: row.storage_status,
        storage: row.storage,
        days: row.days,
        amount: row.storage_amount,
      }));

    const data = {
      order_id: first.order_id,
      rgl_booking_number: first.rgl_booking_number,
      created_at: first.created_at,
      loading_place_name: first.loading_place_name,
      destination_place_name: first.destination_place_name,
      receiver: {
        receiver_name: first.receiver_customer_name || first.receiver_name,
        receiver_contact:
          first.receiver_customer_phone || first.receiver_contact,
        receiver_address:
          first.receiver_customer_address || first.receiver_address,
        status: first.receiver_status,
      },
      delivery,
      storage,
      items,
    };

    logger.info("getShipment: success", {
      rgl_booking_number: id,
      customer_id,
      itemCount: items.length,
      deliveryCount: delivery.length,
      storageCount: storage.length,
    });

    return res.status(200).json({
      success: true,
      data,
    });
  } catch (error) {
    logger.error("getShipment: query failed", {
      rgl_booking_number: id,
      customer_id,
      error: {
        message: error.message,
        code: error.code,
        detail: error.detail,
        hint: error.hint,
      },
    });

    return res.status(500).json({
      success: false,
      message: "Something went wrong.",
    });
  }
};

export const purchaseStorage = async (req, res) => {
  const { id } = req.params;
  const { days, totalCost, cartons, selectedShipment } = req.body;
  try {
    const shipmentValue = Array.isArray(selectedShipment)
      ? selectedShipment.join(", ")
      : selectedShipment;

    const { rows } = await pool.query(
      "INSERT INTO storage_purchase (customer_id, shipment, storage, days, amount) VALUES ($1, $2, $3, $4, $5) RETURNING id",
      [id, shipmentValue, cartons, days, totalCost],
    );

    if (rows.length) {
      logger.info("Storage Purchase has been created!", rows[0].id);
      return res.status(200).json({
        success: true,
        message: "Storage Purchase request has been sent!",
      });
    }
    logger.error("Couldn't create storage purchase!");
    return res.status(400).json({
      success: false,
      message: "Couldn't create storage purchase!",
    });
  } catch (error) {
    logger.error("Failed to purchase storage", {
      error: {
        message: error.message,
      },
    });

    return res.status(500).json({
      success: false,
      message: "Something went wrong.",
    });
  }
};

export const createDelivery = async (req, res) => {
  const { id } = req.params;
  const {
    name,
    contact_number,
    delivery_address,
    addons,
    totalAmount,
    selectedShipment,
  } = req.body;

  try {
    const deliveryOptionsJson = JSON.stringify(addons ?? []);

    const checkDelivery = await pool.query(
      "SELECT customer_id, shipment, status FROM app_delivery WHERE customer_id = $1 AND shipment = $2",
      [id, selectedShipment],
    );

    if (
      checkDelivery.rows.length > 0 &&
      checkDelivery.rows[0].status === "pending"
    ) {
      logger.info("Delivery already created for: ", selectedShipment);
      return res.status(200).json({
        success: true,
        message: "Delivery already created for this shipment!",
      });
    }

    const { rows } = await pool.query(
      "INSERT INTO app_delivery (customer_id, shipment, recevier_name, delivery_contact_number, delivery_address, delivery_options, delivery_amount) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id",
      [
        id,
        selectedShipment,
        name,
        contact_number,
        delivery_address,
        deliveryOptionsJson,
        totalAmount,
      ],
    );

    if (rows.length) {
      logger.info("Delivery has been created!", rows[0].id);
      return res.status(200).json({
        success: true,
        message: "Delivery request has been created!",
      });
    }
    logger.error("Couldn't create delivery request!");
    return res.status(400).json({
      success: false,
      message: "Couldn't create delivery request!",
    });
  } catch (error) {
    logger.error("Failed to created delivery request", {
      error: {
        message: error.message,
      },
    });

    return res.status(500).json({
      success: false,
      message: "Something went wrong.",
    });
  }
};

export const getGatepasses = async (req, res) => {
  const { id } = req.params;
  try {
    const { rows } = await pool.query(
      `SELECT
         oc.id AS collection_id,
         o.rgl_booking_number,
         oc.order_id,
         oc.receiver_id,
         oc.collection_method,
         oc.delivery_date,
         g.id AS gatepass_id,
         g.url,
         g.originalname,
         g.mimetype,
         g.uploaded_at,
         oi.item_ref,
         oi.category,
         oi.subcategory,
         oi.status,
         oi.total_number,
         oi.weight
       FROM receivers r
       JOIN order_collections oc ON oc.receiver_id = r.id
       JOIN order_collection_gatepass g ON g.collection_id = oc.id
       LEFT JOIN order_collection_items oci ON oci.collection_id = oc.id
       LEFT JOIN order_items oi ON oi.id = oci.order_item_id
       LEFT JOIN orders o ON o.id = oc.order_id
       WHERE r.receiver_ref = $1
       ORDER BY oc.delivery_date DESC, g.uploaded_at DESC`,
      [id],
    );

    return res.status(200).json({
      success: true,
      gatepass: rows,
    });
  } catch (error) {
    logger.error("Failed to fetch gatepass", {
      error: { message: error.message },
    });

    return res.status(500).json({
      success: false,
      message: "Something went wrong.",
    });
  }
};
