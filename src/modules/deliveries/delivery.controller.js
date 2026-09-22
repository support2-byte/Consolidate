import pool from "../../db/pool.js";

export const getItemForDeliveryRequest = async (req, res) => {
  try {
    const { itemRef } = req.params;

    const { rows: existing } = await pool.query(
      `SELECT 1 FROM delivery_requests WHERE shipment_ref = $1 LIMIT 1`,
      [itemRef],
    );

    if (existing.length) {
      return res.json({ success: true, alreadySubmitted: true });
    }

    const { rows } = await pool.query(
      `SELECT
         oi.item_ref,
         oi.category,
         oi.subcategory,
         oi.total_number,
         oi.weight,
         oi.receiver_id,
         r.receiver_ref,
         r.receiver_name,
         r.receiver_contact
       FROM order_items oi
       LEFT JOIN receivers r ON r.id = oi.receiver_id
       WHERE oi.item_ref = $1`,
      [itemRef],
    );

    if (!rows.length) {
      return res
        .status(404)
        .json({ success: false, message: "Item not found." });
    }

    const row = rows[0];

    return res.json({
      success: true,
      item: {
        itemRef: row.item_ref,
        category: row.category,
        subcategory: row.subcategory,
        totalNumber: row.total_number,
        weight: row.weight !== null ? Number(row.weight) : null,
        receiverRef: row.receiver_ref,
        receiverName: row.receiver_name,
        receiverContact: row.receiver_contact,
      },
    });
  } catch (error) {
    console.error("getItemForDeliveryRequest error:", error);
    return res
      .status(500)
      .json({ success: false, message: "Unable to load item details." });
  }
};

export const createDeliveryRequest = async (req, res) => {
  try {
    const {
      customerId,
      recevierName,
      deliveryContact,
      deliveryAddress,
      deliveryOptions,
      deliveryAmount,
      shipmentRef,
    } = req.body;

    if (
      !customerId ||
      !recevierName ||
      !deliveryContact ||
      !deliveryAddress ||
      !deliveryOptions ||
      deliveryAmount === undefined ||
      deliveryAmount === null ||
      !shipmentRef
    ) {
      return res
        .status(400)
        .json({ success: false, message: "Missing required fields." });
    }

    const { rows } = await pool.query(
      `INSERT INTO delivery_requests
         (customer_ref, recevier_name, delivery_contact_number, delivery_address,
          delivery_options, delivery_amount, shipment_ref)
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       RETURNING id, status, created_at`,
      [
        customerId,
        recevierName,
        deliveryContact,
        deliveryAddress,
        JSON.stringify(deliveryOptions),
        deliveryAmount,
        shipmentRef,
      ],
    );

    return res.json({ success: true, deliveryRequest: rows[0] });
  } catch (error) {
    console.error("createDeliveryRequest error:", error);
    return res
      .status(500)
      .json({ success: false, message: "Unable to submit delivery request." });
  }
};
