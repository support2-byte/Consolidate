import pool from "../../db/pool.js";

export const getItemForDropOffRequest = async (req, res) => {
  try {
    const { itemRef } = req.params;

    const { rows: existing } = await pool.query(
      `SELECT 1 FROM drop_off_requests WHERE shipment_ref = $1 LIMIT 1`,
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
        oi.sender_id,
        s.sender_ref,
        s.sender_name,
        s.sender_contact,
        s.sender_address
       FROM order_items oi
       LEFT JOIN senders s ON s.id = oi.sender_id
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
        senderRef: row.sender_ref,
        senderName: row.sender_name,
        senderContact: row.sender_contact,
        senderAddress: row.sender_address,
      },
    });
  } catch (error) {
    console.error("getItemForDropOffRequest error:", error);
    return res
      .status(500)
      .json({ success: false, message: "Unable to load item details." });
  }
};

export const createDropOffRequest = async (req, res) => {
  try {
    const {
      customerRef,
      customerName,
      shipmentRef,
      pickupAddress,
      contactNumber,
      pickupAmount,
      pickupDate,
      zone,
    } = req.body;

    if (
      !customerRef ||
      !customerName ||
      !shipmentRef ||
      !pickupAddress ||
      !contactNumber ||
      !pickupDate ||
      pickupAmount === undefined ||
      pickupAmount === null ||
      !zone
    ) {
      return res
        .status(400)
        .json({ success: false, message: "Missing required fields." });
    }

    const { rows } = await pool.query(
      `INSERT INTO drop_off_requests
        (customer_ref, shipment_ref, pickup_address, customer_name, contact_number, pickup_amount, pickup_date, zone, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())
       RETURNING id, status, created_at`,
      [
        customerRef,
        shipmentRef,
        pickupAddress,
        customerName,
        contactNumber,
        pickupAmount,
        pickupDate,
        zone,
      ],
    );

    return res.json({
      success: true,
      message: "Drop-off request submitted successfully.",
    });
  } catch (error) {
    console.error("createDropOffRequest error:", error);
    return res
      .status(500)
      .json({ success: false, message: "Unable to submit drop-off request." });
  }
};
