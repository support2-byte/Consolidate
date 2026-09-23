import pool from "../../db/pool.js";

export const getItemForStoragePurchase = async (req, res) => {
  try {
    const { itemRef } = req.params;

    const { rows: existing } = await pool.query(
      `SELECT 1 FROM storage_purchase WHERE shipment_ref = $1 LIMIT 1`,
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
         r.receiver_name
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
      },
    });
  } catch (error) {
    console.error("getItemForStoragePurchase error:", error);
    return res
      .status(500)
      .json({ success: false, message: "Unable to load item details." });
  }
};

export const createStoragePurchase = async (req, res) => {
  try {
    const {
      customerRef,
      shipmentRef,
      storage,
      sizeKey,
      sizeLabel,
      category,
      subcategory,
      requiredFrom,
      durationDays,
      notes,
      amount,
    } = req.body;

    if (
      !customerRef ||
      !shipmentRef ||
      storage === undefined ||
      storage === null ||
      !sizeKey ||
      !category ||
      !subcategory ||
      !requiredFrom ||
      durationDays === undefined ||
      durationDays === null ||
      amount === undefined ||
      amount === null
    ) {
      return res
        .status(400)
        .json({ success: false, message: "Missing required fields." });
    }

    if (
      !Number.isInteger(durationDays) ||
      durationDays <= 0 ||
      durationDays > 30
    ) {
      return res.status(400).json({
        success: false,
        message: "Duration must be a whole number of days between 1 and 30.",
      });
    }

    if (Number.isNaN(Date.parse(requiredFrom))) {
      return res
        .status(400)
        .json({ success: false, message: "Required from date is invalid." });
    }

    const { rows: itemRows } = await pool.query(
      `SELECT total_number FROM order_items WHERE item_ref = $1`,
      [shipmentRef],
    );

    if (!itemRows.length) {
      return res
        .status(404)
        .json({ success: false, message: "Shipment item not found." });
    }

    const totalNumber = itemRows[0].total_number;
    if (totalNumber !== null && storage > totalNumber) {
      return res.status(400).json({
        success: false,
        message: `Quantity to store (${storage}) can't exceed the item total (${totalNumber}).`,
      });
    }

    const sizeVale = sizeKey.split("_")[0];
    const sizeUnit = sizeKey.split("_")[1];

    const { rows } = await pool.query(
      `INSERT INTO storage_purchase
         (customer_ref, shipment_ref, storage, size_value, size_unit, category,
          subcategory, required_from, duration_days, notes, amount)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
       RETURNING id, status, created_at`,
      [
        customerRef,
        shipmentRef,
        storage,
        sizeVale,
        sizeUnit || null,
        category,
        subcategory,
        requiredFrom,
        durationDays,
        notes || null,
        amount,
      ],
    );

    return res.json({ success: true, storagePurchase: rows[0] });
  } catch (error) {
    console.error("createStoragePurchase error:", error);
    return res
      .status(500)
      .json({ success: false, message: "Unable to submit storage purchase." });
  }
};
