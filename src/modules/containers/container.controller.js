import express from "express";
import pool from "../../db/pool.js";


export const addContainer = async (req, res) => {
  try {
    const {
      container_no,
      container_size,
      container_type,     // ✅ new
      ownership_type,     // ✅ new
      shipper,
      date_hired,
      date_reached,
      free_days,
      return_date,
      place_of_loading,
      place_of_delivery,
    } = req.body;

    // Validation
    if (
      !container_no ||
      !container_size ||
      !container_type ||    // validate new field
      !ownership_type ||    // validate new field
      !date_hired ||
      !free_days ||
      !place_of_loading ||
      !place_of_delivery
    ) {
      return res.status(400).json({ error: "Required fields are missing" });
    }

    const newContainer = await pool.query(
      `INSERT INTO containers 
      (container_no, container_size, container_type, ownership_type, shipper, date_hired, date_reached, free_days, return_date, place_of_loading, place_of_delivery) 
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING *`,
      [
        container_no,
        container_size,
        container_type,    // save new field
        ownership_type,    // save new field
        shipper,
        date_hired,
        date_reached,
        free_days,
        return_date,
        place_of_loading,
        place_of_delivery,
      ]
    );

    res.status(201).json(newContainer.rows[0]);
  } catch (error) {
    console.error("❌ Error in addContainer:", error.message);
    res.status(500).json({ error: "Server error while adding container" });
  }
};


// ✅ Get all containers
export const getContainers = async (req, res) => {
  try {
    const containers = await pool.query(
      "SELECT * FROM containers ORDER BY id DESC"
    );
    res.json(containers.rows);
  } catch (error) {
    console.error("❌ Error in getContainers:", error.message);
    res.status(500).json({ error: "Server error while fetching containers" });
  }
};

