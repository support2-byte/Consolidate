import pool from "../../db/pool.js";
import { logErrorAndNotify } from "../../services/errorMail.js";
import logger from "../../services/logger.js";
import { sendBugReportEmail } from "../../services/sendBugReportEmail.js";

const formatOptions = (rows, valueField = "id", labelField = "name") => {
  return rows.map((row) => ({
    value: row[valueField],
    label: row[labelField],
  }));
};

export async function getThirdParties(req, res) {
  try {
    const result = await pool.query(
      "SELECT * FROM third_parties ORDER BY company_name ASC",
    );
    if (result.rowCount === 0) {
      return res.status(404).json({
        success: false,
        message: "No Third Parties Found!",
      });
    }
    return res.json({ success: true, third_parties: result.rows });
  } catch (err) {
    logger.error("Error fetching third parties:", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
}

export async function createThirdParty(req, res) {
  try {
    const {
      company_name,
      contact_name,
      contact_email,
      contact_phone,
      address,
      type,
    } = req.body;
    const result = await pool.query(
      "INSERT INTO third_parties (company_name, contact_name, contact_email, contact_phone, address, type) VALUES ($1, $2, $3, $4, $5, $6) RETURNING *",
      [company_name, contact_name, contact_email, contact_phone, address, type],
    );
    if (result.rowCount === 0) {
      return res.status(400).json({
        success: false,
        message: "Failed to create third party!",
      });
    }
    return res.status(201).json({ success: true, third_party: result.rows[0] });
  } catch (err) {
    logger.error("Error creating third party:", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
}

export async function updateThirdParty(req, res) {
  try {
    const { id } = req.params;
    const {
      company_name,
      contact_name,
      contact_email,
      contact_phone,
      address,
      type,
    } = req.body;
    const result = await pool.query(
      "UPDATE third_parties SET company_name = $1, contact_name = $2, contact_email = $3, contact_phone = $4, address = $5, type = $6 WHERE id = $7 RETURNING *",
      [
        company_name,
        contact_name,
        contact_email,
        contact_phone,
        address,
        type,
        id,
      ],
    );
    if (result.rowCount === 0) {
      return res
        .status(404)
        .json({ success: false, message: "Third Party Not Found!" });
    }
    return res.json({ success: true, third_party: result.rows[0] });
  } catch (err) {
    logger.error("Error updating third party:", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
}

export async function deleteThirdParty(req, res) {
  try {
    const { id } = req.params;
    const result = await pool.query(
      "DELETE FROM third_parties WHERE id = $1 RETURNING *",
      [id],
    );
    if (result.rowCount === 0) {
      return res
        .status(404)
        .json({ success: false, message: "Third Party Not Found!" });
    }
    return res.json({
      success: true,
      message: "Third party deleted successfully",
      third_party: result.rows[0],
    });
  } catch (err) {
    logger.error("Error deleting third party:", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
}

// --- PLACES ---

export async function getPlaces(req, res) {
  try {
    const result = await pool.query(
      "SELECT id, name, is_loading, is_destination, country, latitude, longitude FROM places ORDER BY id",
    );
    if (result.rowCount === 0) {
      return res.status(404).json({
        success: false,
        message: "No Places Found!",
      });
    }
    return res.json({ success: true, places: result.rows });
  } catch (err) {
    logger.error("Error fetching places:", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
}

export async function createPlace(req, res) {
  try {
    const { name, is_loading, is_destination, country, latitude, longitude } =
      req.body;
    if (!name || !country) {
      return res
        .status(400)
        .json({ success: false, message: "Name and country are required" });
    }
    const result = await pool.query(
      "INSERT INTO places (name, is_loading, is_destination, country, latitude, longitude) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id, name, is_loading, is_destination, country, latitude, longitude",
      [name, is_loading, is_destination, country, latitude, longitude],
    );
    if (result.rowCount === 0) {
      return res.status(400).json({
        success: false,
        message: "Failed to create place!",
      });
    }
    return res.status(201).json({ success: true, place: result.rows[0] });
  } catch (err) {
    logger.error("Error creating place:", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
}

export async function updatePlace(req, res) {
  try {
    const { id } = req.params;
    const { name, is_loading, is_destination, country, latitude, longitude } =
      req.body;
    if (!name || !country) {
      return res
        .status(400)
        .json({ success: false, message: "Name and country are required" });
    }
    const result = await pool.query(
      "UPDATE places SET name = $1, is_loading = $2, is_destination = $3, country = $4, latitude = $5, longitude = $6 WHERE id = $7 RETURNING id, name, is_loading, is_destination, country, latitude, longitude",
      [name, is_loading, is_destination, country, latitude, longitude, id],
    );
    if (result.rowCount === 0) {
      return res
        .status(404)
        .json({ success: false, message: "Place Not Found!" });
    }
    return res.json({ success: true, place: result.rows[0] });
  } catch (err) {
    logger.error("Error updating place:", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
}

export async function deletePlace(req, res) {
  try {
    const { id } = req.params;
    const result = await pool.query("DELETE FROM places WHERE id = $1", [id]);
    if (result.rowCount === 0) {
      return res
        .status(404)
        .json({ success: false, message: "Place Not Found!" });
    }
    return res.json({ success: true, message: "Place deleted successfully" });
  } catch (err) {
    logger.error("Error deleting place:", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
}

// --- BANKS ---

export async function getBanks(req, res) {
  try {
    const result = await pool.query("SELECT * FROM banks ORDER BY name ASC");
    if (result.rowCount === 0) {
      return res.status(404).json({
        success: false,
        message: "No Banks Found!",
      });
    }
    return res.json({ success: true, banks: result.rows });
  } catch (err) {
    logger.error("Error fetching banks:", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
}

export async function createBank(req, res) {
  try {
    const { name, account_number, swift_code, branch, address, currency } =
      req.body;
    const result = await pool.query(
      "INSERT INTO banks (name, account_number, swift_code, branch, address, currency) VALUES ($1, $2, $3, $4, $5, $6) RETURNING *",
      [name, account_number, swift_code, branch, address, currency || "USD"],
    );
    if (result.rowCount === 0) {
      return res.status(400).json({
        success: false,
        message: "Failed to create bank!",
      });
    }
    return res.status(201).json({ success: true, bank: result.rows[0] });
  } catch (err) {
    logger.error("Error creating bank:", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
}

export async function updateBank(req, res) {
  try {
    const { id } = req.params;
    const { name, account_number, swift_code, branch, address, currency } =
      req.body;
    const result = await pool.query(
      "UPDATE banks SET name = $1, account_number = $2, swift_code = $3, branch = $4, address = $5, currency = $6 WHERE id = $7 RETURNING *",
      [name, account_number, swift_code, branch, address, currency, id],
    );
    if (result.rowCount === 0) {
      return res
        .status(404)
        .json({ success: false, message: "Bank Not Found!" });
    }
    return res.json({ success: true, bank: result.rows[0] });
  } catch (err) {
    logger.error("Error updating bank:", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
}

export async function deleteBank(req, res) {
  try {
    const { id } = req.params;
    const result = await pool.query(
      "DELETE FROM banks WHERE id = $1 RETURNING *",
      [id],
    );
    if (result.rowCount === 0) {
      return res
        .status(404)
        .json({ success: false, message: "Bank Not Found!" });
    }
    return res.json({
      success: true,
      message: "Bank deleted successfully",
      bank: result.rows[0],
    });
  } catch (err) {
    logger.error("Error deleting bank:", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
}

export async function getPaymentTypes(req, res) {
  try {
    const paymentTypes = await pool.query(
      "SELECT * FROM payment_types ORDER BY id",
    );
    if (paymentTypes.rowCount === 0) {
      return res.status(404).json({
        success: false,
        message: "No Payment Types Found!",
      });
    }
    const options = formatOptions(paymentTypes.rows, "value", "value");
    return res.json({ success: true, paymentTypes: options });
  } catch (err) {
    logger.error("Error fetching payment type:", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
}

export async function createPaymentType(req, res) {
  try {
    const { name, type, percent = 0, days = 0 } = req.body;
    if (!name || !type) {
      return res
        .status(400)
        .json({ success: false, message: "Name and type are required" });
    }
    const payment = await pool.query(
      "INSERT INTO payment_types (name, type, percent, days) VALUES ($1, $2, $3, $4) RETURNING id, name, type, percent, days",
      [name, type, percent, days],
    );

    if (payment.rows.length === 0) {
      return res.status(400).json({
        success: false,
        message: "Could not create a new payment type!",
      });
    }
    return res.status(201).json({
      success: true,
      message: "New Payment type create successfully!",
      paymentType: payment.rows[0],
    });
  } catch (err) {
    logger.error("Error creating payment type:", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
}

export async function updatePaymentType(req, res) {
  try {
    const { id } = req.params;
    const { name, type, percent = 0, days = 0 } = req.body;
    if (!name || !type) {
      return res
        .status(400)
        .json({ success: false, message: "Name and type are required" });
    }

    const payment = await pool.query(
      "UPDATE payment_types SET name = $1, type = $2, percent = $3, days = $4 WHERE id = $5 RETURNING id, name, type, percent, days",
      [name, type, percent, days, id],
    );
    if (payment.rows.length === 0) {
      return res
        .status(404)
        .json({ success: false, message: "Payment type not found" });
    }

    return res.json({
      success: true,
      message: "New Payment type create successfully!",
      paymentType: payment.rows[0],
    });
  } catch (err) {
    logger.error("Error updating payment type:", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
}

export async function deletePaymentType(req, res) {
  try {
    const { id } = req.params;
    const { rowCount } = await pool.query(
      "DELETE FROM payment_types WHERE id = $1",
      [id],
    );
    if (rowCount === 0) {
      return res
        .status(404)
        .json({ success: false, message: "Payment type not found" });
    }
    return res.json({
      success: true,
      message: "Payment type deleted successfully",
    });
  } catch (err) {
    logger.error("Error deleting payment type:", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
}

export async function getCategories(req, res) {
  try {
    const { rows } = await pool.query("SELECT * FROM categories ORDER BY id");

    if (rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: "No Categories found",
      });
    }
    return res.json({ success: true, categories: rows });
  } catch (err) {
    logger.error("Error fetching categories:", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
}

export async function createCategory(req, res) {
  try {
    const { name } = req.body;
    if (!name) {
      return res
        .status(400)
        .json({ success: false, message: "Name is required" });
    }
    const { rows } = await pool.query(
      "INSERT INTO categories (name) VALUES ($1) RETURNING *",
      [name],
    );

    if (rows.length === 0) {
      return res.status(400).json({
        success: false,
        message: "Failed to create new category",
      });
    }
    return res.status(201).json({ success: true, category: rows[0] });
  } catch (err) {
    logger.error("Error creating categories:", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
}

export async function updateCategory(req, res) {
  try {
    const { id } = req.params;
    const { name, status } = req.body;
    if (!name) {
      return res
        .status(400)
        .json({ success: false, message: "Category name is required" });
    }
    const { rows } = await pool.query(
      "UPDATE categories SET name = $1, status = $2 WHERE id = $3 RETURNING *",
      [name, status, id],
    );
    if (rows.length === 0) {
      return res
        .status(404)
        .json({ success: false, message: "Failed to create category" });
    }
    return res.json({
      success: true,
      message: "Category updated successfully",
      category: rows[0],
    });
  } catch (err) {
    logger.error("Error updating categories:", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
}

export async function deleteCategory(req, res) {
  try {
    const { id } = req.params;
    const { rowCount } = await pool.query(
      "DELETE FROM categories WHERE id = $1",
      [id],
    );
    if (rowCount === 0) {
      return res
        .status(404)
        .json({ success: false, message: "Could not delete category" });
    }
    return res.json({
      success: true,
      message: "Category deleted successfully",
    });
  } catch (err) {
    logger.error("Error deleting categories:", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
}

export async function getSubcategories(req, res) {
  try {
    const { category_id } = req.query;

    let data;
    if (category_id) {
      data = await pool.query(
        "SELECT * FROM subcategories WHERE category_id = $1 ORDER BY id",
        [category_id],
      );
    } else {
      data = await pool.query("SELECT * FROM subcategories ORDER BY id");
    }

    if (data.rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: "No Sub-Categories found",
      });
    }
    return res.status(200).json({
      success: true,
      subCategories: data.rows,
    });
  } catch (err) {
    logger.error("Error fetching subCategories:", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
}

export async function createSubcategory(req, res) {
  try {
    const { name, category_id } = req.body;
    if (!name || !category_id) {
      return res
        .status(400)
        .json({ success: false, message: "Name and Category ID are required" });
    }
    const { rows } = await pool.query(
      "INSERT INTO subcategories (name, category_id) VALUES ($1, $2) RETURNING id, name, category_id",
      [name, category_id],
    );

    if (rows.length === 0) {
      return res
        .status(400)
        .json({ success: false, message: "Could not create sub-category" });
    }
    return res.status(201).json({ success: true, subcategory: rows[0] });
  } catch (err) {
    logger.error("Error creating subCategories:", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
}

export async function updateSubcategory(req, res) {
  try {
    const { id } = req.params;
    const { name, status, category_id } = req.body;
    if (!name || !category_id) {
      return res
        .status(400)
        .json({ success: false, message: "Name and Category ID are required" });
    }
    const { rows } = await pool.query(
      "UPDATE subcategories SET name = $1, category_id = $2, status = $3 WHERE id = $4 RETURNING *",
      [name, category_id, status, id],
    );
    if (rows.length === 0) {
      return res
        .status(404)
        .json({ success: false, message: "Subcategory not found" });
    }
    return res.json({ subcategory: rows[0] });
  } catch (err) {
    logger.error("Error updating subCategories:", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
}

export async function deleteSubcategory(req, res) {
  try {
    const { id } = req.params;
    const { rowCount } = await pool.query(
      "DELETE FROM subcategories WHERE id = $1",
      [id],
    );
    if (rowCount === 0) {
      return res
        .status(404)
        .json({ success: false, message: "Could not delete sub-category" });
    }
    return res.json({ message: "Subcategory deleted successfully" });
  } catch (err) {
    logger.error("Error delete subCategories:", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
}

export async function getVessels(req, res) {
  try {
    const { rows } = await pool.query(
      "SELECT id, name, capacity, status FROM vessels ORDER BY id",
    );
    if (rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: "No Vessel found",
      });
    }
    return res.json({ success: true, vessels: rows });
  } catch (err) {
    logger.error("Error fetching vessels:", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
}

export async function createVessel(req, res) {
  try {
    const { name, capacity, status } = req.body;
    if (!name || !capacity || !status) {
      return res
        .status(400)
        .json({ success: true, message: "Fileds are reqiured!" });
    }
    const { rows } = await pool.query(
      "INSERT INTO vessels (name, capacity, status) VALUES ($1, $2, $3) RETURNING *",
      [name, capacity, status],
    );
    return res.status(201).json({
      success: true,
      message: "Vessel added successfully",
      vessel: rows[0],
    });
  } catch (err) {
    logger.error("Error creating vessels:", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
}

export async function updateVessel(req, res) {
  try {
    const { id } = req.params;
    const { name, capacity, status } = req.body;
    if (!name || !capacity || !status) {
      return res.status(400).json({
        success: false,
        message: "Name, capacity, and status are required",
      });
    }
    const { rows } = await pool.query(
      "UPDATE vessels SET name = $1, capacity = $2, status = $3 WHERE id = $4 RETURNING *",
      [name, capacity, status, id],
    );
    if (rows.length === 0) {
      return res
        .status(404)
        .json({ success: false, message: "Vessel not found" });
    }
    return res.json({
      success: true,
      message: "Updated Vessel successfully",
      vessel: rows[0],
    });
  } catch (err) {
    logger.error("Error updating vessels:", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
}

export async function deleteVessel(req, res) {
  try {
    const { id } = req.params;
    const { rowCount } = await pool.query("DELETE FROM vessels WHERE id = $1", [
      id,
    ]);
    if (rowCount === 0) {
      return res
        .status(400)
        .json({ success: false, message: "Could not delete Vessel" });
    }
    return res.json({ success: true, message: "Vessel deleted successfully" });
  } catch (err) {
    logger.error("Error deleting vessels:", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
}

export async function getShippingLines(req, res) {
  try {
    const { rows } = await pool.query(
      "SELECT id, name FROM shipping_lines ORDER BY name ASC",
    );
    if (rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: "No Sipping Lines Found!",
      });
    }
    const options = formatOptions(rows, "id", "name");
    return res.json({ success: true, shippingLineOptions: options });
  } catch (err) {
    logger.error("Error fetching shipping lines:", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
}

export async function getCurrencies(req, res) {
  try {
    const { rows } = await pool.query(
      "SELECT code as value FROM currencies ORDER BY code ASC",
    );
    if (rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: "No Currencies Found!",
      });
    }
    const options = formatOptions(rows, "value", "value");
    return res.json({ success: true, currencyOptions: options });
  } catch (err) {
    logger.error("Error fetching currencies:", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
}

export const addNewStatus = async (req, res) => {
  try {
    const {
      order_status,
      container_status,
      consignment_status,
      days_offset,
      sorting_number,
    } = req.body;

    const newStatus = await pool.query(
      `INSERT INTO statuses (order_status, container_status, consignment_status, days_offset, sorting_number, status)
       VALUES ($1, $2, $3, $4, $5, true)
       RETURNING *`,
      [
        order_status || null,
        container_status || null,
        consignment_status || null,
        days_offset ?? 0,
        sorting_number ?? 0,
      ],
    );

    return res.status(201).json({
      success: true,
      message: "Status added!",
      status: newStatus.rows[0],
    });
  } catch (err) {
    logger.error("Error creating status for statuses:", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
};

export const getAllStatus = async (req, res) => {
  try {
    const statuses = await pool.query(
      `SELECT * FROM statuses ORDER BY sorting_number ASC`,
    );
    if (statuses.rowCount === 0) {
      return res
        .status(404)
        .json({ success: false, message: "No statuses found!" });
    }
    return res.status(200).json({ success: true, statuses: statuses.rows });
  } catch (err) {
    logger.error("Error fetching statuses:", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
};

export const updateStatus = async (req, res) => {
  try {
    const { id } = req.params;
    const {
      order_status,
      container_status,
      consignment_status,
      days_offset,
      status,
      sorting_number,
    } = req.body;

    const updatedStatus = await pool.query(
      `UPDATE statuses
       SET
         order_status = COALESCE($1, order_status),
         container_status = COALESCE($2, container_status),
         consignment_status = COALESCE($3, consignment_status),
         days_offset = COALESCE($4, days_offset),
         status = COALESCE($5, status),
         sorting_number = COALESCE($6, sorting_number)
       WHERE id = $7
       RETURNING *`,
      [
        order_status,
        container_status,
        consignment_status,
        days_offset,
        status,
        sorting_number,
        id,
      ],
    );

    if (updatedStatus.rowCount === 0) {
      return res
        .status(404)
        .json({ success: false, message: "Status not found!" });
    }

    return res.status(200).json({
      success: true,
      message: "Status updated!",
      status: updatedStatus.rows[0],
    });
  } catch (err) {
    logger.error("Error updating status for statuses:", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
};

export const deleteStatus = async (req, res) => {
  try {
    const { id } = req.params;

    const deleted = await pool.query(
      `DELETE FROM statuses WHERE id = $1 RETURNING *`,
      [id],
    );

    if (deleted.rowCount === 0) {
      return res
        .status(404)
        .json({ success: false, message: "Status not found!" });
    }

    return res.status(200).json({ success: true, message: "Status deleted!" });
  } catch (err) {
    logger.error("Error deleting status from statuses:", {
      error: err.message,
    });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
};

export const createBugReport = async (req, res) => {
  const { title, description } = req.body;

  if (!title?.trim() || !description?.trim()) {
    logger.warn("Bug report validation failed", {
      reason: "Missing title or description",
    });

    return res
      .status(400)
      .json({ message: "Title and description are required." });
  }

  if (title.trim().length > 80) {
    logger.warn("Bug report validation failed", {
      reason: "Title exceeds maximum length",
    });

    return res
      .status(400)
      .json({ message: "Title must be 80 characters or fewer." });
  }

  if (description.trim().length > 1000) {
    logger.warn("Bug report validation failed", {
      reason: "Description exceeds maximum length",
    });

    return res
      .status(400)
      .json({ message: "Description must be 1000 characters or fewer." });
  }

  try {
    const reportQuery = `
      INSERT INTO bug_reports (title, description, created_at) 
      VALUES ($1, $2, NOW()) 
      RETURNING id, title, description, is_fixed AS "isFixed", created_at AS "createdAt"
    `;

    const { rows } = await pool.query(reportQuery, [
      title.trim(),
      description.trim(),
    ]);

    const newReport = rows[0];

    logger.info("Bug report created", {
      reportId: newReport.id,
    });

    let attachments = [];

    if (req.files?.length) {
      const attachmentInserts = req.files.map((file) =>
        pool.query(
          `INSERT INTO bug_report_attachments (report_id, image_url, created_at) VALUES ($1, $2, NOW()) RETURNING image_url`,
          [newReport.id, file.path],
        ),
      );

      const results = await Promise.all(attachmentInserts);
      attachments = results.map((r) => r.rows[0].image_url);

      logger.info("Bug report attachments uploaded", {
        reportId: newReport.id,
        count: attachments.length,
      });
    }

    try {
      await sendBugReportEmail({
        title: newReport.title,
        description: newReport.description,
        submittedAt: newReport.createdAt,
        attachments,
      });

      logger.info("Bug report notification email sent", {
        reportId: newReport.id,
      });
    } catch (emailErr) {
      logger.error("Failed to send bug report notification email", {
        reportId: newReport.id,
        error: emailErr.message,
      });
    }

    return res.status(201).json({
      message: "Bug report submitted successfully.",
      report: { ...newReport, attachments },
    });
  } catch (err) {
    logger.error("Failed to create bug report", {
      error: err.message,
    });

    return res
      .status(500)
      .json({ message: "Failed to save the report. Please try again." });
  }
};

export const getBugReports = async (req, res) => {
  try {
    const query = `
      SELECT 
        br.id,
        br.title,
        br.description,
        br.is_fixed AS "isFixed",
        br.created_at AS "createdAt",
        COALESCE(
          JSON_AGG(bra.image_url) FILTER (WHERE bra.image_url IS NOT NULL),
          '[]'
        ) AS attachments
      FROM bug_reports br
      LEFT JOIN bug_report_attachments bra ON bra.report_id = br.id
      GROUP BY br.id
      ORDER BY br.created_at DESC
    `;

    const { rows } = await pool.query(query);

    logger.info("Bug reports fetched", {
      count: rows.length,
    });

    return res.status(200).json({ reports: rows });
  } catch (err) {
    await logErrorAndNotify("getBugReports", err, req);
    logger.error("Error fetching bug reports:", {
      error: err.message,
    });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
};

export const updateBugReport = async (req, res) => {
  const { id } = req.params;
  const { title, description, isFixed } = req.body;

  try {
    const { rows } = await pool.query(
      `UPDATE bug_reports 
       SET 
         title = COALESCE($1, title), 
         description = COALESCE($2, description), 
         is_fixed = COALESCE($3, is_fixed)
       WHERE id = $4 
       RETURNING id, title, description, is_fixed AS "isFixed", created_at AS "createdAt"`,
      [title?.trim() || null, description?.trim() || null, isFixed ?? null, id],
    );

    if (rows.length === 0) {
      logger.warn("Bug report not found", {
        reportId: id,
      });

      return res.status(404).json({ message: "Bug report not found." });
    }

    let attachments = [];

    if (req.files?.length) {
      const inserts = req.files.map((file) =>
        pool.query(
          `INSERT INTO bug_report_attachments (report_id, image_url, created_at) VALUES ($1, $2, NOW()) RETURNING image_url`,
          [id, file.path],
        ),
      );

      const results = await Promise.all(inserts);
      attachments = results.map((r) => r.rows[0].image_url);

      logger.info("Bug report attachments uploaded", {
        reportId: id,
        count: attachments.length,
      });
    }

    const { rows: existingAttachments } = await pool.query(
      `SELECT image_url FROM bug_report_attachments WHERE report_id = $1`,
      [id],
    );

    logger.info("Bug report updated", {
      reportId: id,
      status: isFixed,
    });

    return res.status(200).json({
      message: "Bug report updated successfully.",
      report: {
        ...rows[0],
        attachments: existingAttachments.map((a) => a.image_url),
      },
    });
  } catch (err) {
    logger.error("Failed to update bug report", {
      reportId: id,
      error: err.message,
    });

    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
};

export const deleteBugReport = async (req, res) => {
  const { id } = req.params;

  try {
    const query = `DELETE FROM bug_reports WHERE id = $1 RETURNING id`;
    const { rows } = await pool.query(query, [id]);

    if (rows.length === 0) {
      logger.warn("Bug report not found", {
        reportId: id,
      });

      return res.status(404).json({
        message: "Bug report not found.",
      });
    }

    logger.info("Bug report deleted", {
      reportId: id,
    });

    return res.status(200).json({
      message: "Bug report deleted successfully.",
    });
  } catch (err) {
    logger.error("Failed to delete bug report", {
      reportId: id,
      error: err.message,
    });

    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
};

export const getDashboardData = async (req, res) => {
  const client = await pool.connect();

  try {
    const [
      recentOrders,
      ordersCount,
      containersCount,
      consignmentsCount,
      recentConsignments,
      statusesResult,
      ordersByStatusResult,
      customersCount,
      sendersCount,
      receiversCount,
    ] = await Promise.all([
      client.query(`
        SELECT
          o.id,
          o.rgl_booking_number,
          (SELECT r.status FROM receivers r WHERE r.order_id = o.id ORDER BY r.id DESC LIMIT 1) as order_status,
          (SELECT r.receiver_name FROM receivers r WHERE r.order_id = o.id ORDER BY r.id DESC LIMIT 1) as receiver_name,
          (SELECT r.eta FROM receivers r WHERE r.order_id = o.id ORDER BY r.id DESC LIMIT 1) as eta,
          (SELECT oi.item_ref 
          FROM order_items oi 
          WHERE oi.order_id = o.id 
          ORDER BY oi.id DESC 
          LIMIT 1) as item_ref
        FROM orders o
        ORDER BY o.id DESC
        LIMIT 10
      `),

      client.query(`SELECT COUNT(*) as total FROM orders`),
      client.query(`SELECT COUNT(*) as total FROM container_master`),
      client.query(`SELECT COUNT(*) as total FROM consignments`),

      client.query(`
        SELECT
          cons.id,
          cons.consignment_number,
          cons.status,
          COALESCE(consignee_tp.company_name, cons.consignee) AS consignee,
          cons.eta
        FROM consignments cons
        LEFT JOIN third_parties consignee_tp ON cons.consignee_id = consignee_tp.id
        ORDER BY cons.id DESC
        LIMIT 10
      `),

      client.query(
        `SELECT * FROM statuses WHERE status = true ORDER BY sorting_number ASC`,
      ),

      client.query(`
        SELECT r.status, COUNT(DISTINCT r.order_id) as count 
        FROM receivers r 
        WHERE r.status IS NOT NULL 
        GROUP BY r.status
      `),

      client.query(`SELECT COUNT(*) as total FROM customers`),
      client.query(`SELECT COUNT(*) as total FROM senders`),
      client.query(`SELECT COUNT(*) as total FROM receivers`),
    ]);

    const statuses = statusesResult.rows;

    const formatCounts = (rows) => {
      const obj = {};
      rows.forEach((r) => (obj[r.status] = Number(r.count)));
      return obj;
    };

    const ordersByStatus = formatCounts(ordersByStatusResult.rows);

    return res.status(200).json({
      success: true,
      data: {
        counts: {
          orders: Number(ordersCount.rows[0]?.total || 0),
          containers: Number(containersCount.rows[0]?.total || 0),
          consignments: Number(consignmentsCount.rows[0]?.total || 0),
          customers: Number(customersCount.rows[0]?.total || 0),
          senders: Number(sendersCount.rows[0]?.total || 0),
          receivers: Number(receiversCount.rows[0]?.total || 0),
        },
        countsByStatus: {
          orders: ordersByStatus,
        },
        statuses,
        recentOrders: recentOrders.rows,
        recentConsignments: recentConsignments.rows,
      },
    });
  } catch (err) {
    logger.error("Error fetching dashboard data", { error: err.message });
    return res
      .status(500)
      .json({ success: false, message: "Failed to fetch dashboard data" });
  } finally {
    client.release();
  }
};
