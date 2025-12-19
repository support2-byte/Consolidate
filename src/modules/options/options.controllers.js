// src/modules/options/functions.js
import pool from '../../db/pool.js';  // Fixed: Removed extra space, added .js

// Helper function to format options (shared across all functions)
const formatOptions = (rows, valueField = 'id', labelField = 'name') => {
  return rows.map(row => ({
    value: row[valueField],
    label: row[labelField]
  }));
};

// Hardcoded common ports for origin and destination (since no dedicated table/FK in schema)
const commonPorts = [
  'Port of Felixstowe',
  'Port of Southampton',
  'Port of London Gateway',
  'Port of Liverpool',
  'Port of Rotterdam',
  'Port of Antwerp',
  'Port of Singapore',
  'Port of Shanghai',
  'Port of New York',
  'Port of Los Angeles'
];

// GET Shippers
export async function getShippers(req, res) {
  try {
    const { rows } = await pool.query('SELECT id, name FROM shippers ORDER BY name ASC');
    const options = formatOptions(rows, 'id', 'name');
    res.json({ shipperOptions: options });
  } catch (err) {
    console.error('Error fetching shippers:', err);
    res.status(500).json({ error: 'Failed to fetch shippers' });
  }
}

// GET Consignees
export async function getConsignees(req, res) {
  try {
    const { rows } = await pool.query('SELECT id, name FROM consignees ORDER BY name ASC');
    const options = formatOptions(rows, 'id', 'name');
    res.json({ consigneeOptions: options });
  } catch (err) {
    console.error('Error fetching consignees:', err);
    res.status(500).json({ error: 'Failed to fetch consignees' });
  }
}
// GET Third Parties
export async function getThirdParties(req, res) {
  try {
    const { rows } = await pool.query('SELECT * FROM third_parties ORDER BY company_name ASC');
    res.json({ third_parties: rows });
  } catch (err) {
    console.error('Error fetching third parties:', err);
    res.status(500).json({ error: 'Failed to fetch third parties' });
  }
}

// POST Third Party
export async function createThirdParty(req, res) {
  try {
    const { company_name, contact_name, contact_email, contact_phone, address, type } = req.body;
    const { rows } = await pool.query(
      'INSERT INTO third_parties (company_name, contact_name, contact_email, contact_phone, address, type) VALUES ($1, $2, $3, $4, $5, $6) RETURNING *',
      [company_name, contact_name, contact_email, contact_phone, address, type]
    );
    res.status(201).json({ third_party: rows[0] });
  } catch (err) {
    console.error('Error creating third party:', err);
    res.status(500).json({ error: 'Failed to create third party' });
  }
}

// PUT Third Party
export async function updateThirdParty(req, res) {
  try {
    const { id } = req.params;
    const { company_name, contact_name, contact_email, contact_phone, address, type } = req.body;
    const { rows } = await pool.query(
      'UPDATE third_parties SET company_name = $1, contact_name = $2, contact_email = $3, contact_phone = $4, address = $5, type = $6 WHERE id = $7 RETURNING *',
      [company_name, contact_name, contact_email, contact_phone, address, type, id]
    );
    if (rows.length === 0) {
      return res.status(404).json({ error: 'Third party not found' });
    }
    res.json({ third_party: rows[0] });
  } catch (err) {
    console.error('Error updating third party:', err);
    res.status(500).json({ error: 'Failed to update third party' });
  }
}

// DELETE Third Party
export async function deleteThirdParty(req, res) {
  try {
    const { id } = req.params;
    const { rows } = await pool.query('DELETE FROM third_parties WHERE id = $1 RETURNING *', [id]);
    if (rows.length === 0) {
      return res.status(404).json({ error: 'Third party not found' });
    }
    res.json({ message: 'Third party deleted successfully', third_party: rows[0] });
  } catch (err) {
    console.error('Error deleting third party:', err);
    res.status(500).json({ error: 'Failed to delete third party' });
  }
}
// GET Origins (hardcoded since no FK/table in schema)
export async function getOrigins(req, res) {
  try {
    const options = commonPorts.map(port => ({ value: port, label: port }));
    res.json({ originOptions: options });
  } catch (err) {
    console.error('Error fetching origins:', err);
    res.status(500).json({ error: 'Failed to fetch origins' });
  }
}

// GET Places (full list)
export async function getPlaces(req, res) {
  try {
    const { rows } = await pool.query('SELECT id, name, is_loading, is_destination, country, latitude, longitude FROM places ORDER BY id');
    res.json({ places: rows });
  } catch (err) {
    console.error('Error fetching places:', err);
    res.status(500).json({ error: 'Failed to fetch places' });
  }
}

// POST Place
export async function createPlace(req, res) {
  try {
    const { name, is_loading, is_destination, country, latitude, longitude } = req.body;
    if (!name || !country) {
      return res.status(400).json({ error: 'Name and country are required' });
    }
    const { rows } = await pool.query(
      'INSERT INTO places (name, is_loading, is_destination, country, latitude, longitude) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id, name, is_loading, is_destination, country, latitude, longitude',
      [name, is_loading, is_destination, country, latitude, longitude]
    );
    res.status(201).json({ place: rows[0] });
  } catch (err) {
    console.error('Error creating place:', err);
    res.status(500).json({ error: 'Failed to create place' });
  }
}

// PUT Place
export async function updatePlace(req, res) {
  try {
    const { id } = req.params;
    const { name, is_loading, is_destination, country, latitude, longitude } = req.body;
    if (!name || !country) {
      return res.status(400).json({ error: 'Name and country are required' });
    }
    const { rows } = await pool.query(
      'UPDATE places SET name = $1, is_loading = $2, is_destination = $3, country = $4, latitude = $5, longitude = $6 WHERE id = $7 RETURNING id, name, is_loading, is_destination, country, latitude, longitude',
      [name, is_loading, is_destination, country, latitude, longitude, id]
    );
    if (rows.length === 0) {
      return res.status(404).json({ error: 'Place not found' });
    }
    res.json({ place: rows[0] });
  } catch (err) {
    console.error('Error updating place:', err);
    res.status(500).json({ error: 'Failed to update place' });
  }
}

// DELETE Place
export async function deletePlace(req, res) {
  try {
    const { id } = req.params;
    const { rowCount } = await pool.query('DELETE FROM places WHERE id = $1', [id]);
    if (rowCount === 0) {
      return res.status(404).json({ error: 'Place not found' });
    }
    res.json({ message: 'Place deleted successfully' });
  } catch (err) {
    console.error('Error deleting place:', err);
    res.status(500).json({ error: 'Failed to delete place' });
  }
}

// GET Destinations (hardcoded since no FK/table in schema)
export async function getDestinations(req, res) {
  try {
    const options = commonPorts.map(port => ({ value: port, label: port }));
    res.json({ destinationOptions: options });
  } catch (err) {
    console.error('Error fetching destinations:', err);
    res.status(500).json({ error: 'Failed to fetch destinations' });
  }
}

// // GET Banks
// export async function getBanks(req, res) {
//   try {
//     const { rows } = await pool.query('SELECT id, name FROM banks ORDER BY name ASC');
//     const options = formatOptions(rows, 'id', 'name');
//     res.json({ bankOptions: options });
//   } catch (err) {
//     console.error('Error fetching banks:', err);
//     res.status(500).json({ error: 'Failed to fetch banks' });
//   }
// }


// GET Banks
export async function getBanks(req, res) {
  try {
    const { rows } = await pool.query('SELECT * FROM banks ORDER BY name ASC');
    res.json({ banks: rows });
  } catch (err) {
    console.error('Error fetching banks:', err);
    res.status(500).json({ error: 'Failed to fetch banks' });
  }
}

// POST Bank
export async function createBank(req, res) {
  try {
    const { name, account_number, swift_code, branch, address, currency } = req.body;
    const { rows } = await pool.query(
      'INSERT INTO banks (name, account_number, swift_code, branch, address, currency) VALUES ($1, $2, $3, $4, $5, $6) RETURNING *',
      [name, account_number, swift_code, branch, address, currency || 'USD']
    );
    res.status(201).json({ bank: rows[0] });
  } catch (err) {
    console.error('Error creating bank:', err);
    res.status(500).json({ error: 'Failed to create bank' });
  }
}

// PUT Bank
export async function updateBank(req, res) {
  try {
    const { id } = req.params;
    const { name, account_number, swift_code, branch, address, currency } = req.body;
    const { rows } = await pool.query(
      'UPDATE banks SET name = $1, account_number = $2, swift_code = $3, branch = $4, address = $5, currency = $6 WHERE id = $7 RETURNING *',
      [name, account_number, swift_code, branch, address, currency, id]
    );
    if (rows.length === 0) {
      return res.status(404).json({ error: 'Bank not found' });
    }
    res.json({ bank: rows[0] });
  } catch (err) {
    console.error('Error updating bank:', err);
    res.status(500).json({ error: 'Failed to update bank' });
  }
}

// DELETE Bank
export async function deleteBank(req, res) {
  try {
    const { id } = req.params;
    const { rows } = await pool.query('DELETE FROM banks WHERE id = $1 RETURNING *', [id]);
    if (rows.length === 0) {
      return res.status(404).json({ error: 'Bank not found' });
    }
    res.json({ message: 'Bank deleted successfully', bank: rows[0] });
  } catch (err) {
    console.error('Error deleting bank:', err);
    res.status(500).json({ error: 'Failed to delete bank' });
  }
}
// Assuming the table schema is:
// CREATE TYPE payment_category AS ENUM ('DP', 'AP', 'DA');
// CREATE TABLE payment_types (
//   id SERIAL PRIMARY KEY,
//   name VARCHAR(255) UNIQUE NOT NULL,
//   category payment_category NOT NULL,
//   percent INTEGER DEFAULT 0 CHECK (percent >= 0),
//   days INTEGER DEFAULT 0 CHECK (days >= 0)
// );

// GET Payment Types (full list from table)
export async function getPaymentTypes(req, res) {
  try {
    const { rows } = await pool.query('SELECT id, name, type, percent, days FROM payment_types ORDER BY id');
    res.json({ paymentTypes: rows });
  } catch (err) {
    console.error('Error fetching payment types:', err);
    res.status(500).json({ error: 'Failed to fetch payment types' });
  }
}

// GET Payment Type Options (from enum, for dropdowns elsewhere)
export async function getPaymentTypeOptions(req, res) {
  try {
    const { rows } = await pool.query(`SELECT unnest(enum_range(NULL::payment_category)) as value`);
    const options = formatOptions(rows, 'value', 'value');
    res.json({ paymentTypeOptions: options });
  } catch (err) {
    console.error('Error fetching payment type options:', err);
    res.status(500).json({ error: 'Failed to fetch payment type options' });
  }
}

// POST Payment Type
export async function createPaymentType(req, res) {
  try {
    const { name, type, percent = 0, days = 0 } = req.body;
    if (!name || !type) {
      return res.status(400).json({ error: 'Name and type are required' });
    }
    // Optional: Dynamically add to enum if not exists (requires privileges)
    const enumCheck = await pool.query(
      `SELECT 1 FROM pg_enum WHERE enumtypid = 'payment_category'::regtype AND enumlabel = $1`, 
      [type]
    );
    if (enumCheck.rowCount === 0) {
      await pool.query(`ALTER TYPE payment_category ADD VALUE IF NOT EXISTS $1`, [type]);
    }
    const { rows } = await pool.query(
      'INSERT INTO payment_types (name, type, percent, days) VALUES ($1, $2, $3, $4) RETURNING id, name, type, percent, days',
      [name, type, percent, days]
    );
    res.status(201).json({ paymentType: rows[0] });
  } catch (err) {
    console.error('Error creating payment type:', err);
    if (err.code === '23505') { // Unique violation
      res.status(409).json({ error: 'Payment type name already exists' });
    } else if (err.code === '42703') { // Enum value not found (if FK enforced)
      res.status(400).json({ error: 'Invalid payment type value' });
    } else {
      res.status(500).json({ error: 'Failed to create payment type' });
    }
  }
}

// PUT Payment Type
export async function updatePaymentType(req, res) {
  try {
    const { id } = req.params;
    const { name, type, percent = 0, days = 0 } = req.body;
    if (!name || !type) {
      return res.status(400).json({ error: 'Name and type are required' });
    }
    // Optional: Dynamically add to enum if new type
    const enumCheck = await pool.query(
      `SELECT 1 FROM pg_enum WHERE enumtypid = 'payment_category'::regtype AND enumlabel = $1`, 
      [type]
    );
    if (enumCheck.rowCount === 0) {
      await pool.query(`ALTER TYPE payment_category ADD VALUE IF NOT EXISTS $1`, [type]);
    }
    const { rows } = await pool.query(
      'UPDATE payment_types SET name = $1, type = $2, percent = $3, days = $4 WHERE id = $5 RETURNING id, name, type, percent, days',
      [name, type, percent, days, id]
    );
    if (rows.length === 0) {
      return res.status(404).json({ error: 'Payment type not found' });
    }
    res.json({ paymentType: rows[0] });
  } catch (err) {
    console.error('Error updating payment type:', err);
    if (err.code === '23505') { // Unique violation
      res.status(409).json({ error: 'Payment type name already exists' });
    } else if (err.code === '42703') { // Enum value not found
      res.status(400).json({ error: 'Invalid payment type value' });
    } else {
      res.status(500).json({ error: 'Failed to update payment type' });
    }
  }
}

// DELETE Payment Type
export async function deletePaymentType(req, res) {
  try {
    const { id } = req.params;
    const { rowCount } = await pool.query('DELETE FROM payment_types WHERE id = $1', [id]);
    if (rowCount === 0) {
      return res.status(404).json({ error: 'Payment type not found' });
    }
    // Note: Enum values aren't deleted automatically; handle manually if needed
    res.json({ message: 'Payment type deleted successfully' });
  } catch (err) {
    console.error('Error deleting payment type:', err);
    res.status(500).json({ error: 'Failed to delete payment type' });
  }
}

// Assuming the table schema:
// CREATE TABLE categories (
//   id SERIAL PRIMARY KEY,
//   name VARCHAR(255) UNIQUE NOT NULL
// );
//
// CREATE TABLE subcategories (
//   id SERIAL PRIMARY KEY,
//   name VARCHAR(255) NOT NULL,
//   category_id INTEGER REFERENCES categories(id) ON DELETE CASCADE
// );

// GET Categories (full list)
export async function getCategories(req, res) {
  try {
    const { rows } = await pool.query('SELECT id, name FROM categories ORDER BY id');
    res.json({ categories: rows });
  } catch (err) {
    console.error('Error fetching categories:', err);
    res.status(500).json({ error: 'Failed to fetch categories' });
  }
}

// POST Category
export async function createCategory(req, res) {
  try {
    const { name } = req.body;
    if (!name) {
      return res.status(400).json({ error: 'Name is required' });
    }
    const { rows } = await pool.query(
      'INSERT INTO categories (name) VALUES ($1) RETURNING id, name',
      [name]
    );
    res.status(201).json({ category: rows[0] });
  } catch (err) {
    console.error('Error creating category:', err);
    if (err.code === '23505') { // Unique violation
      res.status(409).json({ error: 'Category name already exists' });
    } else {
      res.status(500).json({ error: 'Failed to create category' });
    }
  }
}

// PUT Category
export async function updateCategory(req, res) {
  try {
    const { id } = req.params;
    const { name } = req.body;
    if (!name) {
      return res.status(400).json({ error: 'Name is required' });
    }
    const { rows } = await pool.query(
      'UPDATE categories SET name = $1 WHERE id = $2 RETURNING id, name',
      [name, id]
    );
    if (rows.length === 0) {
      return res.status(404).json({ error: 'Category not found' });
    }
    res.json({ category: rows[0] });
  } catch (err) {
    console.error('Error updating category:', err);
    if (err.code === '23505') { // Unique violation
      res.status(409).json({ error: 'Category name already exists' });
    } else {
      res.status(500).json({ error: 'Failed to update category' });
    }
  }
}

// DELETE Category
export async function deleteCategory(req, res) {
  try {
    const { id } = req.params;
    const { rowCount } = await pool.query('DELETE FROM categories WHERE id = $1', [id]);
    if (rowCount === 0) {
      return res.status(404).json({ error: 'Category not found' });
    }
    // Subcategories will be deleted via ON DELETE CASCADE
    res.json({ message: 'Category deleted successfully' });
  } catch (err) {
    console.error('Error deleting category:', err);
    res.status(500).json({ error: 'Failed to delete category' });
  }
}

// GET Subcategories (full list, optionally filtered by category_id)
export async function getSubcategories(req, res) {
  try {
    const { category_id } = req.query;
    let query = 'SELECT id, name, category_id FROM subcategories';
    let params = [];
    if (category_id) {
      query += ' WHERE category_id = $1';
      params = [category_id];
    }
    query += ' ORDER BY id';
    const { rows } = await pool.query(query, params);
    res.json({ subcategories: rows });
  } catch (err) {
    console.error('Error fetching subcategories:', err);
    res.status(500).json({ error: 'Failed to fetch subcategories' });
  }
}

// POST Subcategory
export async function createSubcategory(req, res) {
  try {
    const { name, category_id } = req.body;
    if (!name || !category_id) {
      return res.status(400).json({ error: 'Name and category_id are required' });
    }
    // Verify category exists
    const catCheck = await pool.query('SELECT id FROM categories WHERE id = $1', [category_id]);
    if (catCheck.rowCount === 0) {
      return res.status(400).json({ error: 'Invalid category_id' });
    }
    const { rows } = await pool.query(
      'INSERT INTO subcategories (name, category_id) VALUES ($1, $2) RETURNING id, name, category_id',
      [name, category_id]
    );
    res.status(201).json({ subcategory: rows[0] });
  } catch (err) {
    console.error('Error creating subcategory:', err);
    res.status(500).json({ error: 'Failed to create subcategory' });
  }
}

// PUT Subcategory
export async function updateSubcategory(req, res) {
  try {
    const { id } = req.params;
    const { name, category_id } = req.body;
    if (!name || !category_id) {
      return res.status(400).json({ error: 'Name and category_id are required' });
    }
    // Verify category exists
    const catCheck = await pool.query('SELECT id FROM categories WHERE id = $1', [category_id]);
    if (catCheck.rowCount === 0) {
      return res.status(400).json({ error: 'Invalid category_id' });
    }
    const { rows } = await pool.query(
      'UPDATE subcategories SET name = $1, category_id = $2 WHERE id = $3 RETURNING id, name, category_id',
      [name, category_id, id]
    );
    if (rows.length === 0) {
      return res.status(404).json({ error: 'Subcategory not found' });
    }
    res.json({ subcategory: rows[0] });
  } catch (err) {
    console.error('Error updating subcategory:', err);
    res.status(500).json({ error: 'Failed to update subcategory' });
  }
}

// DELETE Subcategory
export async function deleteSubcategory(req, res) {
  try {
    const { id } = req.params;
    const { rowCount } = await pool.query('DELETE FROM subcategories WHERE id = $1', [id]);
    if (rowCount === 0) {
      return res.status(404).json({ error: 'Subcategory not found' });
    }
    res.json({ message: 'Subcategory deleted successfully' });
  } catch (err) {
    console.error('Error deleting subcategory:', err);
    res.status(500).json({ error: 'Failed to delete subcategory' });
  }
}


// GET Vessels (full list)
export async function getVessels(req, res) {
  try {
    const { rows } = await pool.query('SELECT id, name, capacity, status FROM vessels ORDER BY id');
    res.json({ vessels: rows });
  } catch (err) {
    console.error('Error fetching vessels:', err);
    res.status(500).json({ error: 'Failed to fetch vessels' });
  }
}

// POST Vessel
export async function createVessel(req, res) {
  try {
    const { name, capacity, status } = req.body;
    if (!name || !capacity || !status) {
      return res.status(400).json({ error: 'Name, capacity, and status are required' });
    }
    const { rows } = await pool.query(
      'INSERT INTO vessels (name, capacity, status) VALUES ($1, $2, $3) RETURNING id, name, capacity, status',
      [name, capacity, status]
    );
    res.status(201).json({ vessel: rows[0] });
  } catch (err) {
    console.error('Error creating vessel:', err);
    res.status(500).json({ error: 'Failed to create vessel' });
  }
}

// PUT Vessel
export async function updateVessel(req, res) {
  try {
    const { id } = req.params;
    const { name, capacity, status } = req.body;
    if (!name || !capacity || !status) {
      return res.status(400).json({ error: 'Name, capacity, and status are required' });
    }
    const { rows } = await pool.query(
      'UPDATE vessels SET name = $1, capacity = $2, status = $3 WHERE id = $4 RETURNING id, name, capacity, status',
      [name, capacity, status, id]
    );
    if (rows.length === 0) {
      return res.status(404).json({ error: 'Vessel not found' });
    }
    res.json({ vessel: rows[0] });
  } catch (err) {
    console.error('Error updating vessel:', err);
    res.status(500).json({ error: 'Failed to update vessel' });
  }
}

// DELETE Vessel
export async function deleteVessel(req, res) {
  try {
    const { id } = req.params;
    const { rowCount } = await pool.query('DELETE FROM vessels WHERE id = $1', [id]);
    if (rowCount === 0) {
      return res.status(404).json({ error: 'Vessel not found' });
    }
    res.json({ message: 'Vessel deleted successfully' });
  } catch (err) {
    console.error('Error deleting vessel:', err);
    res.status(500).json({ error: 'Failed to delete vessel' });
  }
}

// GET Shipping Lines
export async function getShippingLines(req, res) {
  try {
    const { rows } = await pool.query('SELECT id, name FROM shipping_lines ORDER BY name ASC');
    const options = formatOptions(rows, 'id', 'name');
    res.json({ shippingLineOptions: options });
  } catch (err) {
    console.error('Error fetching shipping lines:', err);
    res.status(500).json({ error: 'Failed to fetch shipping lines' });
  }
}

// GET Currencies
export async function getCurrencies(req, res) {
  try {
    const { rows } = await pool.query('SELECT code as value FROM currencies ORDER BY code ASC');
    const options = formatOptions(rows, 'value', 'value');
    res.json({ currencyOptions: options });
  } catch (err) {
    console.error('Error fetching currencies:', err);
    res.status(500).json({ error: 'Failed to fetch currencies' });
  }
}


// GET all ETA configs
export async function getEtaConfigs(req, res) {
  try {
    const { rows } = await pool.query('SELECT * FROM eta_config ORDER BY id ASC');
    res.json(rows);
  } catch (err) {
    console.error('Error fetching eta configs:', err);
    res.status(500).json({ error: 'Failed to fetch eta configs' });
  }
}

// POST new ETA config
export async function createEtaConfig(req, res) {
  const { status, days_offset } = req.body;
  try {
    const { rows } = await pool.query(
      'INSERT INTO eta_config (status, days_offset) VALUES ($1, $2) RETURNING *',
      [status, days_offset || 0] // Default handled by DB, but explicit for safety
    );
    res.status(201).json(rows[0]);
  } catch (err) {
    console.error('Error creating eta config:', err);
    if (err.code === '23505') { // Unique violation on status
      return res.status(409).json({ error: 'Status already exists' });
    }
    res.status(500).json({ error: 'Failed to create eta config' });
  }
}

// PUT update ETA config
export async function updateEtaConfig(req, res) {
  const { id } = req.params;
  const { status, days_offset } = req.body;
  try {
    const { rows } = await pool.query(
      'UPDATE eta_config SET status = $1, days_offset = $2, updated_at = CURRENT_TIMESTAMP WHERE id = $3 RETURNING *',
      [status, days_offset || 0, id]
    );
    if (rows.length === 0) {
      return res.status(404).json({ error: 'Eta config not found' });
    }
    res.json(rows[0]);
  } catch (err) {
    console.error('Error updating eta config:', err);
    if (err.code === '23505') { // Unique violation on status
      return res.status(409).json({ error: 'Status already exists' });
    }
    res.status(500).json({ error: 'Failed to update eta config' });
  }
}

// DELETE ETA config
export async function deleteEtaConfig(req, res) {
  const { id } = req.params;
  try {
    const { rows } = await pool.query(
      'DELETE FROM eta_config WHERE id = $1 RETURNING *',
      [id]
    );
    if (rows.length === 0) {
      return res.status(404).json({ error: 'Eta config not found' });
    }
    res.json({ message: 'Eta config deleted successfully' });
  } catch (err) {
    console.error('Error deleting eta config:', err);
    res.status(500).json({ error: 'Failed to delete eta config' });
  }
}
// GET Statuses (from enum)
export async function getStatuses(req, res) {
  try {
    const { rows } = await pool.query(`SELECT unnest(enum_range(NULL::consignment_status)) as value`);
    const options = formatOptions(rows, 'value', 'value');
    res.json({ statusOptions: options });
  } catch (err) {
    console.error('Error fetching statuses:', err);
    res.status(500).json({ error: 'Failed to fetch statuses' });
  }
}

// GET Container Statuses (hardcoded since no table/enum)
export async function getContainerStatuses(req, res) {
  try {
    const hardcodedStatuses = [
      'Pending',
      'Loaded',
      'In Transit',
      'Delivered',
      'Returned'
    ];
    const options = hardcodedStatuses.map(status => ({ value: status, label: status }));
    res.json({ containerStatusOptions: options });
  } catch (err) {
    console.error('Error fetching container statuses:', err);
    res.status(500).json({ error: 'Failed to fetch container statuses' });
  }
}