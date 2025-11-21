import pool from "../../db/pool.js";

function isValidDate(dateString) {
  if (!dateString) return false;
  const normalized = dateString.toString().split('T')[0];  // Strip time if full ISO
  const date = new Date(normalized);
  return !isNaN(date.getTime()) && normalized.match(/^\d{4}-\d{2}-\d{2}$/);
}

function normalizeDate(dateString) {
  if (!dateString) return null;
  const normalized = dateString.toString().split('T')[0];
  return isValidDate(normalized) ? normalized : null;
}

// Validate core consignment fields (required checks with error messages)
function validateConsignmentFields({
  consignmentNumber, status, remarks, shipper, consignee, origin, destination,
  eform, eformDate, bank, consignmentValue, paymentType, vessel, voyage,
  eta, shippingLine, sealNo, netWeight, grossWeight, containers, orders
}) {
  const errors = [];
  if (!consignmentNumber) errors.push('consignmentNumber');
  if (!status) errors.push('status');
  if (!shipper) errors.push('shipper');
  if (!consignee) errors.push('consignee');
  if (!origin) errors.push('origin');
  if (!destination) errors.push('destination');
  if (!eform || !eform.match(/^[A-Z]{3}-\d{6}$/)) errors.push(`eform (invalid format, got: "${eform}")`);
  if (!isValidDate(eformDate)) errors.push(`eformDate (got: "${eformDate}")`);
  if (!bank) errors.push('bank');
  if (consignmentValue === undefined || consignmentValue < 0 || isNaN(consignmentValue)) errors.push('consignmentValue (must be non-negative number)');
  if (!paymentType) errors.push('paymentType');
  if (!vessel) errors.push('vessel');
  if (!voyage || voyage.length < 3) errors.push(`voyage (min 3 chars, got: "${voyage}")`);
  if (eta && !isValidDate(eta)) errors.push(`eta (got: "${eta}")`);
  if (!shippingLine) errors.push('shippingLine');  // Optional? Adjust if needed
  if (sealNo && sealNo.length < 3) errors.push(`sealNo (min 3 chars, got: "${sealNo}")`);  // Optional validation
  if (netWeight === undefined || netWeight < 0 || isNaN(netWeight)) errors.push('netWeight (must be non-negative number)');
  if (grossWeight === undefined || grossWeight < 0 || isNaN(grossWeight)) errors.push('grossWeight (must be non-negative number)');
  if (!Array.isArray(containers) || containers.length < 1) errors.push('containers (at least one required)');
  if (!Array.isArray(orders) || orders.length < 1) errors.push('orders (at least one required)');  // Adjust if optional
  return errors;
}

// Validate containers array items
function validateContainers(containers) {
  return containers.map((container, index) => {
    const errors = [];
    if (!container.containerNo) errors.push(`containers[${index}].containerNo`);
    if (!container.size) errors.push(`containers[${index}].size`);
    if (container.numberOfDays !== undefined && (isNaN(container.numberOfDays) || container.numberOfDays < 0)) {
      errors.push(`containers[${index}].numberOfDays (must be non-negative number)`);
    }
    return { index, errors };
  }).filter(item => item.errors.length > 0);
}

// Validate orders array items
function validateOrders(orders) {
  return orders.map((order, index) => {
    const errors = [];
    if (order.quantity === undefined || order.quantity < 1 || isNaN(order.quantity)) {
      errors.push(`orders[${index}].quantity (must be positive integer)`);
    }
    if (order.price === undefined || order.price < 0 || isNaN(order.price)) {
      errors.push(`orders[${index}].price (must be non-negative number)`);
    }
    return { index, errors };
  }).filter(item => item.errors.length > 0);
}

// Helper for status color mapping (for dynamic options)
function getStatusColor(status) {
  const colors = {
    'Draft': 'info',
    'Submitted': 'warning',
    'In Transit': 'warning',
    'Delivered': 'success',
    'Cancelled': 'error'
  };
  return colors[status] || 'default';
}

export async function getStatuses(req, res) {
  try {
    // Updated SQL query aligned with consignment status flow
    // Derives based on workflow states, defaults to 'Draft' if no matching
    const query = `
      SELECT DISTINCT status AS value, status AS label 
      FROM consignments
      WHERE status IS NOT NULL
      ORDER BY status
    `;
    
    const result = await pool.query(query);
    let statuses = result.rows.map(row => ({
      ...row,
      color: getStatusColor(row.value)
    }));

    // Hardcode full list if query returns < 5 (or always, for consistency)
    if (statuses.length < 5) {
      statuses = [
        { value: 'Draft', label: 'Draft', color: getStatusColor('Draft') },
        { value: 'Submitted', label: 'Submitted', color: getStatusColor('Submitted') },
        { value: 'In Transit', label: 'In Transit', color: getStatusColor('In Transit') },
        { value: 'Delivered', label: 'Delivered', color: getStatusColor('Delivered') },
        { value: 'Cancelled', label: 'Cancelled', color: getStatusColor('Cancelled') }
      ];
    }

    // Remove duplicates if mixing dynamic + hardcoded
    statuses = statuses.filter((s, index, self) => 
      index === self.findIndex(t => t.value === s.value)
    );

    res.json(statuses);
  } catch (err) {
    console.error("Error fetching statuses:", err);
    res.status(500).json({ error: 'Failed to fetch statuses' });
  }
}

// Additional exported functions for full CRUD (extend as needed for routes)
export async function getConsignments(req, res) {
  try {
    const { page = 1, limit = 10, status } = req.query;
    const offset = (page - 1) * limit;
    let query = `
      SELECT id, consignment_number, status, remarks, shipper_name, consignee_name, 
             origin, destination, eform, eform_date, consignment_value, currency_code,
             payment_type, vessel, eta, voyage, shipping_line, delivered, pending,
             seal_no, net_weight, gross_weight, created_at, updated_at
      FROM consignments
      WHERE status = 1  -- Active only; adjust as needed
      ORDER BY created_at DESC
      LIMIT $1 OFFSET $2
    `;
    const params = [limit, offset];

    if (status) {
      query = query.replace('WHERE status = 1', `WHERE status = 1 AND status = $${params.length + 1}`);
      params.push(status);
    }

    const { rows } = await pool.query(query, params);

    // Count for pagination
    const countQuery = `
      SELECT COUNT(*) as total
      FROM consignments
      WHERE status = 1 ${status ? `AND status = $${params.length}` : ''}
    `;
    const countParams = status ? [status] : [];
    const countResult = await pool.query(countQuery, countParams);
    const total = parseInt(countResult.rows[0].total);

    res.json({ data: rows, pagination: { page: parseInt(page), limit: parseInt(limit), total, pages: Math.ceil(total / limit) } });
  } catch (err) {
    console.error("Error fetching consignments:", err);
    res.status(500).json({ error: 'Failed to fetch consignments' });
  }
}

export async function getConsignmentById(req, res) {
  try {
    const { id } = req.params;
    const query = `
      SELECT * FROM consignments WHERE id = $1
    `;
    const { rows } = await pool.query(query, [id]);

    if (rows.length === 0) {
      return res.status(404).json({ error: 'Consignment not found' });
    }

    // Enhance with colors
    const consignment = rows[0];
    consignment.statusColor = getStatusColor(consignment.status);

    res.json({ data: consignment });
  } catch (err) {
    console.error("Error fetching consignment:", err);
    res.status(500).json({ error: 'Failed to fetch consignment' });
  }
}

export async function createConsignment(req, res) {
    console.log("Create Consignment Request Body:", req.body);
  try {
    const data = req.body;
    const validationErrors = validateConsignmentFields(data);
    if (validationErrors.length > 0) {
      return res.status(400).json({ error: 'Validation failed', details: validationErrors });
    }

    const containerErrors = validateContainers(data.containers || []);
    const orderErrors = validateOrders(data.orders || []);
    if (containerErrors.length > 0 || orderErrors.length > 0) {
      return res.status(400).json({ error: 'Array validation failed', details: [...containerErrors, ...orderErrors] });
    }

    // Normalize dates
    const normalizedData = {
      ...data,
      eformDate: normalizeDate(data.eformDate),
      eta: normalizeDate(data.eta)
    };

    // Dynamic insert (similar to previous backend)
    const fields = Object.keys(normalizedData).map(key => key.replace(/([A-Z])/g, '_$1').toLowerCase());
    const values = Object.values(normalizedData);
    const placeholders = fields.map((_, i) => `$${i + 1}`).join(', ');
    const query = `INSERT INTO consignments (${fields.join(', ')}) VALUES (${placeholders}) RETURNING *`;

    const result = await pool.query(query, values);
    res.status(201).json({ message: 'Consignment created', data: result.rows[0] });
  } catch (err) {
    console.error("Error creating consignment:", err);
    res.status(500).json({ error: 'Failed to create consignment' });
  }
}

export async function updateConsignment(req, res) {
  try {
    const { id } = req.params;
    const data = req.body;
    const validationErrors = validateConsignmentFields({ ...data, id });  // Include ID if needed
    if (validationErrors.length > 0) {
      return res.status(400).json({ error: 'Validation failed', details: validationErrors });
    }

    // Similar array validations...
    const containerErrors = validateContainers(data.containers || []);
    const orderErrors = validateOrders(data.orders || []);
    if (containerErrors.length > 0 || orderErrors.length > 0) {
      return res.status(400).json({ error: 'Array validation failed', details: [...containerErrors, ...orderErrors] });
    }

    // Normalize dates
    const normalizedData = {
      ...data,
      eformDate: normalizeDate(data.eformDate),
      eta: normalizeDate(data.eta)
    };

    // Dynamic update
    const updates = Object.keys(normalizedData).map(key => {
      const dbKey = key.replace(/([A-Z])/g, '_$1').toLowerCase();
      return `${dbKey} = $${Object.keys(normalizedData).indexOf(key) + 1}`;
    }).join(', ');
    const values = Object.values(normalizedData);
    values.push(id);
    const query = `UPDATE consignments SET ${updates}, updated_at = CURRENT_TIMESTAMP WHERE id = $${values.length} RETURNING *`;

    const result = await pool.query(query, values);
    if (result.rowCount === 0) {
      return res.status(404).json({ error: 'Consignment not found' });
    }

    res.json({ message: 'Consignment updated', data: result.rows[0] });
  } catch (err) {
    console.error("Error updating consignment:", err);
    res.status(500).json({ error: 'Failed to update consignment' });
  }
}

export async function advanceStatus(req, res) {
  try {
    const { id } = req.params;
    const { rows } = await pool.query('SELECT status FROM consignments WHERE id = $1', [id]);
    if (rows.length === 0) {
      return res.status(404).json({ error: 'Consignment not found' });
    }

    const currentStatus = rows[0].status;
    const nextStatusMap = {
      'Draft': 'Submitted',
      'Submitted': 'In Transit',
      'In Transit': 'Delivered',
      'Delivered': null,
      'Cancelled': null
    };
    const nextStatus = nextStatusMap[currentStatus];
    if (!nextStatus) {
      return res.status(400).json({ error: 'No next status available' });
    }

    await pool.query('UPDATE consignments SET status = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2', [nextStatus, id]);
    res.json({ message: `Status advanced to ${nextStatus}` });
  } catch (err) {
    console.error("Error advancing status:", err);
    res.status(500).json({ error: 'Failed to advance status' });
  }
}

export async function deleteConsignment(req, res) {
  try {
    const { id } = req.params;
    const result = await pool.query('DELETE FROM consignments WHERE id = $1 RETURNING id', [id]);
    if (result.rowCount === 0) {
      return res.status(404).json({ error: 'Consignment not found' });
    }
    res.json({ message: 'Consignment deleted' });
  } catch (err) {
    console.error("Error deleting consignment:", err);
    res.status(500).json({ error: 'Failed to delete consignment' });
  }
}