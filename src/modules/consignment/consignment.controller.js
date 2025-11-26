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
  consignment_number, status, remarks, shipper, consignee, origin, destination,
  eform, eform_date, bank, consignment_value, paymentType, vessel, voyage,
  eta, shippingLine, seal_no, netWeight, gross_weight, containers, orders
}) {
  const errors = [];
  if (!consignment_number) errors.push('consignment_number');
  if (!status) errors.push('status');
  if (!shipper) errors.push('shipper');
  if (!consignee) errors.push('consignee');
  if (!origin) errors.push('origin');
  if (!destination) errors.push('destination');
  if (!eform || !eform.match(/^[A-Z]{3}-\d{6}$/)) errors.push(`eform (invalid format, got: "${eform}")`);
  if (!isValidDate(eform_date)) errors.push(`eform_date (got: "${eform_date}")`);
  // if (!bank) errors.push('bank');
  if (consignment_value === undefined || consignment_value < 0 || isNaN(consignment_value)) errors.push('consignment_value (must be non-negative number)');
  if (!paymentType) errors.push('paymentType');
  // if (!vessel) errors.push('vessel');
  if (!voyage || voyage.length < 3) errors.push(`voyage (min 3 chars, got: "${voyage}")`);
  if (eta && !isValidDate(eta)) errors.push(`eta (got: "${eta}")`);
  if (!shippingLine) errors.push('shippingLine');  // Optional? Adjust if needed
  if (seal_no && seal_no.length < 3) errors.push(`seal_no (min 3 chars, got: "${seal_no}")`);  // Optional validation
  if (netWeight === undefined || netWeight < 0 || isNaN(netWeight)) errors.push('netWeight (must be non-negative number)');
  if (gross_weight === undefined || gross_weight < 0 || isNaN(gross_weight)) errors.push('gross_weight (must be non-negative number)');
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
  console.log('Validating orders:', orders);  
  return orders.map((order, index) => {
    const errors = [];
    if (order === undefined || order <= 0) {
      errors.push(`orders[${index}].quantity (must be positive integer)`);
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
    const {
      page = 1,
      limit = 10,
      order_by = 'created_at',
      order = 'desc',
      consignment_id = '',
      status = ''
    } = req.query;

    const offset = (parseInt(page) - 1) * parseInt(limit);
    const validOrderBys = [
      'id', 'consignment_number', 'status', 's.name', 'c.name',  // Updated: Use joined aliases for sorting
      'eta', 'created_at', 'gross_weight', 'delivered', 'pending'
    ];
    const safeOrderBy = validOrderBys.includes(order_by) ? 
      (order_by === 'shipper' ? 's.name' : order_by === 'consignee' ? 'c.name' : order_by) : 
      'created_at';  // Map frontend keys to joined fields
    const safeOrder = order.toLowerCase() === 'asc' ? 'ASC' : 'DESC';

    let baseQuery = `
      SELECT 
        cons.id, 
        cons.consignment_number, 
        cons.status, 
        COALESCE(s.name, cons.shipper) AS shipper,  -- Prefer joined name, fallback to denormalized
        COALESCE(c.name, cons.consignee) AS consignee, 
        cons.eta, 
        cons.created_at,
        cons.gross_weight, 
        cons.orders, 
        cons.delivered, 
        cons.pending
      FROM consignments cons
      LEFT JOIN shippers s ON cons.shipper_id = s.id  -- LEFT JOIN to handle missing refs
      LEFT JOIN consignees c ON cons.consignee_id = c.id
    `;
    let whereClauses = [];
    let queryParams = [];

    if (consignment_id.trim()) {
      whereClauses.push(`cons.consignment_number ILIKE $${queryParams.length + 1}`);
      queryParams.push(`%${consignment_id.trim()}%`);
    }

    if (status.trim()) {
      whereClauses.push(`cons.status = $${queryParams.length + 1}`);
      queryParams.push(status.trim());
    }

    let whereClause = '';
    if (whereClauses.length > 0) {
      whereClause = ` WHERE ${whereClauses.join(' AND ')}`;
    }

    const orderByClause = ` ORDER BY ${safeOrderBy} ${safeOrder}`;
    const limitOffset = ` LIMIT $${queryParams.length + 1} OFFSET $${queryParams.length + 2}`;
    queryParams.push(parseInt(limit), offset);

    const fullQuery = baseQuery + whereClause + orderByClause + limitOffset;

    const { rows } = await pool.query(fullQuery, queryParams);

    // Count for pagination (reuse where clause params, no JOIN needed for count)
    const countQuery = `
      SELECT COUNT(*) as total
      FROM consignments cons
      ${whereClause}
    `;
    const countResult = await pool.query(countQuery, queryParams.slice(0, -2)); // Exclude limit/offset
    const total = parseInt(countResult.rows[0].total);

    res.json({ data: rows, total });
  } catch (err) {
    console.error("Error fetching consignments:", err);
    res.status(500).json({ error: 'Failed to fetch consignments' });
  }
}
export async function getConsignmentById(req, res) {
  // console.log('asasa',req,res)
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
    
    // Map mixed-case input to consistent snake_case for validation and DB
    const normalizedInput = {
      consignment_number: data.consignment_number || data.consignmentNumber,
      status: data.status,
      remarks: data.remarks,
      shipper: data.shipper,
      consignee: data.consignee,
      origin: data.origin,
      destination: data.destination,
      eform: data.eform,
      eform_date: data.eform_date || data.eformDate,
      bank: data.bank,
      consignment_value: data.consignment_value || data.consignmentValue,
      paymentType: data.paymentType || data.payment_type,
      vessel: data.vessel,
      voyage: data.voyage,
      eta: data.eta,
      shippingLine: data.shippingLine || data.shipping_line,
      seal_no: data.seal_no || data.sealNo,
      netWeight: data.netWeight || data.net_weight,
      gross_weight: data.gross_weight || data.grossWeight,
      currency_code: data.currency_code || data.currencyCode,
      delivered: data.delivered || 0,
      pending: data.pending || 0,
      containers: data.containers || [],
      orders: data.orders || []
    };

    const validationErrors = validateConsignmentFields(normalizedInput);
    if (validationErrors.length > 0) {
      return res.status(400).json({ error: 'Validation failed', details: validationErrors });
    }

    const containerErrors = validateContainers(normalizedInput.containers);
    const orderErrors = validateOrders(normalizedInput.orders);
    if (containerErrors.length > 0 || orderErrors.length > 0) {
      return res.status(400).json({ error: 'Array validation failed', details: [...containerErrors, ...orderErrors] });
    }

    // Normalize dates and stringify JSON fields - use snake_case keys
    const normalizedData = {
      consignment_number: normalizedInput.consignment_number,
      status: normalizedInput.status,
      remarks: normalizedInput.remarks,
      shipper: normalizedInput.shipper,
      consignee: normalizedInput.consignee,
      origin: normalizedInput.origin,
      destination: normalizedInput.destination,
      eform: normalizedInput.eform,
      eform_date: normalizeDate(normalizedInput.eform_date),
      bank: normalizedInput.bank,
      consignment_value: normalizedInput.consignment_value,
      payment_type: normalizedInput.paymentType,
      vessel: normalizedInput.vessel,
      voyage: normalizedInput.voyage,
      eta: normalizeDate(normalizedInput.eta),
      shipping_line: normalizedInput.shippingLine,
      seal_no: normalizedInput.seal_no,
      net_weight: normalizedInput.netWeight,
      gross_weight: normalizedInput.gross_weight,
      currency_code: normalizedInput.currency_code,
      delivered: normalizedInput.delivered,
      pending: normalizedInput.pending,
      containers: JSON.stringify(normalizedInput.containers),
      orders: JSON.stringify(normalizedInput.orders)
    };

    // Dynamic insert (snake_case already, but keep the converter for safety)
    const fields = Object.keys(normalizedData).map(key => key.replace(/([A-Z])/g, '_$1').toLowerCase());
    const values = Object.values(normalizedData);
    const placeholders = fields.map((_, i) => `$${i + 1}`).join(', ');
    const query = `INSERT INTO consignments (${fields.join(', ')}) VALUES (${placeholders}) RETURNING *`;

    const result = await pool.query(query, values);
    console.log("Consignment created with ID:", result);
    res.status(201).json({ message: 'Consignment created', data: result.rows[0] });
  } catch (err) {
    console.error("Error creating consignment:", err);
    res.status(500).json({ error: 'Failed to create consignment' });
  }
}
export async function updateConsignment(req, res) {
  const { id } = req.params; // Assume ID from params
  // console.log("Update Consignment Request Body:", req.body);
  try {
    const data = req.body;
    
    // Map mixed-case input to consistent snake_case for validation and DB
    // Exclude non-DB/computed fields like status_color, created_at, updated_at
    const normalizedInput = {
      consignment_number: data.consignment_number || data.consignmentNumber,
      status: data.status,
      remarks: data.remarks,
      shipper: data.shipper,
      shipper_address: data.shipper_address || data.shipperAddress,
      consignee: data.consignee,
      consignee_address: data.consignee_address || data.consigneeAddress,
      origin: data.origin,
      destination: data.destination,
      eform: data.eform,
      eform_date: data.eform_date || data.eformDate,
      bank: data.bank,
      consignment_value: data.consignment_value || data.consignmentValue,
      paymentType: data.paymentType || data.payment_type,
      vessel: data.vessel,
      voyage: data.voyage,
      eta: data.eta,
      shippingLine: data.shippingLine || data.shipping_line,
      seal_no: data.seal_no || data.sealNo,
      netWeight: data.netWeight || data.net_weight,
      gross_weight: data.gross_weight || data.grossWeight,
      currency_code: data.currency_code || data.currencyCode,
      delivered: data.delivered || 0,
      pending: data.pending || 0,
      containers: data.containers || [],
      orders: data.orders || []
      // Do NOT include status_color, created_at, or updated_at here
    };

    const validationErrors = validateConsignmentFields(normalizedInput);
    if (validationErrors.length > 0) {
      return res.status(400).json({ error: 'Validation failed', details: validationErrors });
    }

    const containerErrors = validateContainers(normalizedInput.containers);
    const orderErrors = validateOrders(normalizedInput.orders);
    if (containerErrors.length > 0 || orderErrors.length > 0) {
      return res.status(400).json({ error: 'Array validation failed', details: [...containerErrors, ...orderErrors] });
    }

    // Normalize dates and stringify JSON fields - use snake_case keys only
    // Explicitly define to avoid any inheritance or duplicates
    const normalizedData = {
      consignment_number: normalizedInput.consignment_number,
      status: normalizedInput.status,
      remarks: normalizedInput.remarks,
      shipper: normalizedInput.shipper,
      shipper_address: normalizedInput.shipper_address,
      consignee: normalizedInput.consignee,
      consignee_address: normalizedInput.consignee_address,
      origin: normalizedInput.origin,
      destination: normalizedInput.destination,
      eform: normalizedInput.eform,
      eform_date: normalizeDate(normalizedInput.eform_date),
      bank: normalizedInput.bank,
      consignment_value: normalizedInput.consignment_value,
      payment_type: normalizedInput.paymentType,
      vessel: normalizedInput.vessel,
      voyage: normalizedInput.voyage,
      eta: normalizeDate(normalizedInput.eta),
      shipping_line: normalizedInput.shippingLine,
      seal_no: normalizedInput.seal_no,
      net_weight: normalizedInput.netWeight,
      gross_weight: normalizedInput.gross_weight,
      currency_code: normalizedInput.currency_code,
      delivered: normalizedInput.delivered,
      pending: normalizedInput.pending,
      containers: JSON.stringify(normalizedInput.containers),
      orders: JSON.stringify(normalizedInput.orders)
      // Explicitly NO status_color, created_at, or updated_at
    };

    // Build SET clause carefully: use only keys from normalizedData, skip timestamps and non-DB fields
    const updateFields = Object.keys(normalizedData)
      .filter(key => !['id', 'created_at', 'updated_at', 'status_color'].includes(key)); // Explicit exclude
    const setClauseParts = updateFields.map((key, index) => {
      // Since keys are already snake_case, no need for replace, but keep for safety
      const dbKey = key.replace(/([A-Z])/g, '_$1').toLowerCase();
      return `${dbKey} = $${index + 2}`; // $1 is ID
    });
    const setClause = setClauseParts.join(', ');
    const values = [id, ...updateFields.map(key => normalizedData[key])];
    const query = `UPDATE consignments SET ${setClause}, updated_at = CURRENT_TIMESTAMP WHERE id = $1 RETURNING *`;

    console.log('Update query:', query); // Debug log
    console.log('Update values:', values); // Debug log

    const result = await pool.query(query, values);
    if (result.rowCount === 0) {
      return res.status(404).json({ error: 'Consignment not found' });
    }

    // Compute and add client-side fields to response (e.g., statusColor based on status)
    const responseData = {
      ...result.rows[0],
      statusColor: getStatusColor(result.rows[0].status), // Assume you have a function getStatusColor
      shipperAddress: result.rows[0].shipper_address || '',
      consigneeAddress: result.rows[0].consignee_address || '',
      paymentType: result.rows[0].payment_type || '',
      shippingLine: parseInt(result.rows[0].shipping_line, 10) || null,
      netWeight: result.rows[0].net_weight || '0.00'
      // Add other computed fields as needed
    };

    console.log("Consignment updated with ID:", result.rows[0].id);
    res.status(200).json({ message: 'Consignment updated', data: responseData });
  } catch (err) {
    console.error("Error updating consignment:", err);
    res.status(500).json({ error: 'Failed to update consignment', details: err.message });
  }
}
export async function advanceStatus(req, res) {
  console.log("Advance Status Request Params:", req.params);    
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