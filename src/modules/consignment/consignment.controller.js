import pool from "../../db/pool.js";


// Helper: Normalize date to ISO string or null
function normalizeDate(dateString) {
  if (!dateString) return null;
  const normalized = dateString.toString().split('T')[0];
  return isValidDate(normalized) ? normalized : null;
}

function isValidDate(dateString) {
  if (!dateString) return false;
  const normalized = dateString.toString().split('T')[0];  // Strip time if full ISO
  const date = new Date(normalized);
  return !isNaN(date.getTime()) && normalized.match(/^\d{4}-\d{2}-\d{2}$/);
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

// Validate orders array items (fixed: assume orders are objects with quantity)
function validateOrders(orders) {
  console.log('Validating orders:', orders);  
  return orders.map((order, index) => {
    const errors = [];
    if (!order || typeof order !== 'object') {
      errors.push(`orders[${index}]: Must be an object`);
    } else if (order.quantity === undefined || order.quantity <= 0 || !Number.isInteger(order.quantity)) {
      errors.push(`orders[${index}].quantity (must be positive integer)`);
    }
    // Add more order-specific validations if needed (e.g., order.id, order.status)
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
    'Cancelled': 'error',
    'Created': 'info'
  };
  return colors[status] || 'default';
}

// Helper: Calculate ETA based on status and eta_config (updated with client and baseDate)
async function calculateETA(client, status, baseDate = new Date()) {
  try {
    const configQuery = `SELECT days_offset FROM eta_config WHERE status = $1`;
    const configResult = await client.query(configQuery, [status]);
    if (configResult.rowCount === 0) {
      console.log(`No ETA config for status: ${status}; using baseDate`);
      return baseDate.toISOString().split('T')[0]; // YYYY-MM-DD
    }
    const days = configResult.rows[0].days_offset;
    if (status.toLowerCase().includes('delivered')) return baseDate.toISOString().split('T')[0]; // No offset
    const etaDate = new Date(baseDate.getTime() + days * 86400000); // ms per day
    return etaDate.toISOString().split('T')[0];
  } catch (err) {
    console.error('ETA calc error:', err);
    return new Date().toISOString().split('T')[0];
  }
}

// Helper: Send notification (placeholder—integrate with your GAS/notifications module)
async function sendNotification(consignmentData, event = 'created') {
  // e.g., await emailService.send({ to: consignmentData.consignee.email, subject: `Consignment ${consignmentData.consignment_number} ${event}` });
  console.log(`Notification sent for consignment ${consignmentData.consignment_number}: ${event}`);
}

// Helper: Log to tracking table
async function logToTracking(client, consignmentId, action, details) {
  await client.query(
    'INSERT INTO consignment_tracking (consignment_id, action, details, created_at) VALUES ($1, $2, $3, NOW())',
    [consignmentId, action, JSON.stringify(details)]
  );
}

// Helper: Wrap in transaction
async function withTransaction(operation) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await operation(client);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
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
        { value: 'Cancelled', label: 'Cancelled', color: getStatusColor('Cancelled') },
        { value: 'Created', label: 'Created', color: getStatusColor('Created') }
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
  console.log('asasa',res)
  try {
    const { id } = req.params;
    const query = `
      SELECT * FROM consignments WHERE id = $1
    `;
    const { rows } = await pool.query(query, [id]);

    if (rows.length === 0) {
      return res.status(404).json({ error: 'Consignment not found' });
    }

    // Enhance with colors and computations
    const consignment = rows[0];
    consignment.statusColor = getStatusColor(consignment.status);

    // Compute days_until_eta using current date (Dec 05, 2025, as per context)
    if (consignment.eta) {
      const etaDate = new Date(consignment.eta);
      const today = new Date('2025-12-05');  // Fixed current date
      consignment.days_until_eta = Math.max(0, Math.ceil((etaDate - today) / (1000 * 60 * 60 * 24)));
    }

    // Fetch linked orders for alignment/computations (assume orders table has consignment_id FK)
    const orderQuery = `
      SELECT id, sender_name AS shipper, receiver_name AS consignee, eta, etd, 
             qty_delivered AS delivered, total_assigned_qty, status AS order_status
      FROM orders o 
      WHERE o.id = ANY($1::int[])  -- Use consignment.orders as array of IDs
    `;
    let orderIds = typeof consignment.orders === 'string' ? JSON.parse(consignment.orders) : consignment.orders;
    if (orderIds.length > 0) {
      const { rows: linkedOrders } = await pool.query(orderQuery, [orderIds]);
      if (linkedOrders.length > 0) {
        // Sync from first order (or aggregate)
        const firstOrder = linkedOrders[0];
        consignment.shipper = firstOrder.shipper || consignment.shipper;
        consignment.consignee = firstOrder.consignee || consignment.consignee;
        consignment.status = firstOrder.order_status || consignment.status;  // Sync status
        consignment.etd = firstOrder.etd ? normalizeDate(firstOrder.etd) + 'T00:00:00.000Z' : null;

        // Compute delivered/pending from orders (removed gross_weight aggregation due to missing column)
        const totalDelivered = linkedOrders.reduce((sum, o) => sum + (o.delivered || 0), 0);
        const totalPending = linkedOrders.reduce((sum, o) => sum + (o.total_assigned_qty || 0), 0);
        consignment.delivered = totalDelivered;
        consignment.pending = totalPending - totalDelivered;

        // Update orders array to full objects if needed
        consignment.orders = linkedOrders.map(o => ({ id: o.id, ...o }));  // Expand
      }
    }

    // Dynamic status update based on ETA (after syncing from orders)
    if (consignment.eta && consignment.days_until_eta !== undefined) {
      let originalStatus = consignment.status;
      if (consignment.days_until_eta <= 0) {
        consignment.status = 'Delivered';
      } else if (consignment.days_until_eta <= 7) {
        consignment.status = 'In Transit';
      } else if (consignment.days_until_eta <= 30) {
        consignment.status = 'Submitted';
      }
      // Re-apply color after potential update
      consignment.statusColor = getStatusColor(consignment.status);

      // Optional: Persist update to DB if status changed (uncomment if needed)
      // if (consignment.status !== originalStatus) {
      //   await pool.query('UPDATE consignments SET status = $1, updated_at = NOW() WHERE id = $2', [consignment.status, id]);
      //   await logToTracking(pool, id, 'status_updated', { 
      //     newStatus: consignment.status, 
      //     days_until_eta: consignment.days_until_eta,
      //     originalStatus 
      //   });
      // }
    }

    // Enrich containers: Parse and optionally fetch full details
    if (typeof consignment.containers === 'string') {
      consignment.containers = JSON.parse(consignment.containers);
    }
    // Add from linked orders (flatten nested) - example; expand with actual query if needed
    // linkedOrders.forEach(order => { /* flatten receivers.containers */ });

    // Stringify shipping_line if ID (lookup via join)
    if (typeof consignment.shipping_line === 'number' && consignment.shipping_line > 0) {
      const lineQuery = 'SELECT name FROM shipping_lines WHERE id = $1';
      const { rows: lines } = await pool.query(lineQuery, [consignment.shipping_line]);
      consignment.shipping_line = lines[0]?.name || consignment.shipping_line;
    }

    // Remove null status_color (if any)
    if (consignment.status_color === null || consignment.status_color === undefined) {
      delete consignment.status_color;
    }

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
    // Skip orders validation as IDs are sent directly; handle as array of numbers or objects
    const orderErrors = []; // No validation needed for order IDs
    if (containerErrors.length > 0 || orderErrors.length > 0) {
      return res.status(400).json({ error: 'Array validation failed', details: [...containerErrors, ...orderErrors] });
    }
    // Auto-calculate ETA if missing (now uses transaction client)
    let computedETA = normalizedInput.eta;
    if (!computedETA) {
      // Will be computed inside transaction
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
      eta: normalizeDate(computedETA), // Placeholder; set in tx
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
    // Dynamic insert (keys already snake_case; converter is redundant but safe)
    const fields = Object.keys(normalizedData).map(key => key.replace(/([A-Z])/g, '_$1').toLowerCase());
    const values = Object.values(normalizedData);
    const placeholders = fields.map((_, i) => `$${i + 1}`).join(', ');
    const insertQuery = `INSERT INTO consignments (${fields.join(', ')}) VALUES (${placeholders}) RETURNING id, *`;
    let newConsignment;
    await withTransaction(async (client) => {
      // Auto-calculate ETA inside transaction
      computedETA = await calculateETA(client, normalizedInput.status);
      // Update eta in normalizedData (but since insert already done? Rebuild values or use UPDATE)
      // Better: Insert without eta, then update
      const tempFields = fields.filter(f => f !== 'eta');
      const tempValues = values.filter((_, i) => fields[i] !== 'eta');
      const tempPlaceholders = tempFields.map((_, i) => `$${i + 1}`).join(', ');
      const tempInsertQuery = `INSERT INTO consignments (${tempFields.join(', ')}) VALUES (${tempPlaceholders}) RETURNING id`;
      const tempResult = await client.query(tempInsertQuery, tempValues);
      const tempId = tempResult.rows[0].id;
      // Update with computed ETA
      await client.query(
        'UPDATE consignments SET eta = $1 WHERE id = $2',
        [normalizeDate(computedETA), tempId]
      );
      // Fetch full newConsignment
      const fullQuery = `SELECT * FROM consignments WHERE id = $1`;
      const fullResult = await client.query(fullQuery, [tempId]);
      newConsignment = fullResult.rows[0];
      // Cascade: Update linked orders/containers (example: set consignment_id FK) - skipped as columns may not exist yet
      // let orderIds = normalizedInput.orders.map(o => typeof o === 'object' ? o.id : o).filter(Boolean);
      // if (orderIds.length > 0) {
      //   await client.query(
      //     'UPDATE orders SET consignment_id = $1 WHERE id = ANY($2::int[])',
      //     [newConsignment.id, orderIds]
      //   );
      // }
      // let containerIds = normalizedInput.containers.map(c => c.id).filter(Boolean);
      // if (containerIds.length > 0) {
      //   await client.query(
      //     'UPDATE containers SET consignment_id = $1 WHERE id = ANY($2::int[])',
      //     [newConsignment.id, containerIds]
      //   );
      // }
    });
    // After transaction commits, handle optional logging and notifications outside to avoid aborting main tx
    try {
      // Log to tracking - use pool or separate client, not the tx client
      await logToTracking(null, newConsignment.id, 'created', { status: normalizedInput.status, eta: computedETA }); // Adjust logToTracking to handle no client
    } catch (trackingErr) {
      console.warn("Tracking log failed:", trackingErr.message);
      // Continue without failing
    }
    // Send notification if status triggers (e.g., 'Created' or 'Submitted') - assume sendNotification doesn't require tx
    if (['Created', 'Submitted'].includes(normalizedInput.status)) {
      try {
        await sendNotification(newConsignment, 'created');
      } catch (notifErr) {
        console.warn("Notification send failed:", notifErr.message);
      }
    }
    // Enhance response with computed fields
    newConsignment.statusColor = getStatusColor(newConsignment.status);
    console.log("Consignment created with ID:", newConsignment.id);
    res.status(201).json({ message: 'Consignment created successfully', data: newConsignment });
  } catch (err) {
    console.error("Error creating consignment:", err);
    if (err.code === '23505') { // Unique violation (e.g., duplicate consignment_number)
      return res.status(409).json({ error: 'Consignment number already exists' });
    }
    res.status(500).json({ error: 'Failed to create consignment', details: err.message });
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

    // Auto-calculate ETA if missing or if status changed (recompute)
    let computedETA = normalizedInput.eta;
    if (!computedETA || data.status !== undefined) { // Recompute if status provided
      // Will be computed inside transaction
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
      eta: normalizeDate(computedETA),
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

    let updatedConsignment;
    await withTransaction(async (client) => {
      // Auto-calculate ETA inside transaction if needed
      if (!computedETA || data.status !== undefined) {
        computedETA = await calculateETA(client, normalizedInput.status || data.status);
        // Update eta in values (find index and replace)
        const etaIndex = updateFields.findIndex(f => f === 'eta');
        if (etaIndex !== -1) {
          values[etaIndex + 1] = normalizeDate(computedETA);  // +1 for id
        }
      }

      // Update consignment
      const updateResult = await client.query(query, values);
      if (updateResult.rowCount === 0) {
        throw new Error('Consignment not found');
      }
      updatedConsignment = updateResult.rows[0];

      // Log to tracking if status changed
      if (data.status) {
        await logToTracking(client, id, 'updated', { newStatus: normalizedInput.status, eta: computedETA });
      }

      // Cascade: Update linked orders/containers if arrays changed
      let orderIds = normalizedInput.orders.map(o => o.id).filter(Boolean);
      if (orderIds.length > 0) {
        await client.query(
          'UPDATE orders SET consignment_id = $1 WHERE id = ANY($2::int[])',
          [id, orderIds]
        );
      }
      let containerIds = normalizedInput.containers.map(c => c.id).filter(Boolean);
      if (containerIds.length > 0) {
        await client.query(
          'UPDATE containers SET consignment_id = $1 WHERE id = ANY($2::int[])',
          [id, containerIds]
        );
      }

      // Send notification if status updated to trigger (e.g., 'In Transit' or 'Delivered')
      if (['In Transit', 'Delivered'].includes(normalizedInput.status)) {
        await sendNotification(updatedConsignment, 'updated');
      }
    });

    // Compute and add client-side fields to response (e.g., statusColor based on status)
    const responseData = {
      ...updatedConsignment,
      statusColor: getStatusColor(updatedConsignment.status), // Assume you have a function getStatusColor
      shipperAddress: updatedConsignment.shipper_address || '',
      consigneeAddress: updatedConsignment.consignee_address || '',
      paymentType: updatedConsignment.payment_type || '',
      shippingLine: updatedConsignment.shipping_line || null, // Fixed: Treat as string, not int
      netWeight: updatedConsignment.net_weight || '0.00'
      // Add other computed fields as needed
    };

    console.log("Consignment updated with ID:", updatedConsignment.id);
    res.status(200).json({ message: 'Consignment updated', data: responseData });
  } catch (err) {
    console.error("Error updating consignment:", err);
    if (err.message === 'Consignment not found') {
      return res.status(404).json({ error: 'Consignment not found' });
    }
    if (err.code === '23505') { // Unique violation
      return res.status(409).json({ error: 'Consignment number already exists' });
    }
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
      'Cancelled': null,
      'Created': 'Submitted'
    };
    const nextStatus = nextStatusMap[currentStatus];
    if (!nextStatus) {
      return res.status(400).json({ error: 'No next status available' });
    }

    await withTransaction(async (client) => {
      await client.query('UPDATE consignments SET status = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2', [nextStatus, id]);
      
      // Log the advance
      await logToTracking(client, id, 'status_advanced', { from: currentStatus, to: nextStatus });

      // Optional: Recalculate ETA based on new status
      const eta = await calculateETA(client, nextStatus);
      await client.query('UPDATE consignments SET eta = $1 WHERE id = $2', [normalizeDate(eta), id]);

      // Send notification
      const updated = await client.query('SELECT * FROM consignments WHERE id = $1', [id]);
      await sendNotification(updated.rows[0], `status_advanced_to_${nextStatus}`);
    });

    res.json({ message: `Status advanced to ${nextStatus}` });
  } catch (err) {
    console.error("Error advancing status:", err);
    res.status(500).json({ error: 'Failed to advance status' });
  }
}

export async function deleteConsignment(req, res) {
  try {
    const { id } = req.params;
    await withTransaction(async (client) => {
      // Log deletion
      await logToTracking(client, id, 'deleted', { reason: 'user_request' });

      const result = await client.query('DELETE FROM consignments WHERE id = $1 RETURNING id', [id]);
      if (result.rowCount === 0) {
        throw new Error('Consignment not found');
      }
    });
    res.json({ message: 'Consignment deleted' });
  } catch (err) {
    console.error("Error deleting consignment:", err);
    if (err.message === 'Consignment not found') {
      return res.status(404).json({ error: 'Consignment not found' });
    }
    res.status(500).json({ error: 'Failed to delete consignment' });
  }
}