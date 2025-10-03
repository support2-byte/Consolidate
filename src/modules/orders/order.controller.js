import pool from "../../db/pool.js";

// Date validation helpers (unchanged)
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

// Updated validation: Distinguish missing vs. invalid; cleaner messages
function validateOrderFields({
  bookingRef, status, eta, etd, placeOfLoading, finalDestination, placeOfDelivery,
  senderName, receiverName, shippingLine
}) {
  const errors = [];
  // Required non-dates: Check if missing
  if (!bookingRef) errors.push('booking_ref required');
  if (!status) errors.push('status required');
  if (!placeOfLoading) errors.push('place_of_loading required');
  if (!finalDestination) errors.push('final_destination required');
  if (!placeOfDelivery) errors.push('place_of_delivery required');
  if (!senderName) errors.push('sender_name required');
  if (!receiverName) errors.push('receiver_name required');
  if (!shippingLine) errors.push('shipping_line required');

  // Required dates: Check if missing or invalid
  if (eta === undefined || eta === null || eta === '') {
    errors.push('eta required');
  } else if (!isValidDate(eta)) {
    errors.push(`eta invalid format (got: "${eta}")`);
  }
  if (etd === undefined || etd === null || etd === '') {
    errors.push('etd required');
  } else if (!isValidDate(etd)) {
    errors.push(`etd invalid format (got: "${etd}")`);
  }

  return errors;
}

// Helper for order status color mapping (unchanged)
function getOrderStatusColor(status) {
  const colors = {
    'Created': 'info',
    'In Transit': 'warning',
    'Delivered': 'success',
    'Cancelled': 'error'
  };
  return colors[status] || 'default';
}

// Map order status to container availability (unchanged)
export function getContainerAvailability(orderStatus) {
  switch (orderStatus) {
    case 'Created': return 'Assigned to Job';
    case 'In Transit': return 'In Transit';
    case 'Delivered': return 'Available';
    case 'Cancelled': return 'Available';
    default: return 'Available';
  }
}

// Create a new order (added body logging; refined validation call)
export async function createOrder(req, res) {
  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');

    const {
      bookingRef,
      status,
      rglBookingNumber,
      consignmentRemarks,
      placeOfLoading,
      finalDestination,
      placeOfDelivery,
      orderRemarks,
      associatedContainer,
      consignmentNumber,
      consignmentVessel,
      consignmentVoyage,
      senderName,
      senderContact,
      senderAddress,
      senderEmail,
      receiverName,
      receiverContact,
      receiverAddress,
      receiverEmail,
      eta,
      etd,
      shippingLine,
      driverName,
      driverContact,
      driverNic,
      driverPickupLocation,
      truckNumber,
      thirdPartyTransport
    } = req.body;

    // Debug: Log req.body for missing fields
    console.log('Order create body (key fields):', { bookingRef, status, eta, etd, shippingLine, placeOfLoading, finalDestination, placeOfDelivery, senderName, receiverName });

    // Handle attachments: store file paths as JSON array
    const attachments = req.files ? req.files.map(file => file.path) : [];

    // Validation (updated function)
    const validationErrors = validateOrderFields({
      bookingRef, status, eta, etd, placeOfLoading, finalDestination, placeOfDelivery,
      senderName, receiverName, shippingLine
    });
    if (validationErrors.length > 0) {
      await client.query('ROLLBACK');
      console.warn('Validation failed for fields:', validationErrors);
      return res.status(400).json({ 
        error: 'Order fields missing or invalid',
        details: validationErrors.join('; ')
      });
    }

    // Normalize dates (now safe since validated)
    const normEta = normalizeDate(eta);
    const normEtd = normalizeDate(etd);

    const query = `
      INSERT INTO orders (
        booking_ref, status, rgl_booking_number, consignment_remarks,
        place_of_loading, final_destination, place_of_delivery, order_remarks,
        associated_container, consignment_number, consignment_vessel, consignment_voyage,
        sender_name, sender_contact, sender_address, sender_email,
        receiver_name, receiver_contact, receiver_address, receiver_email,
        eta, etd, shipping_line,
        driver_name, driver_contact, driver_nic, driver_pickup_location, truck_number,
        third_party_transport, attachments, created_by
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31)
      RETURNING *
    `;

    const values = [
      bookingRef, status, rglBookingNumber, consignmentRemarks,
      placeOfLoading, finalDestination, placeOfDelivery, orderRemarks,
      associatedContainer, consignmentNumber, consignmentVessel, consignmentVoyage,
      senderName, senderContact, senderAddress, senderEmail,
      receiverName, receiverContact, receiverAddress, receiverEmail,
      normEta, normEtd, shippingLine,
      driverName, driverContact, driverNic, driverPickupLocation, truckNumber,
      thirdPartyTransport, JSON.stringify(attachments), 'system'  // Assume system creator
    ];

    const result = await client.query(query, values);
    const newOrder = result.rows[0];

    // If associatedContainer is provided, verify it exists and insert new container status history
    if (associatedContainer) {
      const containerCheck = await client.query(
        'SELECT cid FROM container_master WHERE container_number = $1 AND status = 1',
        [associatedContainer]
      );
      if (containerCheck.rowCount === 0) {
        await client.query('ROLLBACK');
        return res.status(400).json({ error: 'Associated container not found' });
      }
      const cid = containerCheck.rows[0].cid;
      const availability = getContainerAvailability(status);
      await client.query(
        'INSERT INTO container_status (cid, availability, status_notes, created_by) VALUES ($1, $2, $3, $4)',
        [cid, availability, `Assigned to order ${newOrder.id} (${status})`, 'system']
      );
    }

    await client.query('COMMIT');
    console.log("Created new order:", { id: newOrder.id });
    res.status(201).json(newOrder);
  } catch (error) {
    if (client) {
      await client.query('ROLLBACK');
    }
    console.error('Error creating order:', error.message);
    if (error.code === '23505') {
      return res.status(409).json({ error: 'Booking reference already exists' });
    }
    if (error.code === '23514') {
      return res.status(400).json({ error: 'Invalid value for constrained field (e.g., status)' });
    }
    if (error.code === '22007' || error.code === '22008') {
      return res.status(400).json({ error: 'Invalid date format. Use YYYY-MM-DD.' });
    }
    res.status(500).json({ error: 'Failed to create order', details: error.message });
  } finally {
    if (client) {
      client.release();
    }
  }
}

// Update an existing order (refined validation for provided fields only; added body logging)
export async function updateOrder(req, res) {
  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');

    const { id } = req.params;
    const updates = req.body;
    const created_by = updates.created_by || 'system'; // Assume system updater

    // Debug: Log updates for missing fields
    console.log('Order update body (key fields):', { booking_ref: updates.booking_ref, status: updates.status, eta: updates.eta, etd: updates.etd, shipping_line: updates.shipping_line });

    // Fetch current order
    const currentResult = await client.query('SELECT * FROM orders WHERE id = $1', [id]);
    if (currentResult.rowCount === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Order not found' });
    }
    const currentOrder = currentResult.rows[0];

    // Updated validation: Only for provided fields (like container update)
    const updatedFields = {
      bookingRef: updates.booking_ref,
      status: updates.status,
      eta: updates.eta,
      etd: updates.etd,
      placeOfLoading: updates.place_of_loading,
      finalDestination: updates.final_destination,
      placeOfDelivery: updates.place_of_delivery,
      senderName: updates.sender_name,
      receiverName: updates.receiver_name,
      shippingLine: updates.shipping_line,
    };
    const updateErrors = [];
    for (const [field, value] of Object.entries(updatedFields)) {
      if (value !== undefined) {  // Only validate if explicitly provided
        const actualField = field === 'bookingRef' ? 'booking_ref' : 
                           field === 'placeOfLoading' ? 'place_of_loading' :
                           field === 'finalDestination' ? 'final_destination' :
                           field === 'placeOfDelivery' ? 'place_of_delivery' :
                           field === 'senderName' ? 'sender_name' :
                           field === 'receiverName' ? 'receiver_name' :
                           field === 'shippingLine' ? 'shipping_line' : field.toLowerCase();
        if (['eta', 'etd'].includes(field) && !isValidDate(value)) {
          updateErrors.push(`${actualField} invalid format (got: "${value}")`);
        } else if (value === '' || value === null) {  // Empty provided values are invalid for required
          updateErrors.push(`${actualField} cannot be empty`);
        } else if (!value && !['eta', 'etd'].includes(field)) {
          updateErrors.push(`${actualField} required`);
        }
      }
    }

    if (updateErrors.length > 0) {
      console.warn('Update validation failed for fields:', updateErrors);
      await client.query('ROLLBACK');
      return res.status(400).json({ 
        error: 'Invalid update fields',
        details: updateErrors.join('; ')
      });
    }

    // Normalize dates for update (only if provided)
    const normEta = updates.eta ? normalizeDate(updates.eta) : currentOrder.eta;
    const normEtd = updates.etd ? normalizeDate(updates.etd) : currentOrder.etd;

    if ((updates.eta && !normEta) || (updates.etd && !normEtd)) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: 'Normalized dates invalid. Use YYYY-MM-DD.' });
    }

    // If status is being updated, handle container status change (insert new history row)
    if (updates.status && updates.status !== currentOrder.status) {
      if (currentOrder.associated_container) {
        const containerCheck = await client.query(
          'SELECT cid FROM container_master WHERE container_number = $1 AND status = 1',
          [currentOrder.associated_container]
        );
        if (containerCheck.rowCount > 0) {
          const cid = containerCheck.rows[0].cid;
          const newAvailability = getContainerAvailability(updates.status);
          await client.query(
            'INSERT INTO container_status (cid, availability, status_notes, created_by) VALUES ($1, $2, $3, $4)',
            [cid, newAvailability, `Updated via order ${id} status to ${updates.status}`, created_by]
          );
        }
      }
    }

    // If associated_container is being updated (assigned/unassigned)
    if (updates.associated_container !== undefined && updates.associated_container !== currentOrder.associated_container) {
      // Unassign from old container if exists (insert 'Available' history)
      if (currentOrder.associated_container) {
        const oldContainerCheck = await client.query(
          'SELECT cid FROM container_master WHERE container_number = $1 AND status = 1',
          [currentOrder.associated_container]
        );
        if (oldContainerCheck.rowCount > 0) {
          const oldCid = oldContainerCheck.rows[0].cid;
          await client.query(
            'INSERT INTO container_status (cid, availability, status_notes, created_by) VALUES ($1, $2, $3, $4)',
            [oldCid, 'Available', `Unassigned from order ${id}`, created_by]
          );
        }
      }

      // Assign to new container if provided (insert new availability history)
      if (updates.associated_container) {
        const newContainerCheck = await client.query(
          'SELECT cid FROM container_master WHERE container_number = $1 AND status = 1',
          [updates.associated_container]
        );
        if (newContainerCheck.rowCount === 0) {
          await client.query('ROLLBACK');
          return res.status(400).json({ error: 'New associated container not found' });
        }
        const newCid = newContainerCheck.rows[0].cid;
        const availability = getContainerAvailability(updates.status || currentOrder.status);
        await client.query(
          'INSERT INTO container_status (cid, availability, status_notes, created_by) VALUES ($1, $2, $3, $4)',
          [newCid, availability, `Assigned to order ${id} (${updates.status || currentOrder.status})`, created_by]
        );
      }
    }

    // Update order fields dynamically (with normalized dates)
    const updateFields = Object.keys(updates).filter(key => key !== 'id');
    if (updateFields.length > 0) {
      // Normalize in values
      const normalizedUpdates = {
        ...updates,
        eta: normEta,
        etd: normEtd
      };
      const setClause = updateFields.map((key, index) => `${key} = $${index + 1}`).join(', ');
      const values = updateFields.map(key => normalizedUpdates[key] || updates[key]);
      values.push(id);
      const updateQuery = `UPDATE orders SET ${setClause}, updated_at = CURRENT_TIMESTAMP WHERE id = $${values.length} RETURNING *`;
      const updateResult = await client.query(updateQuery, values);
      await client.query('COMMIT');
      console.log("Updated order:", id);
      res.json(updateResult.rows[0]);
    } else {
      await client.query('COMMIT');
      res.json(currentOrder);
    }
  } catch (error) {
    if (client) {
      await client.query('ROLLBACK');
    }
    console.error('Error updating order:', error.message);
    if (error.code === '23505') {
      return res.status(409).json({ error: 'Booking reference already exists' });
    }
    if (error.code === '23514') {
      return res.status(400).json({ error: 'Invalid value for constrained field (e.g., status)' });
    }
    if (error.code === '22007' || error.code === '22008') {
      return res.status(400).json({ error: 'Invalid date format. Use YYYY-MM-DD.' });
    }
    res.status(500).json({ error: 'Failed to update order', details: error.message });
  } finally {
    if (client) {
      client.release();
    }
  }
}

// getOrders, getOrderById, getOrderStatuses, getOrderUsageHistory, cancelOrder (unchanged from previous)
export async function getOrders(req, res) {
  try {
    const { page = 1, limit = 50, ...filters } = req.query;

    // Build dynamic WHERE clause for base filters (non-computed)
    let baseWhereClause = "WHERE o.status != 'Cancelled' AND o.status IS NOT NULL";  // Filter active orders
    const baseParams = [];
    let paramIndex = 1;

    // Add filters for order fields (adjust field names to match schema)
    if (filters.booking_ref) {
      baseWhereClause += ` AND o.booking_ref ILIKE $${paramIndex}`;
      baseParams.push(`%${filters.booking_ref}%`);
      paramIndex++;
    }
    if (filters.status) {
      baseWhereClause += ` AND o.status = $${paramIndex}`;
      baseParams.push(filters.status);
      paramIndex++;
    }
    if (filters.customer) {
      baseWhereClause += ` AND (o.sender_name ILIKE $${paramIndex} OR o.receiver_name ILIKE $${paramIndex})`;  // Search sender/receiver
      baseParams.push(`%${filters.customer}%`);
      paramIndex++;
    }
    // Example date filter: if (filters.eta_from && filters.eta_to) { ... }

    // If filtering on container-related fields, we'll handle via join
    const containerFilter = filters.container_number ? filters.container_number : null;

    // Compute placeholder positions for LIMIT/OFFSET *before* building query
    let containerParamPosition = containerFilter ? paramIndex : 0;  // 0 if no container filter
    const limitPosition = paramIndex + (containerFilter ? 1 : 0);   // Position for LIMIT (after container if present)
    const offsetPosition = limitPosition + 1;                       // Position for OFFSET

    // Main query with CTE for computed container_derived_status (aligned with container CTE)
    const query = `
      WITH computed_orders AS (
        SELECT 
          o.*,  -- All order fields (e.g., id, booking_ref, status, etc.)
          -- Container details from joined table
          cm.container_number,
          cm.container_size,
          cm.container_type,
          -- Computed derived status for the associated container (manual override or dynamic logic)
          COALESCE(cm.manual_derived_status, 
            -- Dynamic computation based on hire dates and latest availability
            CASE 
              -- Returned if hire ended before today and cleared
              WHEN chd.hire_end_date < CURRENT_DATE AND cs.availability = 'Cleared' THEN 'Returned'
              -- Hired if start date set but no end date
              WHEN chd.hire_end_date IS NULL AND chd.hire_start_date IS NOT NULL THEN 'Hired'
              -- Occupied if hire end date in future
              WHEN chd.hire_end_date > CURRENT_DATE THEN 'Occupied'
              -- Direct availability mappings for transit/loaded states
              WHEN cs.availability IN ('In Transit', 'Loaded', 'Assigned to Job') THEN cs.availability
              -- Specific availability states
              WHEN cs.availability = 'Arrived' THEN 'Arrived'
              WHEN cs.availability = 'De-Linked' THEN 'De-Linked'
              WHEN cs.availability = 'Under Repair' THEN 'Under Repair'
              WHEN cs.availability = 'Returned' THEN 'Returned'
              -- Default fallback
              ELSE 'Available'
            END
          ) AS container_derived_status  -- Alias for the computed container status
        -- From main orders table
        FROM orders o  -- Main table: core order data
        -- Join to container details via container number
        LEFT JOIN container_master cm ON o.associated_container = cm.container_number
        -- Lateral join for latest container status (most recent record per container)
        LEFT JOIN LATERAL (
          SELECT location, availability  -- Get location and availability
          FROM container_status css      -- From status history
          WHERE css.cid = cm.cid         -- Match container ID
          ORDER BY css.sid DESC NULLS LAST  -- Latest by status ID (descending, nulls last)
          LIMIT 1                        -- Only one row per container
        ) cs ON true                       -- Lateral join condition (always true for each row)
        -- Lateral join for active/latest hire details (avoids duplicates)
        LEFT JOIN LATERAL (
          SELECT hire_start_date, hire_end_date, hired_by
          FROM container_hire_details chd_inner
          WHERE chd_inner.cid = cm.cid
            AND (chd_inner.hire_end_date IS NULL OR chd_inner.hire_end_date >= CURRENT_DATE)
          ORDER BY chd_inner.hire_start_date DESC NULLS LAST
          LIMIT 1
        ) chd ON true
        ${baseWhereClause}
      )
      SELECT * FROM computed_orders
      ${containerFilter ? `WHERE container_number ILIKE $${containerParamPosition}` : ''}
      -- Order by creation date (newest first) - FIXED: No alias "o" in outer query
      ORDER BY created_at DESC
      -- Pagination (Use pre-computed positions; no spaces or extra $)
      LIMIT $${limitPosition} OFFSET $${offsetPosition}
    `;

    // Build full params for main query
    let fullParams = [...baseParams];
    if (containerFilter) {
      fullParams.push(`%${containerFilter}%`);
    }
    fullParams.push(parseInt(limit));
    fullParams.push((parseInt(page) - 1) * parseInt(limit));

    // Debug the computed positions and full query
    console.log('Computed Positions - Limit:', limitPosition, 'Offset:', offsetPosition);
    console.log('Full Query:', query); // Debug log
    console.log('Full Params:', fullParams); // Debug log

    const result = await pool.query(query, fullParams);

    // Count query (aligned with container count: use CTE)
    let countParamIndex = 1;
    let countBaseParams = [];
    let countWhereClause = "WHERE o.status != 'Cancelled' AND o.status IS NOT NULL";
    if (filters.booking_ref) {
      countWhereClause += ` AND o.booking_ref ILIKE $${countParamIndex}`;
      countBaseParams.push(`%${filters.booking_ref}%`);
      countParamIndex++;
    }
    if (filters.status) {
      countWhereClause += ` AND o.status = $${countParamIndex}`;
      countBaseParams.push(filters.status);
      countParamIndex++;
    }
    if (filters.customer) {
      countWhereClause += ` AND (o.sender_name ILIKE $${countParamIndex} OR o.receiver_name ILIKE $${countParamIndex})`;
      countBaseParams.push(`%${filters.customer}%`);
      countParamIndex++;
    }
    const countContainerPosition = containerFilter ? countParamIndex : 0;
    let countWhereAdd = containerFilter ? ` AND cm.container_number ILIKE $${countContainerPosition}` : '';
    let countParams = [...countBaseParams];
    if (containerFilter) {
      countParams.push(`%${containerFilter}%`);
    }

    const countQuery = `
      -- Count total matching orders for pagination
      SELECT COUNT(*) as total
      -- From main orders table with container join
      FROM orders o
      LEFT JOIN container_master cm ON o.associated_container = cm.container_number
      -- Apply same filters as main query
      ${countWhereClause}
      ${countWhereAdd}
    `;
    const countResult = await pool.query(countQuery, countParams);
    const total = parseInt(countResult.rows[0]?.total || 0);
// Parse attachments for easier use (handles empty string)
const dataWithParsedAttachments = result.rows.map(row => {
  let parsedAttachments = [];
  if (row.attachments && typeof row.attachments === 'string' && row.attachments.trim() !== '') {
    try {
      parsedAttachments = JSON.parse(row.attachments);
    } catch (parseErr) {
      console.warn('Invalid JSON in attachments for order', row.id, ':', parseErr.message);
      parsedAttachments = [];  // Fallback to empty array
    }
  }
  return {
    ...row,
    attachments: Array.isArray(parsedAttachments) ? parsedAttachments : [],
    color: getOrderStatusColor(row.status)
  };
});
   // Parse attachments for easier use (FIXED: Handle empty string)


console.log(`Fetched orders: ${dataWithParsedAttachments.length} Total: ${total} Filters:`, req.query);
res.json({
  data: dataWithParsedAttachments,
  total,
  page: parseInt(page),
  limit: parseInt(limit)
});
  } catch (err) {
    console.error("Error fetching orders:", err.message, "Query params:", req.query);
    if (err.code === '23514') {
      return res.status(400).json({ error: 'Invalid value for constrained field' });
    }
    res.status(500).json({ error: 'Failed to fetch orders', details: err.message });
  }

}
export async function getOrderById(req, res) {
  try {
    console.log('logggg',req,res)
    const { id } = req.params;
    const { includeContainer = 'false' } = req.query;

    let query = ` 
      SELECT o.*,
        cm.container_number as container_number_full,
        cm.container_size,
        cm.container_type,
        cm.owner_type,
        cs.location as container_location,
        cs.availability as container_availability
    `;
    let fromClause = `
      FROM orders o
      LEFT JOIN container_master cm ON o.associated_container = cm.container_number
      LEFT JOIN LATERAL (
        SELECT location, availability
        FROM container_status css
        WHERE css.cid = cm.cid
        ORDER BY css.sid DESC NULLS LAST
        LIMIT 1
      ) cs ON true
    `;
    let whereClause = 'o.id = $1';

    if (includeContainer === 'true') {
      query += `,
        CASE 
          WHEN cs.availability = 'Cleared' THEN 'Cleared'
          WHEN chd.hire_end_date < CURRENT_DATE AND cs.availability = 'Cleared' THEN 'Returned'
          WHEN chd.hire_end_date IS NULL AND chd.hire_start_date IS NOT NULL THEN 'Hired'
          WHEN chd.hire_end_date > CURRENT_DATE THEN 'Occupied'
          WHEN cs.availability IN ('In Transit', 'Loaded', 'Assigned to Job') THEN cs.availability
          WHEN cs.availability = 'Arrived' THEN 'Arrived'
          WHEN cs.availability = 'De-Linked' THEN 'De-Linked'
          WHEN cs.availability = 'Under Repair' THEN 'Under Repair'
          WHEN cs.availability = 'Returned' THEN 'Returned'
          ELSE 'Available'
        END as container_derived_status,
        cpd.manufacture_date as container_manufacture_date,
        cpd.purchase_date as container_purchase_date,
        cpd.purchase_price as container_purchase_price,
        cpd.owned_by as container_owned_by,
        chd.hire_start_date as container_hire_start_date,
        chd.hire_end_date as container_hire_end_date,
        chd.hired_by as container_hired_by
      `;
      fromClause += `
        LEFT JOIN container_purchase_details cpd ON cm.cid = cpd.cid
        LEFT JOIN container_hire_details chd ON cm.cid = chd.cid
      `;
    }

    query += ` ${fromClause} WHERE ${whereClause} ORDER BY o.created_at DESC`;

    const result = await pool.query(query, [id]);
    if (result.rowCount === 0) {
      return res.status(404).json({ error: 'Order not found' });
    }

let parsedAttachments = [];
if (order.attachments && typeof order.attachments === 'string' && order.attachments.trim() !== '') {
  try {
    parsedAttachments = JSON.parse(order.attachments);
  } catch (parseErr) {
    console.warn('Invalid JSON in attachments for order', order.id, ':', parseErr.message);
    parsedAttachments = [];  // Fallback to empty array
  }
}
order.attachments = Array.isArray(parsedAttachments) ? parsedAttachments : [];
order.color = getOrderStatusColor(order.status);
  } catch (parseErr) {
    console.warn('Invalid JSON in attachments for order', order.id, ':', parseErr.message);
    parsedAttachments = [];  // Fallback to empty array
   }
}

export async function getOrderStatuses(req, res) {
  try {
    // Hardcode full list for consistency (or query distinct if preferred)
    const statuses = [
      { value: 'Created', label: 'Created', color: getOrderStatusColor('Created') },
      { value: 'In Transit', label: 'In Transit', color: getOrderStatusColor('In Transit') },
      { value: 'Delivered', label: 'Delivered', color: getOrderStatusColor('Delivered') },
      { value: 'Cancelled', label: 'Cancelled', color: getOrderStatusColor('Cancelled') }
    ];
    res.json(statuses);
  } catch (err) {
    console.error("Error fetching order statuses:", err);
    res.status(500).json({ error: 'Failed to fetch order statuses' });
  }
}

export async function getOrderUsageHistory(req, res) {
  try {
    const { id } = req.params;
    const query = `
      SELECT 
        o.id, o.booking_ref as job_no, o.status, o.created_at as start_date, o.updated_at as end_date,
        o.place_of_loading as pol, o.final_destination as pod, o.consignment_remarks as remarks,
        cm.container_number, cm.owner_type,
        cs.availability as status,
        cpd.owned_by, chd.hired_by
      FROM orders o
      LEFT JOIN container_master cm ON o.associated_container = cm.container_number
      LEFT JOIN LATERAL (
        SELECT availability
        FROM container_status css
        WHERE css.cid = cm.cid
        ORDER BY css.sid DESC NULLS LAST
        LIMIT 1
      ) cs ON true
      LEFT JOIN container_purchase_details cpd ON cm.cid = cpd.cid
      LEFT JOIN container_hire_details chd ON cm.cid = chd.cid
      WHERE o.id = $1
      ORDER BY o.created_at DESC
    `;
    const result = await pool.query(query, [id]);
    const history = result.rows;

    // Format for frontend
    const formattedHistory = history.map(row => ({
      jobNo: row.job_no,
      pol: row.pol || 'N/A',
      pod: row.pod || 'N/A',
      startDate: row.start_date ? row.start_date.toISOString().split('T')[0] : null,
      endDate: row.end_date ? row.end_date.toISOString().split('T')[0] : null,
      statusProgression: [row.status],
      linkedContainers: row.container_number ? row.container_number : 'N/A',
      remarks: row.remarks || `Status: ${row.status}`
    }));

    console.log(`Fetched ${formattedHistory.length} history entries for order ${id}`);
    res.json(formattedHistory);
  } catch (err) {
    console.error("Error fetching order usage history:", err);
    res.status(500).json({ error: 'Failed to fetch order usage history' });
  }
}

export async function cancelOrder(req, res) {
  try {
    const { id } = req.params;
    const result = await pool.query('UPDATE orders SET status = \'Cancelled\', updated_at = CURRENT_TIMESTAMP WHERE id = $1 RETURNING *', [id]);

    if (result.rowCount === 0) {
      return res.status(404).json({ error: 'Order not found' });
    }

    // If associated container, insert 'Available' status
    const order = result.rows[0];
    if (order.associated_container) {
      const containerCheck = await pool.query(
        'SELECT cid FROM container_master WHERE container_number = $1 AND status = 1',
        [order.associated_container]
      );
      if (containerCheck.rowCount > 0) {
        const cid = containerCheck.rows[0].cid;
        await pool.query(
          'INSERT INTO container_status (cid, availability, status_notes, created_by) VALUES ($1, $2, $3, $4)',
          [cid, 'Available', `Unassigned due to order ${id} cancellation`, 'system']
        );
      }
    }

    console.log("Cancelled order:", id);
    res.json({ message: 'Order cancelled' });
  } catch (err) {
    console.error("pool error:", err.message);
    if (err.code === '23514') {
      return res.status(400).json({ error: 'Invalid value for constrained field' });
    }
    res.status(500).json({ error: err.message || 'Failed to cancel order' });
  }
}