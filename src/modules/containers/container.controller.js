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

function validateSocFields({
  location, manufacture_date, purchase_date, purchase_price, purchase_from, owned_by, available_at
}) {
  const errors = [];
  if (!location) errors.push('location');
  if (!isValidDate(manufacture_date)) errors.push(`manufacture_date (got: "${manufacture_date}")`);
  if (!isValidDate(purchase_date)) errors.push(`purchase_date (got: "${purchase_date}")`);
  if (!purchase_price) errors.push('purchase_price');
  if (!purchase_from) errors.push('purchase_from');
  if (!owned_by) errors.push('owned_by');
  if (!available_at) errors.push('available_at');  // Treat as string from dropdown, not date
  return errors;
}

function validateCocFields({
  hire_start_date, hire_end_date, hired_by, return_date, free_days, place_of_loading, place_of_destination
}) {
  const errors = [];
  if (!isValidDate(hire_start_date)) errors.push(`hire_start_date (got: "${hire_start_date}")`);
  if (!isValidDate(hire_end_date)) errors.push(`hire_end_date (got: "${hire_end_date}")`);
  if (!hired_by) errors.push('hired_by');
  if (free_days === undefined || isNaN(free_days)) errors.push('free_days (must be number)');
  if (!place_of_loading) errors.push('place_of_loading');
  if (!place_of_destination) errors.push('place_of_destination');
  if (return_date && !isValidDate(return_date)) errors.push(`return_date (got: "${return_date}")`);
  return errors;
}

// Helper for status color mapping (for dynamic options)
function getStatusColor(status) {
  const colors = {
    'Available': 'success',
    'Returned': 'success',
    'In Transit': 'warning',
    'Loaded': 'warning',
    'Occupied': 'warning',
    'Hired': 'warning',
    'Arrived': 'error',
    'Under Repair': 'error',
    'De-Linked': 'info',
    'Cleared': 'info',
    'Assigned to Job': 'warning'
  };
  return colors[status] || 'default';
}

export async function getStatuses(req, res) {
  try {
    // Updated SQL query aligned with client status flow
    // Derives based on availability, with precedence for job lifecycle statuses
    // Defaults to 'Available' if no matching status
    // Removed hire-date specific logic (Hired/Occupied) as per client flow
    const query = `
      SELECT DISTINCT derived_status AS value, derived_status AS label 
      FROM (
        SELECT 
          CASE 
            WHEN cs.availability IN ('Assigned to Job', 'Loaded', 'In Transit', 'Arrived', 'De-Linked', 'Cleared', 'Returned') THEN cs.availability
            ELSE 'Available'
          END as derived_status
        FROM container_master cm
        LEFT JOIN LATERAL (
          SELECT location, availability
          FROM container_status css
          WHERE css.cid = cm.cid
          ORDER BY css.sid DESC NULLS LAST
          LIMIT 1
        ) cs ON true
        WHERE cm.status = 1
      ) sub
      WHERE derived_status IS NOT NULL
      ORDER BY value
    `;
    
    const result = await pool.query(query);
    let statuses = result.rows.map(row => ({
      ...row,
      color: getStatusColor(row.value)
    }));

//     // Hardcode full list if query returns < 5 (or always, for consistency)
    if (statuses.length < 5 ) {
      statuses = [
        { value: 'Available', label: 'Available', color: getStatusColor('Available') },
        { value: 'Hired', label: 'Hired', color: getStatusColor('Hired') },
        { value: 'Occupied', label: 'Occupied', color: getStatusColor('Occupied') },
        { value: 'In Transit', label: 'In Transit', color: getStatusColor('In Transit') },
        { value: 'Loaded', label: 'Loaded', color: getStatusColor('Loaded') },
        { value: 'Assigned to Job', label: 'Assigned to Job', color: getStatusColor('Assigned to Job') },
        { value: 'Arrived', label: 'Arrived', color: getStatusColor('Arrived') },
        { value: 'De-Linked', label: 'De-Linked', color: getStatusColor('De-Linked') },
        { value: 'Under Repair', label: 'Under Repair', color: getStatusColor('Under Repair') },
        { value: 'Returned', label: 'Returned', color: getStatusColor('Returned') },
        { value: 'Cleared', label: 'Cleared', color: getStatusColor('Cleared') },

        // Add more if needed
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



// export async function getStatuses(req, res) {
//   try {
//     // Updated SQL query with 'Hired' and 'Occupied' added to availability IN clause
//     const query = `
//       SELECT DISTINCT derived_status AS value, derived_status AS label 
//       FROM (
//         SELECT 
//           CASE 
//             WHEN chd.hire_end_date < CURRENT_DATE AND cs.availability = 'Cleared' THEN 'Returned'
//             WHEN chd.hire_end_date IS NULL AND chd.hire_start_date IS NOT NULL THEN 'Hired'
//             WHEN chd.hire_end_date > CURRENT_DATE THEN 'Occupied'
//             WHEN cs.availability IN ('In Transit', 'Loaded', 'Assigned to Job', 'Hired', 'Occupied') THEN cs.availability
//             WHEN cs.availability = 'Arrived' THEN 'Arrived'
//             WHEN cs.availability = 'De-Linked' THEN 'De-Linked'
//             WHEN cs.availability = 'Under Repair' THEN 'Under Repair'
//             WHEN cs.availability = 'Returned' THEN 'Returned'
//             ELSE 'Available'
//           END as derived_status
//         FROM container_master cm
//         LEFT JOIN LATERAL (
//           SELECT location, availability
//           FROM container_status css
//           WHERE css.cid = cm.cid
//           ORDER BY css.sid DESC NULLS LAST
//           LIMIT 1
//         ) cs ON true
//         LEFT JOIN container_hire_details chd ON cm.cid = chd.cid
//         WHERE cm.status = 1
//       ) sub
//       WHERE derived_status IS NOT NULL
//       ORDER BY value
//     `;
    
//     const result = await pool.query(query);
//     let statuses = result.rows.map(row => ({
//       ...row,
//       color: getStatusColor(row.value)
//     }));

//     res.json(statuses);
//   } catch (err) {
//     console.error('Error fetching statuses:', err);
//     res.status(500).json({ error: 'Failed to fetch statuses' });
//   }
// }


// export async function getStatuses(req, res) {
//   try {
//     // Original SQL query (must include this!)
//     const query = `
//       SELECT DISTINCT derived_status AS value, derived_status AS label 
//       FROM (
//         SELECT 
//           CASE 
//             WHEN chd.hire_end_date < CURRENT_DATE AND cs.availability = 'Cleared' THEN 'Returned'
//             WHEN chd.hire_end_date IS NULL AND chd.hire_start_date IS NOT NULL THEN 'Hired'
//             WHEN chd.hire_end_date > CURRENT_DATE THEN 'Occupied'
//             WHEN cs.availability IN ('In Transit', 'Loaded', 'Assigned to Job') THEN cs.availability
//             WHEN cs.availability = 'Arrived' THEN 'Arrived'
//             WHEN cs.availability = 'De-Linked' THEN 'De-Linked'
//             WHEN cs.availability = 'Under Repair' THEN 'Under Repair'
//             WHEN cs.availability = 'Returned' THEN 'Returned'
//             ELSE 'Available'
//           END as derived_status
//         FROM container_master cm
//         LEFT JOIN LATERAL (
//           SELECT location, availability
//           FROM container_status css
//           WHERE css.cid = cm.cid
//           ORDER BY css.sid DESC NULLS LAST
//           LIMIT 1
//         ) cs ON true
//         LEFT JOIN container_hire_details chd ON cm.cid = chd.cid
//         WHERE cm.status = 1
//       ) sub
//       WHERE derived_status IS NOT NULL
//       ORDER BY value
//     `;
    
//     const result = await pool.query(query);
//     let statuses = result.rows.map(row => ({
//       ...row,
//       color: getStatusColor(row.value)
//     }));

//     // Hardcode full list if query returns < 5 (or always, for consistency)
//     if (statuses.length < 5) {
//       statuses = [
//         { value: 'Available', label: 'Available', color: getStatusColor('Available') },
//         { value: 'Hired', label: 'Hired', color: getStatusColor('Hired') },
//         { value: 'Occupied', label: 'Occupied', color: getStatusColor('Occupied') },
//         { value: 'In Transit', label: 'In Transit', color: getStatusColor('In Transit') },
//         { value: 'Loaded', label: 'Loaded', color: getStatusColor('Loaded') },
//         { value: 'Assigned to Job', label: 'Assigned to Job', color: getStatusColor('Assigned to Job') },
//         { value: 'Arrived', label: 'Arrived', color: getStatusColor('Arrived') },
//         { value: 'De-Linked', label: 'De-Linked', color: getStatusColor('De-Linked') },
//         { value: 'Under Repair', label: 'Under Repair', color: getStatusColor('Under Repair') },
//         { value: 'Returned', label: 'Returned', color: getStatusColor('Returned') },
//         // Add more if needed
//       ];
//     }

//     // Remove duplicates if mixing dynamic + hardcoded
//     statuses = statuses.filter((s, index, self) => 
//       index === self.findIndex(t => t.value === s.value)
//     );

//     res.json(statuses);
//   } catch (err) {
//     console.error("Error fetching statuses:", err);
//     res.status(500).json({ error: 'Failed to fetch statuses' });
//   }
// }




export async function getLocations(req, res) {
  try {
    const query = `
      SELECT DISTINCT location AS value, location AS label 
      FROM container_status 
      WHERE location IS NOT NULL
      UNION
      SELECT DISTINCT available_at AS value, available_at AS label 
      FROM container_purchase_details 
      WHERE available_at IS NOT NULL
      ORDER BY value
    `;
    const result = await pool.query(query);
    res.json(result.rows);
  } catch (err) {
    console.error("Error fetching locations:", err);
    res.status(500).json({ error: 'Failed to fetch locations' });
  }
}

export async function getSizes(req, res) {
  try {
    const query = 'SELECT DISTINCT container_size AS value, container_size AS label FROM container_master WHERE container_size IS NOT NULL ORDER BY value';
    const result = await pool.query(query);
    res.json(result.rows);
  } catch (err) {
    console.error("Error fetching sizes:", err);
    res.status(500).json({ error: 'Failed to fetch sizes' });
  }
}

export async function getTypes(req, res) {
  try {
    // Hardcode the full list to match frontend options
    const types = [
      { value: 'Dry High', label: 'Dry High' },
      { value: 'Dry Standard', label: 'Dry Standard' },
      { value: 'Flat High', label: 'Flat High' },
      { value: 'Flat Standard', label: 'Flat Standard' },
      { value: 'Open Top', label: 'Open Top' },
      { value: 'Open Top High', label: 'Open Top High' },
      { value: 'Reefer High', label: 'Reefer High' },
      { value: 'Reefer Standard', label: 'Reefer Standard' },
      { value: 'Tank', label: 'Tank' }
    ];
    res.json(types);
  } catch (err) {
    console.error("Error fetching types:", err);
    res.status(500).json({ error: 'Failed to fetch types' });
  }
}

export async function getOwnershipTypes(req, res) {
  try {
    const query = `
      SELECT 'soc' AS value, 'SOC (Shipper Owned)' AS label
      UNION
      SELECT 'coc' AS value, 'COC (Hired By)' AS label
      ORDER BY value
    `;
    const result = await pool.query(query);
    res.json(result.rows);
  } catch (err) {
    console.error("Error fetching ownership types:", err);
    res.status(500).json({ error: 'Failed to fetch ownership types' });
  }
}

export async function createContainer(req, res) {
  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');

    const {
      container_number, container_size, container_type, owner_type, remarks, created_by,
      // SOC (owned)
      location, availability, manufacture_date, purchase_date, purchase_price, purchase_from, owned_by, available_at, currency,
      // COC (hired)
      hire_start_date, hire_end_date, hired_by, return_date, free_days, place_of_loading, place_of_destination,
    } = req.body;

    // Validation
    if (!container_number || !container_size || !container_type) {
      return res.status(400).json({ error: 'Container number, size, and type required' });
    }
    if (!['soc', 'coc'].includes(owner_type)) {
      return res.status(400).json({ error: 'owner_type must be "soc" or "coc"' });
    }

    let validationErrors = [];
    if (owner_type === 'soc') {
      validationErrors = validateSocFields({ location, manufacture_date, purchase_date, purchase_price, purchase_from, owned_by, available_at });
    } else {
      validationErrors = validateCocFields({ hire_start_date, hire_end_date, hired_by, return_date, free_days, place_of_loading, place_of_destination });
    }

    if (validationErrors.length > 0) {
      console.warn('Validation failed for fields:', validationErrors);
      return res.status(400).json({ 
        error: `${owner_type.toUpperCase()} fields missing or invalid`,
        details: validationErrors.join(', ')
      });
    }

    if (currency && !/^[A-Z]{3}$/.test(currency)) {
      return res.status(400).json({ error: 'Currency must be 3-letter code (e.g., USD)' });
    }

    // Normalize dates (skip available_at since it's now string)
    const normManufactureDate = normalizeDate(manufacture_date);
    const normPurchaseDate = normalizeDate(purchase_date);
    const normHireStartDate = normalizeDate(hire_start_date);
    const normHireEndDate = normalizeDate(hire_end_date);
    const normReturnDate = normalizeDate(return_date);

    if (owner_type === 'soc' && (!normManufactureDate || !normPurchaseDate)) {
      return res.status(400).json({ error: 'Normalized dates invalid for SOC. Use YYYY-MM-DD.' });
    }
    if (owner_type === 'coc' && (!normHireStartDate || !normHireEndDate)) {
      return res.status(400).json({ error: 'Normalized dates invalid for COC. Use YYYY-MM-DD.' });
    }

    // Check for duplicate container_number
    const checkQuery = 'SELECT cid FROM container_master WHERE container_number = $1 AND status = 1';
    const checkResult = await client.query(checkQuery, [container_number]);
    if (checkResult.rowCount > 0) {
      await client.query('ROLLBACK');
      return res.status(409).json({ error: 'Container number already exists' });
    }

    // Insert master
    const masterQuery = `
      INSERT INTO container_master (container_number, container_size, container_type, owner_type, remarks, status, created_by)
      VALUES ($1, $2, $3, $4, $5, 1, $6)
      RETURNING cid
    `;
    const masterValues = [container_number, container_size, container_type, owner_type, remarks || '', created_by];
    const masterResult = await client.query(masterQuery, masterValues);
    const cid = masterResult.rows[0].cid;

    // Insert initial status history
    await client.query(
      'INSERT INTO container_status (cid, location, availability, status_notes, created_by) VALUES ($1, $2, $3, $4, $5)',
      [cid, location || 'Unknown', availability || 'Available', 'Initial creation', created_by]
    );

    // Conditional insert
    if (owner_type === 'soc') {
      console.log('Inserting SOC with available_at (string):', available_at);
      await client.query(
        'INSERT INTO container_purchase_details (cid, manufacture_date, purchase_date, purchase_price, purchase_from, owned_by, available_at, currency, created_by) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)',
        [cid, normManufactureDate, normPurchaseDate, purchase_price, purchase_from, owned_by, available_at, currency || 'USD', created_by]
      );
    } else {
      await client.query(
        'INSERT INTO container_hire_details (cid, hire_start_date, hire_end_date, hired_by, return_date, free_days, place_of_loading, place_of_destination, created_by) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)',
        [cid, normHireStartDate, normHireEndDate, hired_by, normReturnDate, free_days, place_of_loading, place_of_destination, created_by]
      );
    }

    await client.query('COMMIT');

    console.log("Created new container:", { cid });
    res.status(201).json({ message: 'Container created', cid });
  } catch (err) {
    if (client) {
      await client.query('ROLLBACK');
    }
    console.error("pool error:", err.message);
    if (err.code === '23505') {
      return res.status(409).json({ error: 'Container number already exists' });
    }
    if (err.code === '23514') {
      return res.status(400).json({ error: 'Invalid value for constrained field (e.g., owner_type or availability)' });
    }
    if (err.code === '22007' || err.code === '22008') {
      return res.status(400).json({ error: 'Invalid date format. Use YYYY-MM-DD.' });
    }
    res.status(500).json({ error: err.message || 'Failed to create container' });
  } finally {
    if (client) {
      client.release();
    }
  }
}

export async function updateContainer(req, res) {
  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');

    const { cid } = req.params;
    const updates = req.body;
    const created_by = updates.created_by || 'system'; // Assume created_by if not provided
    console.log('updatess', updates);

    // Fetch current owner_type
    const currentResult = await client.query('SELECT owner_type FROM container_master WHERE cid = $1', [cid]);
    const current = currentResult.rows;
    if (current.length === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Container not found' });
    }
    const currentOwnerType = current[0].owner_type;

    // Prevent changing owner_type
    if (updates.owner_type && updates.owner_type !== currentOwnerType) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: 'Cannot change owner_type; manual migration required' });
    }

    // If derived_status provided, treat as new availability
    if (updates.derived_status) {
      updates.availability = updates.derived_status;
    }

    // Validate updated fields (only if provided)
    const updatedSocFields = {
      location: updates.location,
      manufacture_date: updates.manufacture_date,
      purchase_date: updates.purchase_date,
      purchase_price: updates.purchase_price,
      purchase_from: updates.purchase_from,
      owned_by: updates.owned_by,
      available_at: updates.available_at,
    };
    const updatedCocFields = {
      hire_start_date: updates.hire_start_date,
      hire_end_date: updates.hire_end_date,
      hired_by: updates.hired_by,
      return_date: updates.return_date,
      free_days: updates.free_days,
      place_of_loading: updates.place_of_loading,
      place_of_destination: updates.place_of_destination,
    };

    let updateErrors = [];
    if (currentOwnerType === 'soc') {
      // For SOC, only validate provided fields
      for (const [field, value] of Object.entries(updatedSocFields)) {
        if (value !== undefined) {
          if (['manufacture_date', 'purchase_date'].includes(field) && !isValidDate(value)) {
            updateErrors.push(`${field} (got: "${value}")`);
          } else if (!value && !['location', 'available_at'].includes(field)) {  // location and available_at can be empty
            updateErrors.push(field);
          }
        }
      }
    } else {
      for (const [field, value] of Object.entries(updatedCocFields)) {
        if (value !== undefined) {
          if (['hire_start_date', 'hire_end_date', 'return_date'].includes(field) && !isValidDate(value)) {
            updateErrors.push(`${field} (got: "${value}")`);
          } else if (!value && !['return_date'].includes(field)) {  // return_date optional
            updateErrors.push(field);
          } else if (field === 'free_days' && isNaN(value)) {
            updateErrors.push('free_days (must be number)');
          }
        }
      }
    }

    if (updateErrors.length > 0) {
      console.warn('Update validation failed for fields:', updateErrors);
      await client.query('ROLLBACK');
      return res.status(400).json({ 
        error: 'Invalid update fields',
        details: updateErrors.join(', ')
      });
    }

    // Check for duplicate container_number if updating it
    if (updates.container_number) {
      const checkQuery = 'SELECT cid FROM container_master WHERE container_number = $1 AND status = 1 AND cid != $2';
      const checkResult = await client.query(checkQuery, [updates.container_number, cid]);
      if (checkResult.rowCount > 0) {
        await client.query('ROLLBACK');
        return res.status(409).json({ error: 'Container number already exists' });
      }
    }

    // Update master (core fields) - add updated_at if schema has it
    const masterKeys = ['container_number', 'container_size', 'container_type', 'remarks'];
    const masterUpdates = Object.keys(updates).filter(key => masterKeys.includes(key));
    if (masterUpdates.length > 0) {
      const setClause = masterUpdates.map((key, index) => `${key} = $${index + 1}`).join(', ');
      const values = masterUpdates.map(key => updates[key]);
      values.push(cid);
      const updateQuery = `UPDATE container_master SET ${setClause} WHERE cid = $${values.length}`;
      await client.query(updateQuery, values);
    }

    // Insert new status history entry for availability/location changes (instead of update, to support history)
    if (updates.availability !== undefined || updates.location !== undefined) {
      let columns = ['cid'];
      let placeholders = ['$1'];
      let qvalues = [cid];
      let notes = 'Status updated';

      let paramIndex = qvalues.length + 1;

      if (updates.availability !== undefined) {
        columns.push('availability');
        placeholders.push(`$${paramIndex}`);
        qvalues.push(updates.availability);
        notes += ` availability to ${updates.availability}`;
        paramIndex++;
      }

      if (updates.location !== undefined) {
        columns.push('location');
        placeholders.push(`$${paramIndex}`);
        qvalues.push(updates.location);
        notes += ` location to ${updates.location}`;
        paramIndex++;
      }

      columns.push('status_notes');
      placeholders.push(`$${paramIndex}`);
      qvalues.push(notes);
      paramIndex++;

      columns.push('created_by');
      placeholders.push(`$${paramIndex}`);
      qvalues.push(created_by);

      const insertQuery = `INSERT INTO container_status (${columns.join(', ')}) VALUES (${placeholders.join(', ')})`;
      await client.query(insertQuery, qvalues);
    }

    // Conditional: Update purchase (SOC) or hire (COC) - use current owner_type
    const effectiveOwnerType = updates.owner_type || currentOwnerType;
    if (effectiveOwnerType === 'soc') {
      const purchaseKeys = ['manufacture_date', 'purchase_date', 'purchase_price', 'purchase_from', 'owned_by', 'available_at', 'currency'];
      const purchaseUpdates = Object.keys(updates).filter(key => purchaseKeys.includes(key));
      if (purchaseUpdates.length > 0) {
        // Normalize dates for update
        const normManufactureDate = normalizeDate(updates.manufacture_date);
        const normPurchaseDate = normalizeDate(updates.purchase_date);
        const normAvailableAt = updates.available_at;  // String, no normalize
        const normalizedUpdates = {
          ...updates,
          manufacture_date: normManufactureDate,
          purchase_date: normPurchaseDate,
          available_at: normAvailableAt
        };
        const setClause = purchaseUpdates.map((key, index) => `${key} = $${index + 1}`).join(', ');
        const values = purchaseUpdates.map(key => normalizedUpdates[key]);
        values.push(cid);
        const updateQuery = `UPDATE container_purchase_details SET ${setClause} WHERE cid = $${values.length}`;
        console.log('Updating SOC with normalized dates:', { manufacture_date: normManufactureDate, purchase_date: normPurchaseDate, available_at: normAvailableAt });
        await client.query(updateQuery, values);
      }
    } else {  // COC (hired)
      const hireKeys = ['hire_start_date', 'hire_end_date', 'hired_by', 'return_date', 'free_days', 'place_of_loading', 'place_of_destination'];
      const hireUpdates = Object.keys(updates).filter(key => hireKeys.includes(key));
      if (hireUpdates.length > 0) {
        // Normalize dates for update
        const normHireStartDate = normalizeDate(updates.hire_start_date);
        const normHireEndDate = normalizeDate(updates.hire_end_date);
        const normReturnDate = normalizeDate(updates.return_date);
        const normalizedUpdates = {
          ...updates,
          hire_start_date: normHireStartDate,
          hire_end_date: normHireEndDate,
          return_date: normReturnDate
        };
        const setClause = hireUpdates.map((key, index) => `${key} = $${index + 1}`).join(', ');
        const values = hireUpdates.map(key => normalizedUpdates[key]);
        values.push(cid);
        const updateQuery = `UPDATE container_hire_details SET ${setClause} WHERE cid = $${values.length}`;
        console.log('Updating COC with normalized dates:', { hire_start_date: normHireStartDate, hire_end_date: normHireEndDate, return_date: normReturnDate });
        await client.query(updateQuery, values);
      }
    }

    await client.query('COMMIT');

    console.log("Updated container:", cid);
    res.json({ message: 'Container updated' });
  } catch (err) {
    if (client) {
      await client.query('ROLLBACK');
    }
    console.error("pool error:", err.message);
    if (err.code === '23505') {
      return res.status(409).json({ error: 'Container number already exists' });
    }
    if (err.code === '23514') {
      return res.status(400).json({ error: 'Invalid value for constrained field (e.g., owner_type or availability)' });
    }
    if (err.code === '22007' || err.code === '22008') {
      return res.status(400).json({ error: 'Invalid date format. Use YYYY-MM-DD.' });
    }
    res.status(500).json({ error: err.message || 'Failed to update container' });
  } finally {
    if (client) {
      client.release();
    }
  }
}

export async function getAllContainers(req, res) {
  console.log("getAllContainers called with query:", req.query);
  try {
    const { container_number, container_size, container_type, owner_type, status = '', location, page = 1, limit = 100, includeOrder = 'false' } = req.query;
    const offset = (parseInt(page) - 1) * parseInt(limit);

    let whereClause = 'cm.status = 1';
    let baseValues = [];

    if (container_number) {
      whereClause += ' AND cm.container_number ILIKE $' + (baseValues.length + 1);
      baseValues.push(`%${container_number}%`);
    }
    if (container_size) {
      whereClause += ' AND cm.container_size = $' + (baseValues.length + 1);
      baseValues.push(container_size);
    }
    if (container_type) {
      whereClause += ' AND cm.container_type = $' + (baseValues.length + 1);
      baseValues.push(container_type);
    }
    if (owner_type) {
      whereClause += ' AND cm.owner_type = $' + (baseValues.length + 1);
      baseValues.push(owner_type);
    }
    if (location) {
      whereClause += ' AND cs.location ILIKE $' + (baseValues.length + 1);
      baseValues.push(`%${location}%`);
    }

    let baseFrom = `
      FROM container_master cm
      LEFT JOIN LATERAL (
        SELECT location, availability
        FROM container_status css
        WHERE css.cid = cm.cid
        ORDER BY css.sid DESC NULLS LAST
        LIMIT 1
      ) cs ON true
      LEFT JOIN container_purchase_details cpd ON cm.cid = cpd.cid
      LEFT JOIN container_hire_details chd ON cm.cid = chd.cid
    `;

    let selectClause = `
      SELECT 
        cm.cid, cm.container_number, cm.container_size, cm.container_type, cm.owner_type, cm.remarks, cm.status,
        cs.location, 
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
        END as derived_status,
        cpd.manufacture_date, cpd.purchase_date, cpd.purchase_price, cpd.purchase_from, cpd.owned_by, cpd.available_at, cpd.currency,
        chd.hire_start_date, chd.hire_end_date, chd.hired_by, chd.return_date, chd.free_days, chd.place_of_loading, chd.place_of_destination,
        cm.created_time
    `;

    let orderJoin = '';
    if (includeOrder === 'true') {
      selectClause += `,
        o.id as associated_order_id,
        o.booking_ref as associated_booking_ref,
        o.status as associated_order_status,
        o.place_of_loading as order_place_of_loading,
        o.final_destination as order_final_destination
      `;
      orderJoin = `
        LEFT JOIN orders o ON o.associated_container = cm.container_number AND o.status != 'Cancelled'
      `;
      baseFrom += orderJoin;
    }

    // Use CTE to compute derived_status
    const innerQuery = `${selectClause} ${baseFrom} WHERE ${whereClause}`;

    // Prepare params for limit/offset (always added)
    let fullParams = [...baseValues];
    let statusParamIndex = null;
    let statusWhere = '';

    if (status && status !== '') {
      // Filter by specific status
      statusParamIndex = baseValues.length + 1;
      statusWhere = `WHERE derived_status = $${statusParamIndex}`;
      fullParams.push(status);
    }

    // Add limit and offset
    const limitParamIndex = fullParams.length + 1;
    const offsetParamIndex = limitParamIndex + 1;
    fullParams.push(parseInt(limit), offset);

    let fullQuery;
    if (status && status !== '') {
      fullQuery = `
        WITH container_summary AS (${innerQuery})
        SELECT * FROM container_summary 
        ${statusWhere}
        ORDER BY container_number 
        LIMIT $${limitParamIndex} OFFSET $${offsetParamIndex}
      `;
    } else {
      // No status filter: show all
      fullQuery = `
        WITH container_summary AS (${innerQuery})
        SELECT * FROM container_summary 
        ORDER BY container_number 
        LIMIT $${limitParamIndex} OFFSET $${offsetParamIndex}
      `;
    }

    // Count query
    let countWhere = whereClause;
    let countParams = [...baseValues];
    if (status && status !== '') {
      const countStatusIndex = baseValues.length + 1;
      countWhere += ` AND derived_status = $${countStatusIndex}`;
      countParams.push(status);
    }

    const countInnerQuery = `${selectClause} ${baseFrom} WHERE ${countWhere}`;
    const countQuery = `
      WITH container_summary AS (${countInnerQuery})
      SELECT COUNT(*) as total FROM container_summary
    `;

    console.log("Generated Query:", fullQuery);  // Add logging for debugging
    console.log("Generated Count Query:", countQuery);
    console.log("Full Params:", fullParams);
    console.log("Count Params:", countParams);

    const rowsResult = await pool.query(fullQuery, fullParams);
    const countResult = await pool.query(countQuery, countParams);

    const rows = rowsResult.rows;

    console.log("Fetched containers:", rows.length, "Total:", parseInt(countResult.rows[0].total), "Filters:", { ...req.query, status });
    res.json({
      data: rows,
      total: parseInt(countResult.rows[0].total || 0),
      page: parseInt(page),
      limit: parseInt(limit),
    });
  } catch (err) {
    console.error("pool error:", err.message, "Query params:", req.query);
    res.status(500).json({ error: err.message || 'Failed to fetch containers' });
  }
}
export async function getContainerById(req, res) {
  try {
    const { cid } = req.params;
    const { includeOrder = 'false' } = req.query;
    let query = `
      SELECT 
        cm.*, cs.location, cs.availability,
        cpd.manufacture_date, cpd.purchase_date, cpd.purchase_price, cpd.purchase_from, cpd.owned_by, cpd.available_at, cpd.currency,
        chd.hire_start_date, chd.hire_end_date, chd.hired_by, chd.return_date, chd.free_days, chd.place_of_loading, chd.place_of_destination
    `;
    let fromClause = `
      FROM container_master cm
      LEFT JOIN LATERAL (
        SELECT location, availability
        FROM container_status css
        WHERE css.cid = cm.cid
        ORDER BY css.sid DESC NULLS LAST
        LIMIT 1
      ) cs ON true
      LEFT JOIN container_purchase_details cpd ON cm.cid = cpd.cid
      LEFT JOIN container_hire_details chd ON cm.cid = chd.cid
    `;
    let whereClause = 'cm.cid = $1 AND cm.status = 1';

    if (includeOrder === 'true') {
      query += `,
        o.id as associated_order_id,
        o.booking_ref as associated_booking_ref,
        o.status as associated_order_status,
        o.place_of_loading as order_place_of_loading,
        o.final_destination as order_final_destination,
        o.created_at as order_created_at
      `;
      fromClause += `
        LEFT JOIN orders o ON o.associated_container = cm.container_number AND o.status != 'Cancelled'
      `;
    }

    query += ` ${fromClause} WHERE ${whereClause}`;

    const rowsResult = await pool.query(query, [cid]);
    const rows = rowsResult.rows;

    if (rows.length === 0) {
      return res.status(404).json({ error: 'Container not found' });
    }

    console.log("Fetched container:", rows[0].container_number);
    res.json(rows[0]);
  } catch (err) {
    console.error("pool error:", err.message);
    res.status(500).json({ error: err.message || 'Failed to fetch container' });
  }
}

// Usage History endpoint
export async function getUsageHistory(req, res) {
  try {
    const { cid } = req.params;
    const query = `
      SELECT 
        cs.sid, cs.availability as status, CURRENT_TIMESTAMP as status_date, cs.status_notes, cs.created_by,
        cm.container_number, cm.owner_type,
        cpd.owned_by, chd.hired_by,
        o.id as job_id, o.booking_ref as job_no, o.place_of_loading as pol, o.final_destination as pod,
        o.created_at as start_date, o.updated_at as end_date,
        o.status as order_status
      FROM container_status cs
      JOIN container_master cm ON cs.cid = cm.cid
      LEFT JOIN container_purchase_details cpd ON cm.cid = cpd.cid
      LEFT JOIN container_hire_details chd ON cm.cid = chd.cid
      LEFT JOIN orders o ON o.associated_container = cm.container_number 
        AND o.status != 'Cancelled'
      WHERE cs.cid = $1
      ORDER BY cs.sid DESC
    `;
    const result = await pool.query(query, [cid]);
    const history = result.rows;

    // Format for frontend (group by job if multiple statuses per job)
    const formattedHistory = history.map(row => ({
      jobNo: row.job_no || `JOB-${row.status_date.toISOString().split('T')[0].replace(/-/g, '')}`,
      pol: row.pol || (row.owner_type === 'soc' ? 'Self Depot' : 'Vendor Depot'),
      pod: row.pod || 'Destination Depot',
      startDate: row.start_date ? row.start_date.toISOString().split('T')[0] : row.status_date.toISOString().split('T')[0],
      endDate: row.end_date ? row.end_date.toISOString().split('T')[0] : row.status_date.toISOString().split('T')[0],
      statusProgression: [row.status], // Extend if grouping multiple statuses
      linkedOrders: row.job_no ? `ORD-${row.job_id}` : 'N/A',
      remarks: row.status_notes || `Status: ${row.status} by ${row.created_by}`
    }));

    console.log(`Fetched ${formattedHistory.length} history entries for container ${cid}`);
    res.json(formattedHistory);
  } catch (err) {
    console.error("Error fetching usage history:", err);
    res.status(500).json({ error: 'Failed to fetch usage history' });
  }
}

export async function deleteContainer(req, res) {
  try {
    const { cid } = req.params;
    const result = await pool.query('UPDATE container_master SET status = 0 WHERE cid = $1 RETURNING *', [cid]);

    if (result.rowCount === 0) {
      return res.status(404).json({ error: 'Container not found' });
    }

    console.log("Deactivated container:", cid);
    res.json({ message: 'Container deactivated' });
  } catch (err) {
    console.error("pool error:", err.message);
    res.status(500).json({ error: err.message || 'Failed to delete container' });
  }
}