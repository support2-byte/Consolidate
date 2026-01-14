import pool from "../../db/pool.js";


// Helper: Normalize date to ISO string or null
// function normalizeDate(dateString) {
//   if (!dateString) return null;
//   const normalized = dateString.toString().split('T')[0];
//   return isValidDate(normalized) ? normalized : null;
// }

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
  // if (!status) errors.push('status');
  // if (!validConsignmentStatuses.includes(status));
  if (!shipper) errors.push('shipper');
  if (!consignee) errors.push('consignee');
  if (!origin) errors.push('origin');
  if (!destination) errors.push('destination');
  if (!eform || !eform.match(/^[A-Z]{3}-\d{6}$/)) errors.push(`eform (invalid format, got: "${eform}")`);
  if (!isValidDate(eform_date)) errors.push(`eform_date (got: "${eform_date}")`);
  // if (!bank) errors.push('bank');
  if (consignment_value === undefined || consignment_value < 0 || isNaN(consignment_value)) errors.push('consignment_value (must be non-negative number)');
  // if (!paymentType) errors.push('paymentType');
  // if (!vessel) errors.push('vessel');
  if (!voyage || voyage.length < 3) errors.push(`voyage (min 3 chars, got: "${voyage}")`);
  if (eta && !isValidDate(eta)) errors.push(`eta (got: "${eta}")`);
  // if (!shippingLine) errors.push('shippingLine');  // Optional? Adjust if needed
  if (seal_no && seal_no.length < 3) errors.push(`seal_no (min 3 chars, got: "${seal_no}")`);  // Optional validation
  // if (netWeight === undefined || netWeight < 0 || isNaN(netWeight)) errors.push('netWeight (must be non-negative number)');
  // if (gross_weight === undefined || gross_weight < 0 || isNaN(gross_weight)) errors.push('gross_weight (must be non-negative number)');
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
// function getStatusColor(status) {
//   const colors = {
//     'Draft': 'info',
//     'Submitted': 'warning',
//     'In Transit': 'warning',
//     'Delivered': 'success',
//     'Cancelled': 'error',
//     'Created': 'info'
//   };
//   return colors[status] || 'default';
// }
// === Valid Consignment Statuses (must match your PostgreSQL enum exactly) ===
export const VALID_CONSIGNMENT_STATUSES = [
  'Draft',
  'Submitted',
  'In Transit',
  'Delivered',
  'Cancelled',
  'Drafts Cleared',
  'Submitted On Vessel',
  'In Transit On Vessel',
  'Customs Cleared',
  'Under Shipment Processing',
  'Arrived at Facility',
  'Ready for Delivery',
  'Arrived at Destination',
  'HOLD for Delivery',
  'HOLD'  // Add this if you ever use plain 'HOLD'
  // Note: 'Loaded Into Container' is NOT a consignment status — it's for receivers
];

// === Status Color Mapping (Used everywhere: consignment, container, receiver cards) ===
export function getStatusColor(status) {
  if (!status) return '#E0E0E0'; // Fallback for null/undefined

  const colors = {
    // Legacy / Initial
    'Draft': '#E0E0E0',
    'Drafts Cleared': '#E0E0E0',           // Light gray - draft stage
    'Order Created': '#E0E0E0',
    'Created': '#E0E0E0',

    // Submission & Early Processing
    'Submitted': '#FFEB3B',                // Yellow
    'Submitted On Vessel': '#9C27B0',      // Purple (key milestone)
    'Customs Cleared': '#4CAF50',          // Green

    // In Transit
    'In Transit': '#4CAF50',
    'In Transit On Vessel': '#4CAF50',     // Green
    'Shipment In Transit': '#4CAF50',
    'In Transit': '#4CAF50',

    // Processing
    'Under Shipment Processing': '#FF9800', // Orange
    'Shipment Processing': '#FF9800',
    'Under Processing': '#FF9800',

    // Facility & Ready
    'Arrived at Facility': '#795548',       // Brown
    'Arrived at Sort Facility': '#795548',
    'Ready for Delivery': '#FFEB3B',        // Yellow
    'Cleared for Delivery': '#FFEB3B',

    // Final Destination
    'Arrived at Destination': '#FFEB3B',    // Yellow

    // Completion
    'Delivered': '#2196F3',                 // Blue
    'Shipment Delivered': '#2196F3',
    'Partially Delivered': '#FF9800',

    // Holds & Issues
    'HOLD': '#FF9800',
    'HOLD for Delivery': '#FF9800',
    'Under Repair': '#FF9800',

    // Loading
    'Ready for Loading': '#FFEB3B',
    'Loaded Into Container': '#2196F3',
    'Loaded': '#2196F3',

    // Container-specific
    'Available': '#E0E0E0',
    'Assigned to Job': '#FFEB3B',
    'Assigned to Consignment': '#FFEB3B',
    'Occupied': '#4CAF50',
    'Hired': '#9C27B0',
    'De-linked': '#F44336',
    'Returned': '#795548',

    // Terminal
    'Cancelled': '#F44336'
  };

  return colors[status] || '#9E9E9E'; // Medium gray fallback for unknown statuses
} 
// Aggregate status from linked data
function aggregateConsignmentStatus(linkedOrders, containers, currentStatus) {
  if (['HOLD', 'Cancelled'].includes(currentStatus)) return currentStatus;  // No aggregation for held/cancelled

  // Shipment/Order aggregation (C → A mapping)
  const orderStatuses = linkedOrders.map(o => o.order_status).filter(Boolean);
  if (orderStatuses.length === 0) return currentStatus;

  const statusCounts = {};
  orderStatuses.forEach(s => statusCounts[s] = (statusCounts[s] || 0) + 1);
  const dominantOrderStatus = Object.keys(statusCounts).reduce((a, b) => statusCounts[a] > statusCounts[b] ? a : b);

  const orderToConsignmentMap = {
    'Order Created': 'Drafts Cleared',
    'Ready for Loading': 'Drafts Cleared',
    'Loaded Into Container': 'Submitted On Vessel',
    'Shipment Processing': 'Submitted On Vessel',
    'Shipment In Transit': 'In Transit On Vessel',
    'Under Processing': 'In Transit On Vessel',
    'Arrived at Sort Facility': 'Ready for Delivery',
    'Ready for Delivery': 'Ready for Delivery',
    'Shipment Delivered': 'Delivered',
    'Partially Delivered': 'HOLD for Delivery'  // Partial → Hold for rest
  };
  let fromOrders = orderToConsignmentMap[dominantOrderStatus] || currentStatus;

  // Container aggregation (B → A mapping) - Prioritize if conflicting
  const containerStatuses = (containers || []).map(c => c.status).filter(Boolean);
  if (containerStatuses.length > 0) {
    const containerMap = {
      'Available': 'Drafts Cleared',
      'Hired': 'Drafts Cleared',
      'Occupied': 'Submitted On Vessel',
      'In Transit': 'In Transit On Vessel',
      'Loaded': 'In Transit On Vessel',
      'Assigned to Job': 'Submitted On Vessel',
      'Assigned to Consignment': 'Submitted On Vessel',
      'De-linked': 'HOLD',
      'Under Repair': 'HOLD',
      'Returned': 'HOLD for Delivery',
      'Cleared for Delivery': 'Ready for Delivery',
      'Ready for Delivery': 'Ready for Delivery',
      'Shipment for Processing': 'Submitted On Vessel',
      'Under Processing': 'Submitted On Vessel'
    };
    const dominantContainerStatus = containerStatuses.reduce((acc, s) => {
      acc[s] = (acc[s] || 0) + 1;
      return acc;
    }, {});
    const topContainer = Object.keys(dominantContainerStatus).reduce((a, b) => dominantContainerStatus[a] > dominantContainerStatus[b] ? a : b);
    let fromContainers = containerMap[topContainer] || currentStatus;

    // Resolve conflict: Container overrides order if more advanced (e.g., In Transit > Processing)
    const priorityOrder = ['Drafts Cleared', 'Submitted On Vessel', 'In Transit On Vessel', 'Ready for Delivery', 'Delivered'];
    if (priorityOrder.indexOf(fromContainers) > priorityOrder.indexOf(fromOrders)) {
      return fromContainers;
    }
  }

  return fromOrders;
}
// Helper function to normalize date to ISO string (YYYY-MM-DDTHH:mm:ss.sssZ)
function normalizeDate(dateStr) {
  if (!dateStr) return null;
  const date = new Date(dateStr);
  return date.toISOString();
}
// Helper to extract valid integer IDs from orderIds (handles array of IDs or array of objects; robust parsing)
function extractOrderIds(orderData) {
  if (!orderData || !Array.isArray(orderData)) {
    return [];
  }

  const ids = orderData
    .map(item => {
      let id = null;
      if (typeof item === 'number') {
        id = item;
      } else if (typeof item === 'object' && item !== null) {
        if ('id' in item && item.id !== null && item.id !== undefined) {
          id = parseInt(item.id, 10);
        } else if ('value' in item || 'key' in item) {
          const val = item.value || item.key;
          id = parseInt(val, 10);
        }
      } else if (typeof item === 'string') {
        id = parseInt(item, 10);
      }
      return id;
    })
    .filter(id => Number.isInteger(id) && id > 0);  // Strict: integer and positive

  return ids;
}

// Helper to check if consignment_tracking table exists (for robust logging)
async function tableExists(client, tableName) {
  try {
    const result = await client.query(`
      SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = $1
      );
    `, [tableName]);
    return result.rows[0].exists;
  } catch (err) {
    console.warn(`Error checking table existence for ${tableName}:`, err);
    return false;
  }
}
export async function calculateETA(pool, status) {
  const isProd = process.env.NODE_ENV === 'production';
  const today = isProd ? new Date() : new Date('2025-12-26');  // Fixed for testing
  try {
    const { rows } = await pool.query(
      'SELECT days_offset FROM status_config WHERE status = $1 LIMIT 1', 
      [status]
    );
    let offsetDays;
    if (rows.length === 0) {
      // Map consignment statuses to table aliases (from your data)
      const statusMapping = {
        'Drafts Cleared': 'Order Created',  // 15 days
        'Submitted On Vessel': 'Loaded Into Container',  // 9 days
        'Customs Cleared': 'Shipment Processing',  // 7 days
        'Submitted': 'Submitted',  // 10 days (exact)
        'Under Shipment Processing': 'Shipment Processing',  // 7 days
        'In Transit On Vessel': 'Shipment In Transit',  // 4 days
        'In Transit': 'In Transit',  // 5 days (exact)
        'Arrived at Facility': 'Arrived at Sort Facility',  // 1 day
        'Ready for Delivery': 'Ready for Delivery',  // 0 days
        'Arrived at Destination': 'Shipment Delivered',  // 0 days
        'Delivered': 'Delivered',  // 0 days
        'HOLD for Delivery': 'Under Processing',  // 2 days fallback
        'HOLD': 0,  // Terminal
        'Cancelled': 0  // Terminal
      };
      const mappedStatus = statusMapping[status] || status;
      const mappedRows = await pool.query(
        'SELECT days_offset FROM status_config WHERE status = $1 LIMIT 1', 
        [mappedStatus]
      );
      if (mappedRows.rows.length === 0) {
        const defaultOffsets = { 'Drafts Cleared': 30 };  // Custom fallback
        offsetDays = defaultOffsets[status] || 0;
        console.warn(`No mapped config for '${status}' (tried '${mappedStatus}'); using default ${offsetDays} days`);
      } else {
        offsetDays = mappedRows.rows[0].days_offset || 0;
        console.log(`Mapped '${status}' to '${mappedStatus}' with offset ${offsetDays} days`);
      }
    } else {
      offsetDays = rows[0].days_offset || 0;
    }
    const newDate = new Date(today.getTime() + offsetDays * (1000 * 60 * 60 * 24));
    return newDate.toISOString();
  } catch (err) {
    console.error(`Error calculating ETA for status ${status}:`, err);
    return today.toISOString();  // Graceful fallback
  }
}
// Assuming pg client/pool; call with client for tx safety
// async function safeLogToTracking(client, consignmentId, eventType, logData = {}) {
//   // Validate event_type against schema CHECK (optional, but prevents 23514 errors)
//   const validEvents = ['status_advanced', 'status_updated', 'order_synced', 'status_auto_updated'];  // Sync with DB
//   if (!validEvents.includes(eventType)) {
//     console.warn(`Invalid event_type '${eventType}' – add to DB CHECK constraint`);
//     return { success: false, reason: 'Invalid event' };
//   }

//   try {
//     // Normalize logData to schema fields
//     const {
//       from: oldStatus = null,
//       to: newStatus = null,
//       offsetDays = 0,
//       reason = null,
//       ...extraDetails  // Catch-all for eta, etc.
//     } = logData;

//     const details = {
//       ...extraDetails,
//       old_status: oldStatus,
//       new_status: newStatus,
//       reason,
//       // Legacy: If code expects 'data', mirror here (or drop)
//       data: logData  // Raw input as fallback
//     };

//     const query = `
//       INSERT INTO consignment_tracking (
//         consignment_id, event_type, old_status, new_status, offset_days, details
//       ) VALUES ($1, $2, $3, $4, $5, $6)
//       ON CONFLICT (consignment_id, event_type, timestamp) DO NOTHING
//       RETURNING id
//     `;
//     const result = await client.query(query, [
//       consignmentId,
//       eventType,
//       oldStatus,
//       newStatus,
//       offsetDays,
//       details  // JSONB auto-handled by pg
//     ]);

//     if (result.rowCount > 0) {
//       console.log(`✓ Logged '${eventType}' for ${consignmentId} (ID: ${result.rows[0].id})`);
//       return { success: true, id: result.rows[0].id };
//     } else {
//       console.log(`⚠ Duplicate '${eventType}' skipped for ${consignmentId}`);
//       return { success: true, skipped: true };
//     }
//   } catch (error) {
//     console.error(`Failed to log '${eventType}' for ${consignmentId}:`, error);
//     switch (error.code) {
//       case '42703':  // Column missing
//         console.error('Schema mismatch – verify columns: old_status, new_status, etc.');
//         break;
//       case '23505':  // Unique violation (beyond ON CONFLICT)
//         console.warn('Unexpected unique conflict');
//         break;
//       case '23514':  // CHECK violation
//         console.error(`Invalid event_type '${eventType}' – update DB constraint`);
//         break;
//       default:
//         console.error('Unexpected log error');
//     }
//     return { success: false, error: error.message };
//     // NO THROW – keep tx alive
//   }
// }

export async function getConsignmentById(req, res) {
  console.log('Fetching consignment:', req.params);
  try {
    const { id } = req.params;
    const { autoSync = 'false' } = req.query; // Changed default to false for safety
    const enableAutoSync = autoSync === 'true';

    const numericId = parseInt(id, 10);
    if (isNaN(numericId) || numericId <= 0) {
      return res.status(400).json({ error: 'Invalid consignment ID.' });
    }

    let client = await pool.connect();
    try {
      await client.query('BEGIN');

      // Fetch consignment
      const { rows } = await client.query('SELECT * FROM consignments WHERE id = $1', [numericId]);
      if (rows.length === 0) {
        await client.query('ROLLBACK');
        return res.status(404).json({ error: 'Consignment not found' });
      }
      let consignment = rows[0];

      // Parse orders and containers
      let orderIds = [];
      if (consignment.orders) {
        let raw = typeof consignment.orders === 'string' ? JSON.parse(consignment.orders) : consignment.orders;
        orderIds = extractOrderIds(raw).map(o => parseInt(o, 10)).filter(o => o > 0);
      }

      let linkedOrders = [];
      let receivers = [];
      let minReceiverEta = null;
      let mostAdvancedReceiverStatus = null;

      if (orderIds.length > 0) {
        // Fetch orders
        const orderResult = await client.query(`
          SELECT id, sender_name AS shipper, receiver_name AS consignee, eta AS order_eta, etd, 
                 qty_delivered AS delivered, total_assigned_qty, status AS order_status
          FROM orders WHERE id = ANY($1::int[])
        `, [orderIds]);
        linkedOrders = orderResult.rows;

        // Fetch receivers to get accurate shipment status + ETA
        const receiverResult = await client.query(`
          SELECT status, eta FROM receivers WHERE order_id = ANY($1::int[])
        `, [orderIds]);
        receivers = receiverResult.rows;

        if (receivers.length > 0) {
          // Find earliest ETA and most advanced status
          const validEtas = receivers
            .filter(r => r.eta)
            .map(r => new Date(r.eta));

          if (validEtas.length > 0) {
            minReceiverEta = new Date(Math.min(...validEtas));
          }

          // Optional: determine "most advanced" receiver status for suggestion
          const statusPriority = {
            'Shipment Delivered': 9,
            'Ready for Delivery': 8,
            'Under Processing': 7,
            'Shipment In Transit': 6,
            'Shipment Processing': 5,
            'Loaded Into Container': 4,
            'Ready for Loading': 3,
            'Order Created': 2,
            'Created': 1
          };
          mostAdvancedReceiverStatus = receivers.reduce((best, curr) => {
            const priority = statusPriority[curr.status] || 0;
            const bestPriority = statusPriority[best?.status] || 0;
            return priority > bestPriority ? curr : best;
          }, null)?.status;
        }
      }

      // Parse containers
      let parsedContainers = [];
      if (typeof consignment.containers === 'string') {
        try { parsedContainers = JSON.parse(consignment.containers); }
        catch (e) { parsedContainers = []; }
      } else {
        parsedContainers = consignment.containers || [];
      }
      consignment.containers = parsedContainers.map(c => ({
        ...c,
        statusColor: getStatusColor(c.status || '')
      }));

      // Enhance consignment
      consignment.statusColor = getStatusColor(consignment.status);

      if (linkedOrders.length > 0) {
        const first = linkedOrders[0];
        consignment.shipper = first.shipper || consignment.shipper;
        consignment.consignee = first.consignee || consignment.consignee;
        consignment.etd = first.etd ? normalizeDate(first.etd) : null;

        const totalAssigned = linkedOrders.reduce((s, o) => s + (o.total_assigned_qty || 0), 0);
        const totalDelivered = linkedOrders.reduce((s, o) => s + (o.delivered || 0), 0);
        consignment.delivered = totalDelivered;
        consignment.pending = Math.max(0, totalAssigned - totalDelivered);
        consignment.orders = linkedOrders;
      }

      // Use receiver ETA as source of truth
      if (minReceiverEta) {
        consignment.eta = minReceiverEta.toISOString().split('T')[0];
      }

      // Compute days until ETA
      const today = new Date();
      today.setHours(0, 0, 0, 0);
      if (consignment.eta) {
        const etaDate = new Date(consignment.eta);
        etaDate.setHours(0, 0, 0, 0);
        consignment.days_until_eta = Math.max(0, Math.ceil((etaDate - today) / (86400000)));
      }

      // Resolve shipping line name
      if (typeof consignment.shipping_line === 'number' && consignment.shipping_line > 0) {
        const { rows } = await client.query('SELECT name FROM shipping_lines WHERE id = $1', [consignment.shipping_line]);
        consignment.shipping_line = rows[0]?.name || consignment.shipping_line;
      }

      // Optional: Provide suggested status (for UI warning)
      if (mostAdvancedReceiverStatus && enableAutoSync) {
        const suggestedFromMapping = Object.entries(CONSIGNMENT_TO_STATUS_MAP).find(
          ([_, v]) => v.shipment === mostAdvancedReceiverStatus
        )?.[0];

        if (suggestedFromMapping && suggestedFromMapping !== consignment.status) {
          consignment.suggested_status = suggestedFromMapping;
          consignment.suggested_status_reason = `Based on receiver status: ${mostAdvancedReceiverStatus}`;
        }
      }

      await client.query('COMMIT');

      res.json({ data: consignment });

    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  } catch (err) {
    console.error("Error fetching consignment:", err);
    res.status(500).json({ error: 'Failed to fetch consignment' });
  }
}



export async function updateConsignmentStatus(req, res) {
  console.log("Update Status Request:", { params: req.params, body: req.body });
  try {
    const { id } = req.params;
    const numericId = parseInt(id, 10);
    if (isNaN(numericId) || numericId <= 0) {
      return res.status(400).json({ error: 'Invalid consignment ID. Must be a positive integer.' });
    }

    const { status, reason } = req.body;

    if (!status) {
      return res.status(400).json({ error: 'Status is required in request body' });
    }

    if (!validConsignmentStatuses.includes(status)) {
      return res.status(400).json({ error: `Invalid status: ${status}. Must be one of: ${validConsignmentStatuses.join(', ')}` });
    }

    // Optional: Require reason for terminal/sensitive changes
    const sensitiveStatuses = ['HOLD', 'Cancelled'];
    if (sensitiveStatuses.includes(status) && !reason) {
      return res.status(400).json({ error: `Reason is required for status: ${status}` });
    }

    // Validate consignment exists
    const { rows } = await pool.query('SELECT status FROM consignments WHERE id = $1', [numericId]);
    if (rows.length === 0) {
      return res.status(404).json({ error: 'Consignment not found' });
    }

    const currentStatus = rows[0].status;
    if (currentStatus === status) {
      return res.status(200).json({ message: 'Status unchanged' });
    }

    // Enhanced transition validation (basic state machine)
    const allowedTransitions = {
      'Drafts Cleared': ['Submitted On Vessel', 'Submitted', 'Customs Cleared'],
      'Submitted On Vessel': ['In Transit On Vessel', 'Under Shipment Processing'],
      'In Transit On Vessel': ['Arrived at Facility', 'Ready for Delivery'],
      'Ready for Delivery': ['Delivered', 'HOLD for Delivery'],
      // Terminal: Only self or other terminal
      'Delivered': ['Delivered', 'HOLD'],
      'Cancelled': ['Cancelled'],
      'HOLD': ['HOLD', 'HOLD for Delivery', 'Delivered'],
      // Add more as needed; fallback allows any non-terminal
    };
    const terminalStatuses = ['Delivered', 'Cancelled', 'HOLD'];
    if (terminalStatuses.includes(currentStatus) && !terminalStatuses.includes(status)) {
      return res.status(400).json({ error: `Cannot advance from terminal status: ${currentStatus}` });
    }
    if (!terminalStatuses.includes(currentStatus) && allowedTransitions[currentStatus] && !allowedTransitions[currentStatus].includes(status)) {
      return res.status(400).json({ error: `Invalid transition from ${currentStatus} to ${status}. Allowed: ${allowedTransitions[currentStatus].join(', ')}` });
    }

    // Compute newEta once using pool (set to null for terminal)
    let newEta;
    if (terminalStatuses.includes(status)) {
      newEta = null;  // Or new Date('2025-12-26') for Delivered testing
    } else {
      newEta = await calculateETA(pool, status);
    }
    
    let updateError = null;  // Flag for error handling
    await withTransaction(async (client) => {
      try {
        // Update status, ETA, and timestamp
        await client.query(
          'UPDATE consignments SET status = $1, eta = $2, updated_at = CURRENT_TIMESTAMP WHERE id = $3',
          [status, newEta, numericId]
        );
        
        // Isolated logging (non-critical)
        try {
          await safeLogToTracking(client, numericId, 'status_updated', { 
            from: currentStatus, 
            to: status,
            newEta,
            reason: reason || 'Manual update'
          });
        } catch (logErr) {
          console.warn(`Failed to log tracking for consignment ${numericId}:`, logErr);
          // Continue without abort
        }

        // Isolated notification (non-critical)
        try {
          const updated = await client.query('SELECT * FROM consignments WHERE id = $1', [numericId]);
          await sendNotification(updated.rows[0], `status_updated_to_${status}`, { reason });
        } catch (notifErr) {
          console.warn(`Failed to send notification for consignment ${numericId}:`, notifErr);
          // Continue without abort
        }

        // Optional: Sync linked orders' statuses (skip for partial/terminal to avoid overwriting)
        if (!['Partially Delivered', ...terminalStatuses].includes(status)) {
          try {
            let orderIdsQuery = await client.query('SELECT orders FROM consignments WHERE id = $1', [numericId]);
            let rawOrders = orderIdsQuery.rows[0]?.orders;
            if (typeof rawOrders === 'string') {
              try {
                rawOrders = JSON.parse(rawOrders);
              } catch (parseErr) {
                console.warn(`Failed to parse orders for sync in update:`, parseErr);
                rawOrders = [];
              }
            }
            const syncOrderIds = extractOrderIds(rawOrders).map(oid => parseInt(oid, 10)).filter(oid => !isNaN(oid) && oid > 0);
            // Log for debugging
            console.log(`Sync orderIds for consignment ${numericId}:`, syncOrderIds);
            if (syncOrderIds.length > 0) {
              await client.query(
                'UPDATE orders SET status = $1 WHERE id = ANY($2::int[])',
                [status, syncOrderIds]
              );
            }
          } catch (syncErr) {
            console.warn(`Failed to sync orders for consignment ${numericId}:`, syncErr);
            // Don't rollback—log only
          }
        }
      } catch (updateErr) {
        updateError = updateErr;  // Capture error without throwing yet
        if (updateErr.code === '22P02') {  // Enum violation
          console.warn(`Enum constraint violation for status '${status}' on consignment ${numericId}; skipping update. Add to enum: ALTER TYPE consignment_status ADD VALUE '${status}';`);
        } else {
          throw updateErr;  // Re-throw non-enum errors to abort tx
        }
      }
    });

    // Single response point outside transaction (avoids headers-sent error)
    if (updateError && updateError.code === '22P02') {
      return res.status(409).json({ error: `Status '${status}' not recognized in DB enum. Admin fix needed.` });
    }

    res.json({ 
      message: `Status updated to ${status}`,
      data: { newStatus: status, previousStatus: currentStatus, reason: reason || null, newEta }
    });
  } catch (err) {
    console.error("Error updating status:", err);
    res.status(500).json({ error: 'Failed to update status' });
  }
}
// Helper: Send notification (placeholder—integrate with your GAS/notifications module)
async function sendNotification(consignmentData, event = 'created') {
  // e.g., await emailService.send({ to: consignmentData.consignee.email, subject: `Consignment ${consignmentData.consignment_number} ${event}` });
  console.log(`Notification sent for consignment ${consignmentData.consignment_number}: ${event}`);
}

// Unified logging function: Handles both 'logToTracking' and 'safeLogToTracking' calls
async function logToTracking(client, consignmentId, eventType = 'unknown', logData = {}) {
  // Validate eventType (required, non-null)
  if (!eventType || typeof eventType !== 'string' || eventType.trim() === '') {
    console.error(`Invalid eventType '${eventType}' for consignment ${consignmentId} – defaulting to 'unknown_event'`);
    eventType = 'unknown_event';  // Fallback to avoid NULL violation
  }

  // Validate against schema CHECK (expand as needed)
  const validEvents = ['status_advanced', 'status_updated', 'status_auto_updated', 'updated', 'order_synced'];
  if (!validEvents.includes(eventType)) {
    console.warn(`Event '${eventType}' not in DB CHECK – add to constraint or use valid one`);
  }

  try {
    // Normalize logData
    const {
      from: oldStatus = null,
      to: newStatus = null,
      offsetDays = 0,
      reason = null,
      ...extraDetails
    } = logData;

    const details = {
      ...extraDetails,
      old_status: oldStatus,
      new_status: newStatus,
      reason,
      action: logData.action || eventType  // Legacy: Store 'action' in details if passed
    };

    const query = `
      INSERT INTO consignment_tracking (
        consignment_id, event_type, old_status, new_status, offset_days, details
      ) VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT (consignment_id, event_type, timestamp) DO NOTHING
      RETURNING id
    `;
    const result = await client.query(query, [
      consignmentId,
      eventType.trim(),  // Ensure non-null string
      oldStatus,
      newStatus,
      offsetDays,
      details
    ]);

    if (result.rowCount > 0) {
      console.log(`✓ Logged '${eventType}' for ${consignmentId} (ID: ${result.rows[0].id})`);
      return { success: true, id: result.rows[0].id };
    } else {
      console.log(`⚠ Duplicate '${eventType}' skipped for ${consignmentId}`);
      return { success: true, skipped: true };
    }
  } catch (error) {
    console.error(`Failed to log '${eventType}' for ${consignmentId}:`, error);
    if (error.code === '23502') {
      console.error('NOT NULL violation on event_type – ensure non-null param');
    } else if (error.code === '23514') {
      console.error(`CHECK violation: '${eventType}' not allowed – update DB constraint`);
    }
    return { success: false, error: error.message };
    // No throw – keep tx alive
  }
}
async function safeLogToTracking(client, consignmentId, eventType, logData = {}) {
  // Validate event_type against schema CHECK (optional, but prevents 23514 errors)
  const validEvents = ['status_advanced', 'status_updated', 'order_synced', 'status_auto_updated'];  // Sync with DB
  if (!validEvents.includes(eventType)) {
    console.warn(`Invalid event_type '${eventType}' – add to DB CHECK constraint`);
    return { success: false, reason: 'Invalid event' };
  }
  try {
    // Normalize: Use eventType as event_type; ignore/rename 'action' if present
    const {
      from: oldStatus = null,
      to: newStatus = null,
      offsetDays = 0,
      reason = null,
      action,  // Ignore if passed; use eventType
      ...extraDetails
    } = logData;

    const details = {
      ...extraDetails,
      old_status: oldStatus,
      new_status: newStatus,
      reason,
      action: action || eventType  // Legacy: Store in details if needed
    };

    const query = `
      INSERT INTO consignment_tracking (
        consignment_id, event_type, old_status, new_status, offset_days, details
      ) VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT (consignment_id, event_type, timestamp) DO NOTHING
      RETURNING id
    `;
    const result = await client.query(query, [
      consignmentId,
      eventType,  // Use this for event_type (e.g., 'status_auto_updated')
      oldStatus,
      newStatus,
      offsetDays,
      details
    ]);

    if (result.rowCount > 0) {
      console.log(`✓ Logged '${eventType}' for ${consignmentId} (ID: ${result.rows[0].id})`);
      return { success: true, id: result.rows[0].id };
    } else {
      console.log(`⚠ Duplicate '${eventType}' skipped for ${consignmentId}`);
      return { success: true, skipped: true };
    }
  } catch (error) {
    console.error(`Failed to log '${eventType}' for ${consignmentId}:`, error);
    if (error.code === '42703') {
      console.error('Schema mismatch – check INSERT columns vs. table (e.g., no "action" column)');
    }
    return { success: false, error: error.message };
    // No throw – non-critical
  }
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
    // Extend getStatusColor with missing entries (add to your existing function if needed)
    // In getStatusColor (earlier in file):
    // 'Customs Cleared': '#9C27B0',  // Purple, e.g., post-submission
    // 'Submitted': '#FFEB3B',  // Yellow, initial submit
    // 'Under Shipment Processing': '#9C27B0',  // Purple
    // 'In Transit': '#4CAF50',  // Green (alias for 'In Transit On Vessel')
    // 'Arrived at Facility': '#795548',  // Brown

    // Full list of valid statuses aligned with the consignment status workflow table
    const fullStatuses = [
        { value: 'HOLD', label: 'HOLD', color: getStatusColor('HOLD') },
      { value: 'Cancelled', label: 'Cancelled', color: getStatusColor('Cancelled') },
      { value: 'Drafts Cleared', label: 'Drafts Cleared', color: getStatusColor('Drafts Cleared') },
      { value: 'Submitted On Vessel', label: 'Submitted On Vessel', color: getStatusColor('Submitted On Vessel') },
      { value: 'Customs Cleared', label: 'Customs Cleared', color: getStatusColor('Customs Cleared') },
      { value: 'Submitted', label: 'Submitted', color: getStatusColor('Submitted') },
      { value: 'Under Shipment Processing', label: 'Under Shipment Processing', color: getStatusColor('Under Shipment Processing') },
      { value: 'In Transit', label: 'In Transit', color: getStatusColor('In Transit') },
      { value: 'Arrived at Facility', label: 'Arrived at Facility', color: getStatusColor('Arrived at Facility') },
      { value: 'Ready for Delivery', label: 'Ready for Delivery', color: getStatusColor('Ready for Delivery') },
      { value: 'Arrived at Destination', label: 'Arrived at Destination', color: getStatusColor('Arrived at Destination') },
      { value: 'Delivered', label: 'Delivered', color: getStatusColor('Delivered') },
    
    ];

    // Optional: Query DB for existing statuses to add usage count (non-blocking)
    let dbStatuses = [];
    try {
      const query = `
        SELECT DISTINCT status AS value, 
               COUNT(*) as usage_count
        FROM consignments
        WHERE status IS NOT NULL
        GROUP BY status
        ORDER BY status
      `;
      
      const result = await pool.query(query);
      dbStatuses = result.rows.map(row => ({
        ...row,
        label: row.value,
        color: getStatusColor(row.value) || '#000000'  // Fallback if unknown
      }));
    } catch (dbErr) {
      console.warn('Failed to fetch DB statuses (non-critical); using full list:', dbErr);
      // Continue without DB data—full list still returned
    }

    // Merge: Enhance full list with usage_count from DB
    const statuses = fullStatuses.map(full => {
      const dbMatch = dbStatuses.find(db => db.value === full.value);
      return {
        ...full,
        usage_count: dbMatch ? dbMatch.usage_count : 0
      };
    });

    // Optional: Filter to used-only (uncomment if you want dynamic list)
    // const statuses = fullStatuses.filter(full => {
    //   const dbMatch = dbStatuses.find(db => db.value === full.value);
    //   return dbMatch && dbMatch.usage_count > 0;
    // }).map(full => ({ ...full, usage_count: dbMatch.usage_count }));

    // Match frontend expectation: { statusOptions: [...] }
    res.json({ statusOptions: statuses });
  } catch (err) {
    console.error("Error fetching statuses:", err);
    // Graceful fallback: Minimal options to prevent frontend crash
    res.status(500).json({ 
      statusOptions: [
        { value: 'Drafts Cleared', label: 'Drafts Cleared', color: '#E0E0E0', usage_count: 0 }
      ] 
    });
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

// === Mapping: Consignment Status → Receiver/Shipment Status for ETA lookup ===
const CONSIGNMENT_TO_RECEIVER_STATUS = {
  'Drafts Cleared': 'Ready for Loading',              // → 12 days
  'Submitted On Vessel': 'Shipment Processing',       // → 7 days
  'Customs Cleared': 'Shipment Processing',           // → 7 days
  'Submitted': 'Shipment Processing',
  'Under Shipment Processing': 'Shipment Processing',
  'In Transit': 'Shipment In Transit',                // → 4 days
  'In Transit On Vessel': 'Shipment In Transit',
  'Arrived at Facility': 'Arrived at Sort Facility',  // → 1 day
  'Ready for Delivery': 'Ready for Delivery',         // → 0 days
  'Arrived at Destination': 'Under Processing',       // → 2 days
  'Delivered': 'Shipment Delivered',                  // → 0 days
  'HOLD for Delivery': 'Ready for Delivery',
  'HOLD': 'Ready for Delivery',
  'Cancelled': 'Shipment Delivered',
  // Legacy
  'Draft': 'Ready for Loading'
};

export async function createConsignment(req, res) {
  console.log("Create Consignment Request Body:", req.body);

  try {
    const data = req.body;

    // Normalize input (status is NO longer accepted from input)
    const input = {
      consignment_number: data.consignment_number || data.consignmentNumber,
      remarks: data.remarks || '',
      shipper: data.shipper || '',
      consignee: data.consignee || '',
      shipper_id: data.shipper_id ? parseInt(data.shipper_id) : null,
      consignee_id: data.consignee_id ? parseInt(data.consignee_id) : null,
      shipper_address: data.shipper_address || '',
      consignee_address: data.consignee_address || '',
      origin: data.origin || '',
      destination: data.destination || '',
      eform: data.eform || '',
      eform_date: data.eform_date || data.eformDate,
      bank: data.bank || '',
      bank_id: data.bank_id ? parseInt(data.bank_id) : null,
      consignment_value: data.consignment_value || data.consignmentValue || 0,
      payment_type: data.payment_type || data.paymentType || 'Collect',
      vessel: data.vessel ? parseInt(data.vessel) : null,
      voyage: data.voyage || '',
      eta: data.eta?.trim() || null, // Optional: only if provided
      shipping_line: data.shipping_line || data.shippingLine || '',
      seal_no: data.seal_no || data.sealNo || '',
      net_weight: data.net_weight || data.netWeight || 0,
      gross_weight: data.gross_weight || data.grossWeight || 0,
      currency_code: data.currency_code || data.currencyCode || 'USD',
      delivered: data.delivered || 0,
      pending: data.pending || 0,
      containers: Array.isArray(data.containers) ? data.containers : [],
      orders: Array.isArray(data.orders) ? data.orders.map(id => parseInt(id)) : []
    };

    // Validation (remove status from validation)
    const validationErrors = validateConsignmentFields(input);
    if (validationErrors.length > 0) {
      return res.status(400).json({ error: 'Validation failed', details: validationErrors });
    }

    const containerErrors = validateContainers(input.containers);
    if (containerErrors.length > 0) {
      return res.status(400).json({ error: 'Container validation failed', details: containerErrors });
    }

    // ETA: only normalize if provided
    const finalEta = input.eta ? normalizeDate(input.eta) : null;

    // Prepare data for insert
    // Status is now hardcoded as 'Draft Cleared'
    const dbData = {
      consignment_number: input.consignment_number,
      status: 'Drafts Cleared',                    // ← Hardcoded in backend
      remarks: input.remarks,
      shipper: input.shipper,
      consignee: input.consignee,
      shipper_id: input.shipper_id,
      consignee_id: input.consignee_id,
      shipper_address: input.shipper_address,
      consignee_address: input.consignee_address,
      origin: input.origin,
      destination: input.destination,
      eform: input.eform,
      eform_date: normalizeDate(input.eform_date),
      bank: input.bank,
      bank_id: input.bank_id,
      consignment_value: input.consignment_value,
      payment_type: input.payment_type,
      vessel: input.vessel,
      voyage: input.voyage,
      eta: finalEta,                              // ← Only if provided
      shipping_line_name: input.shipping_line,
      seal_no: input.seal_no,
      net_weight: input.net_weight,
      gross_weight: input.gross_weight,
      currency_code: input.currency_code,
      delivered: input.delivered,
      pending: input.pending,
      containers: JSON.stringify(input.containers),
      orders: JSON.stringify(input.orders)
    };

    // Dynamic INSERT
    const keys = Object.keys(dbData);
    const values = Object.values(dbData);
    const placeholders = keys.map((_, i) => `$${i + 1}`).join(', ');
    const columns = keys.map(k => k.replace(/([A-Z])/g, '_$1').toLowerCase()).join(', ');
    const insertQuery = `INSERT INTO consignments (${columns}) VALUES (${placeholders}) RETURNING *`;

    let newConsignment = null;

    await withTransaction(async (client) => {
      const { rows } = await client.query(insertQuery, values);
      newConsignment = rows[0];
    });

    // Optional: Add status color if needed in response
    // newConsignment.statusColor = getStatusColor(newConsignment.status);

    res.status(201).json({
      message: 'Consignment created successfully',
      data: newConsignment
    });

  } catch (err) {
    console.error("Error creating consignment:", err);

    if (err.code === '23505') {
      return res.status(409).json({ error: 'Consignment number already exists' });
    }

    res.status(500).json({ 
      error: 'Failed to create consignment', 
      details: err.message 
    });
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
      orders: data.orders || []  // Keep as-is: array of IDs (numbers) or objects
      // Do NOT include status_color, created_at, or updated_at here
    };

    const validationErrors = validateConsignmentFields(normalizedInput);
    if (validationErrors.length > 0) {
      return res.status(400).json({ error: 'Validation failed', details: validationErrors });
    }

    const containerErrors = validateContainers(normalizedInput.containers);
    // Handle orders validation: Accept array of numbers (IDs) or objects
    let orderErrors = [];
    if (Array.isArray(normalizedInput.orders)) {
      if (normalizedInput.orders.every(o => typeof o === 'number' && o > 0)) {
        // IDs only: Skip object validation, just check for valid positives
        if (normalizedInput.orders.length > 0 && normalizedInput.orders.some(id => isNaN(id) || id <= 0)) {
          orderErrors = [{ index: -1, errors: ['orders: All IDs must be positive integers'] }];
        }
      } else {
        // Assume objects: Use existing validator
        orderErrors = validateOrders(normalizedInput.orders);
      }
    } else {
      orderErrors = [{ index: -1, errors: ['orders: Must be an array'] }];
    }
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
      orders: JSON.stringify(normalizedInput.orders)  // Stringify IDs or objects as-is
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
      if (data.status !== undefined) {  // Ensure status was provided
        const logResult = await logToTracking(client, id, 'status_updated', { 
          newStatus: normalizedInput.status, 
          eta: computedETA 
        });
        if (!logResult.success) {
          console.warn(`Logging failed for ${id}:`, logResult.error);
        }
      }

      // Cascade: Skip orders and containers UPDATEs (no consignment_id columns; JSON-driven relationships)
      console.log(`Skipped orders/containers cascade for consignment ${id} (JSON-driven relationship)`);

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

export async function calculateETAEndpoint(req, res) {
  const { status } = req.query;
  try {
    const { rows } = await pool.query(
      'SELECT days_offset FROM status_config WHERE status = $1 LIMIT 1', 
      [status]
    );
    let days_offset = 0;
    if (rows.length > 0) {
      days_offset = rows[0].days_offset || 0;
    }
    const eta = await calculateETA(pool, status);  // Reuses the full logic (with mapping/fallbacks)
    res.json({ eta, days_offset });
  } catch (err) {
    console.error(`ETA endpoint error for status ${status}:`, err);
    res.status(500).json({ error: 'ETA calculation failed' });
  }
}