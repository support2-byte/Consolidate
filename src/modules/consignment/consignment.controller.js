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
  if (!status) errors.push('status');
  // if (!validConsignmentStatuses.includes(status));
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
// Valid consignment statuses extracted and standardized from the provided status table
const validConsignmentStatuses = [
  'Draft', 'Submitted', 'In Transit', 'Delivered', 'Cancelled',  // Legacy
  'Drafts Cleared', 'Submitted On Vessel', 'In Transit On Vessel', 'Customs Cleared',
  'Under Shipment Processing', 'Arrived at Facility', 'Ready for Delivery',
  'Arrived at Destination', 'HOLD for Delivery'
  // 'HOLD' if terminal needed (add via ALTER TYPE if missing)
];


function getStatusColor(status) {
  const colors = {
    // Legacy
    'Draft': '#E0E0E0',  // Light gray (same as Drafts Cleared)

    // Consignment (A) - Primary workflow
    'Drafts Cleared': '#E0E0E0',  // Light gray
    'Submitted On Vessel': '#9C27B0',  // Purple
    'In Transit On Vessel': '#4CAF50',  // Green
    'Customs Cleared': '#4CAF50',  // Green (post-clearance)
    'Under Shipment Processing': '#FF9800',  // Orange (processing)
    'Arrived at Facility': '#795548',  // Brown
    'Ready for Delivery': '#FFEB3B',  // Yellow
    'Arrived at Destination': '#FFEB3B',  // Yellow
    'Delivered': '#2196F3',  // Blue (completion)
    'HOLD for Delivery': '#FF9800',  // Orange
    'HOLD': '#FF9800',  // Orange (add if enum has it)

    // Shared (B/C) - Containers/Shipments (fallback for aggregation)
    'Submitted': '#FFEB3B',  // Yellow (initial submit)
    'In Transit': '#4CAF50',  // Green (alias for On Vessel)
    'Loaded Into Container': '#2196F3',  // Blue
    'Shipment Processing': '#FF9800',  // Orange (alias for Under Processing)
    'Shipment In Transit': '#4CAF50',  // Green
    'Under Processing': '#FF9800',  // Orange
    'Arrived at Sort Facility': '#795548',  // Brown
    'Ready for Delivery': '#FFEB3B',  // Yellow
    'Shipment Delivered': '#2196F3',  // Blue
    'Partially Delivered': '#FF9800',  // Orange
    'Order Created': '#E0E0E0',  // Light gray
    'Ready for Loading': '#FFEB3B',  // Yellow
    'Available': '#E0E0E0',  // Light gray
    'Hired': '#9C27B0',  // Purple
    'Occupied': '#4CAF50',  // Green
    'In Transit': '#4CAF50',  // Green
    'Loaded': '#2196F3',  // Blue
    'Assigned to Job': '#FFEB3B',  // Yellow
    'Assigned to Consignment': '#FFEB3B',  // Yellow
    'De-linked': '#F44336',  // Red
    'Under Repair': '#FF9800',  // Orange
    'Returned': '#795548',  // Brown
    'Cleared for Delivery': '#FFEB3B',  // Yellow
    'Shipment for Processing': '#9C27B0',  // Purple

    // Terminals (universal)
    'Cancelled': '#F44336'  // Red
  };
  return colors[status] || '#000000';  // Default black
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
async function safeLogToTracking(client, consignmentId, eventType, logData = {}) {
  // Validate event_type against schema CHECK (optional, but prevents 23514 errors)
  const validEvents = ['status_advanced', 'status_updated', 'order_synced', 'status_auto_updated'];  // Sync with DB
  if (!validEvents.includes(eventType)) {
    console.warn(`Invalid event_type '${eventType}' – add to DB CHECK constraint`);
    return { success: false, reason: 'Invalid event' };
  }

  try {
    // Normalize logData to schema fields
    const {
      from: oldStatus = null,
      to: newStatus = null,
      offsetDays = 0,
      reason = null,
      ...extraDetails  // Catch-all for eta, etc.
    } = logData;

    const details = {
      ...extraDetails,
      old_status: oldStatus,
      new_status: newStatus,
      reason,
      // Legacy: If code expects 'data', mirror here (or drop)
      data: logData  // Raw input as fallback
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
      eventType,
      oldStatus,
      newStatus,
      offsetDays,
      details  // JSONB auto-handled by pg
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
    switch (error.code) {
      case '42703':  // Column missing
        console.error('Schema mismatch – verify columns: old_status, new_status, etc.');
        break;
      case '23505':  // Unique violation (beyond ON CONFLICT)
        console.warn('Unexpected unique conflict');
        break;
      case '23514':  // CHECK violation
        console.error(`Invalid event_type '${eventType}' – update DB constraint`);
        break;
      default:
        console.error('Unexpected log error');
    }
    return { success: false, error: error.message };
    // NO THROW – keep tx alive
  }
}

export async function getConsignmentById(req, res) {
  console.log('Fetching consignment:', req.params);
  try {
    const { id } = req.params;
    const { autoUpdate = 'true' } = req.query;  // Opt-in via ?autoUpdate=true/false (default true)
    const enableAutoUpdate = autoUpdate === 'true';  // Strict boolean check
    
    // Validate id is a valid positive integer
    const numericId = parseInt(id, 10);
    if (isNaN(numericId) || numericId <= 0) {
      console.warn(`Invalid consignment ID provided: ${id}`);
      return res.status(400).json({ error: 'Invalid consignment ID. Must be a positive integer.' });
    }

    console.log('numericidss', numericId);  // Debug: Remove in prod

    let consignment = {}; // Will hold enhanced data

    // Fetch base data
    const query = `SELECT * FROM consignments WHERE id = $1`;
    const { rows } = await pool.query(query, [numericId]);
    if (rows.length === 0) {
      return res.status(404).json({ error: 'Consignment not found' });
    }
    consignment = rows[0];

    // Enhance with color
    consignment.statusColor = getStatusColor(consignment.status);

    // Handle orders: Parse if string, then extract IDs for querying
    let rawOrders = consignment.orders;
    if (typeof rawOrders === 'string') {
      try {
        rawOrders = JSON.parse(rawOrders);
      } catch (parseErr) {
        console.warn(`Failed to parse orders JSON for consignment ${numericId}:`, parseErr);
        rawOrders = [];
      }
    }
    let orderIds = extractOrderIds(rawOrders);
    let linkedOrders = [];

    // Additional safeguard: Re-filter and log
    orderIds = orderIds.map(oid => parseInt(oid, 10)).filter(oid => !isNaN(oid) && Number.isInteger(oid) && oid > 0);

    // Log extracted IDs for debugging (remove in production if needed)
    if (process.env.NODE_ENV === 'development') {
      console.log(`Extracted orderIds for consignment ${numericId}:`, orderIds);
    }

    // Fetch linked orders only if valid IDs present and all are valid integers
    if (orderIds.length > 0 && orderIds.every(oid => Number.isInteger(oid) && oid > 0)) {
      const orderQuery = `
        SELECT id, sender_name AS shipper, receiver_name AS consignee, eta, etd, 
              qty_delivered AS delivered, total_assigned_qty, status AS order_status
        FROM orders o 
        WHERE o.id = ANY($1::int[])
      `;
      const { rows: orders } = await pool.query(orderQuery, [orderIds]);
      linkedOrders = orders;

      if (linkedOrders.length > 0) {
        // Sync from first order (or aggregate)
        const firstOrder = linkedOrders[0];
        consignment.shipper = firstOrder.shipper || consignment.shipper;
        consignment.consignee = firstOrder.consignee || consignment.consignee;
        consignment.etd = firstOrder.etd ? normalizeDate(firstOrder.etd) : null;

        // Compute delivered/pending from orders (initial)
        let totalDelivered = linkedOrders.reduce((sum, o) => sum + (o.delivered || 0), 0);
        let totalAssigned = linkedOrders.reduce((sum, o) => sum + (o.total_assigned_qty || 0), 0);
        consignment.delivered = totalDelivered;
        consignment.pending = Math.max(0, totalAssigned - totalDelivered);

        // Update orders array to full objects
        consignment.orders = linkedOrders.map(o => ({ id: o.id, ...o }));
      } else {
        // No matching orders found; keep rawOrders if they were objects
        consignment.orders = rawOrders;
      }
    } else {
      // No valid order IDs; keep raw if objects
      if (orderIds.length > 0 && !orderIds.every(oid => Number.isInteger(oid) && oid > 0)) {
        console.warn(`Invalid orderIds detected for consignment ${numericId}, skipping orders query:`, orderIds);
      }
      consignment.orders = rawOrders;
    }

    // Enrich containers: Parse if string and add colors
    let parsedContainers = [];
    if (typeof consignment.containers === 'string') {
      try {
        parsedContainers = JSON.parse(consignment.containers);
      } catch (parseErr) {
        console.warn(`Failed to parse containers JSON for consignment ${numericId}:`, parseErr);
        parsedContainers = [];
      }
    } else {
      parsedContainers = consignment.containers || [];
    }
    consignment.containers = parsedContainers.map(c => ({ ...c, statusColor: getStatusColor(c.status || '') }));

    // Compute days_until_eta using current date
    const today = new Date();
    today.setHours(0, 0, 0, 0);  // Normalize to midnight UTC
    if (consignment.eta) {
      // Improved parsing: Force date-only (midnight UTC for both)
      let etaStr = consignment.eta;
      if (etaStr instanceof Date) {
        etaStr = etaStr.toISOString().split('T')[0] + 'T00:00:00.000Z';  // Midnight
      } else if (typeof etaStr !== 'string') {
        console.warn(`Unexpected eta type for consignment ${numericId}:`, typeof etaStr, etaStr);
        etaStr = new Date(etaStr).toISOString().split('T')[0] + 'T00:00:00.000Z';
      }
      const etaDate = new Date(etaStr);
      etaDate.setHours(0, 0, 0, 0);  // Normalize to midnight UTC
      consignment.days_until_eta = Math.max(0, Math.ceil((etaDate - today) / (1000 * 60 * 60 * 24)));  // Ceil for full days ahead
      console.log(`ETA calc for ${numericId}: etaDate=${etaDate.toISOString()}, today=${today.toISOString()}, days=${consignment.days_until_eta}`);  // Debug
    }

    // Aggregate status from orders and containers first
    let aggregatedStatus = aggregateConsignmentStatus(linkedOrders, parsedContainers, consignment.status);

    // Enhanced dynamic status update logic based on ETA (overrides aggregation if triggered)
    let originalStatus = consignment.status;
    let shouldPersist = false;
    let etaTriggeredStatus = aggregatedStatus;  // Start with aggregated
    if (enableAutoUpdate && consignment.eta && consignment.days_until_eta !== undefined && !['HOLD', 'Cancelled', 'Delivered'].includes(aggregatedStatus)) {
      if (consignment.days_until_eta <= 0) {
        etaTriggeredStatus = 'Delivered';
        shouldPersist = true;
      } else if (consignment.days_until_eta <= 7) {
        etaTriggeredStatus = 'In Transit';  // Valid enum status
        shouldPersist = true;
      } else if (consignment.days_until_eta <= 30) {
        etaTriggeredStatus = 'Under Shipment Processing';  // Valid enum status
        shouldPersist = true;
      } else {
        etaTriggeredStatus = 'Customs Cleared';  // Valid and earlier stage
        shouldPersist = true;
      }
    } else {
      // No ETA trigger: Use aggregation if it differs (but only persist if enabled)
      if (aggregatedStatus !== originalStatus) {
        etaTriggeredStatus = aggregatedStatus;
        if (enableAutoUpdate) shouldPersist = true;
      }
    }

    consignment.status = etaTriggeredStatus;  // Apply final status (always for response, but persist only if enabled)

    // Re-apply color after potential update
    consignment.statusColor = getStatusColor(consignment.status);

    // Persist update to DB if status changed (using calculateETA for new ETA) - Gated by enableAutoUpdate
    if (enableAutoUpdate && shouldPersist && consignment.status !== originalStatus) {
      let newEta;
      try {
        newEta = await calculateETA(pool, consignment.status);
      } catch (etaErr) {
        console.warn(`Failed to calculate new ETA for status ${consignment.status}:`, etaErr);
        newEta = consignment.eta;  // Fallback to original
      }

      await withTransaction(async (client) => {
        try {
          await client.query('UPDATE consignments SET status = $1, eta = $2, updated_at = NOW() WHERE id = $3', 
            [consignment.status, newEta, numericId]);

          // Safe log with table check
          await safeLogToTracking(client, numericId, 'status_auto_updated', { 
            newStatus: consignment.status, 
            newEta,
            days_until_eta: consignment.days_until_eta,
            originalStatus,
            aggregatedFrom: 'orders_containers'  // New field for traceability
          });
        } catch (updateErr) {
          if (updateErr.code === '22P02') {  // Enum violation
            console.warn(`Enum constraint violation for status '${consignment.status}' on consignment ${numericId}; skipping update. Add to enum: ALTER TYPE consignment_status ADD VALUE '${consignment.status}';`);
            // Revert local status to original (don't persist invalid)
            consignment.status = originalStatus;
            consignment.statusColor = getStatusColor(originalStatus);
          } else {
            throw updateErr;  // Re-throw other errors
          }
        }
      });

      // Update local object with new values and recompute days_until_eta (outside transaction)
      consignment.eta = newEta;
      let updatedEtaStr = newEta;
      if (updatedEtaStr instanceof Date) {
        updatedEtaStr = updatedEtaStr.toISOString();
      } else if (typeof updatedEtaStr !== 'string') {
        console.warn(`Unexpected newEta type for consignment ${numericId}:`, typeof updatedEtaStr, updatedEtaStr);
        updatedEtaStr = new Date(updatedEtaStr).toISOString();  // Attempt conversion
      }
      const etaDate = new Date(updatedEtaStr + (updatedEtaStr.includes('Z') ? '' : 'T00:00:00.000Z'));
      etaDate.setHours(0, 0, 0, 0);
      consignment.days_until_eta = Math.max(0, Math.ceil((etaDate - today) / (1000 * 60 * 60 * 24)));
      consignment.updated_at = new Date().toISOString();  // Approx server time
    }

    // Auto-adjust delivered/pending if status is 'Delivered' or 'Partially Delivered' (assume full/partial)
    if (['Delivered', 'Partially Delivered'].includes(consignment.status) && linkedOrders.length > 0) {
      const totalQty = linkedOrders.reduce((sum, o) => sum + (o.total_assigned_qty || 0), 0);
      if (consignment.status === 'Delivered') {
        consignment.delivered = totalQty;
        consignment.pending = 0;
      } else {  // Partially Delivered
        consignment.pending = Math.max(0, totalQty - consignment.delivered);  // Keep existing delivered
      }
    }

    // Resolve shipping_line if ID
    if (typeof consignment.shipping_line === 'number' && consignment.shipping_line > 0) {
      const lineQuery = 'SELECT name FROM shipping_lines WHERE id = $1';
      const { rows: lines } = await pool.query(lineQuery, [consignment.shipping_line]);
      consignment.shipping_line = lines[0]?.name || consignment.shipping_line;
    }

    // Clean up null/undefined statusColor
    if (!consignment.statusColor) {
      delete consignment.statusColor;
    }

    res.json({ data: consignment });
  } catch (err) {
    console.error("Error fetching consignment:", err);
    res.status(500).json({ error: 'Failed to fetch consignment' });
  }
}

export async function advanceStatus(req, res) {
  console.log("Advance Status Request Params:", req.params);    
  try {
    const { id } = req.params;
    const numericId = parseInt(id, 10);
    if (isNaN(numericId) || numericId <= 0) {
      return res.status(400).json({ error: 'Invalid consignment ID. Must be a positive integer.' });
    }

    const { rows } = await pool.query('SELECT status FROM consignments WHERE id = $1', [numericId]);
    if (rows.length === 0) {
      return res.status(404).json({ error: 'Consignment not found' });
    }

    const currentStatus = rows[0].status;
    const nextStatusMap = {
      'Drafts Cleared': 'Submitted On Vessel',
      'Submitted On Vessel': 'Customs Cleared',
      'Customs Cleared': 'Submitted',
      'Submitted': 'Under Shipment Processing',
      'Under Shipment Processing': 'In Transit',
      'In Transit': 'Arrived at Facility',
      'Arrived at Facility': 'Ready for Delivery',
      'Ready for Delivery': 'Arrived at Destination',
      'Arrived at Destination': 'Delivered',
      'Delivered': null,
      'HOLD': null,
      'Cancelled': null
    };
    const nextStatus = nextStatusMap[currentStatus];
    if (!nextStatus) {
      return res.status(400).json({ error: `No next status available from ${currentStatus}` });
    }
    // if (!validConsignmentStatuses.includes(currentStatus)) {
    //       return res.status(400).json({ error: `Invalid status: ${currentStatus}. Must be one of: ${validConsignmentStatuses.join(', ')}` });
    //     }
    // Compute newEta once using pool (null for terminal)
    let newEta;
    const terminalStatuses = ['Delivered', 'HOLD', 'Cancelled'];
    if (terminalStatuses.includes(nextStatus)) {
      newEta = null;  // Or current date for Delivered
    } else {
      newEta = await calculateETA(pool, nextStatus);
    }

    let updateError = null;  // Flag for error handling
    await withTransaction(async (client) => {
      try {
        
        await client.query('UPDATE consignments SET status = $1, eta = $2, updated_at = NOW() WHERE id = $3', 
          [nextStatus, newEta, numericId]);
        
// Inside withTransaction try block
const logResult = await safeLogToTracking(client, numericId, 'status_advanced', {
  from: currentStatus,
  to: nextStatus,
  offsetDays: 9,  // From mapping
  newEta,
  reason: 'Manual advance'
});
if (!logResult.success) {
  console.warn(`Logging partial failure for ${numericId}:`, logResult);
  // Optional: If critical, throw logResult.error
}
        // Optional: Sync linked orders' statuses (skip for terminal to avoid overwriting)
        if (!terminalStatuses.includes(nextStatus)) {
          try {
            const orderIdsQuery = await client.query('SELECT orders FROM consignments WHERE id = $1', [numericId]);
            let rawOrders = orderIdsQuery.rows[0]?.orders;
            if (typeof rawOrders === 'string') {
              try {
                rawOrders = JSON.parse(rawOrders);
              } catch (parseErr) {
                console.warn(`Failed to parse orders for sync in advance:`, parseErr);
                rawOrders = [];
              }
            }
            const syncOrderIds = extractOrderIds(rawOrders).map(oid => parseInt(oid, 10)).filter(oid => !isNaN(oid) && oid > 0);
            console.log(`Sync orderIds for consignment ${numericId}:`, syncOrderIds);  // Debug
            if (syncOrderIds.length > 0) {
              await client.query(
                'UPDATE orders SET status = $1 WHERE id = ANY($2::int[])',
                [nextStatus, syncOrderIds]
              );
            }
          } catch (syncErr) {
            console.warn(`Failed to sync orders for consignment ${numericId}:`, syncErr);
            // Don't rollback—log only
          }
        

        }
        // Isolated notification (non-critical)
        try {
          const updated = await client.query('SELECT * FROM consignments WHERE id = $1', [numericId]);
          await sendNotification(updated.rows[0], `status_advanced_to_${nextStatus}`, { reason: 'Manual advance' });
        } catch (notifErr) {
          console.warn(`Failed to send notification for consignment ${numericId}:`, notifErr);
          // Continue without abort
        }
      } catch (updateErr) {
        updateError = updateErr;  // Capture error without throwing yet
        if (updateErr.code === '22P02') {  // Enum violation
          console.warn(`Enum constraint violation for status '${nextStatus}' on consignment ${numericId}; skipping update. Add to enum: ALTER TYPE consignment_status ADD VALUE '${nextStatus}';`);
        } else {
          throw updateErr;  // Re-throw non-enum errors to abort tx
        }
      }
    });

    // Single response point outside transaction (avoids headers-sent error)
    if (updateError && updateError.code === '22P02') {
      return res.status(409).json({ error: `Status '${nextStatus}' not recognized in DB enum. Admin fix needed.` });
    }

    res.json({ 
      message: `Status advanced to ${nextStatus}`,
      data: { newStatus: nextStatus, previousStatus: currentStatus, newEta }
    });
  } catch (err) {
    console.error("Error advancing status:", err);
    res.status(500).json({ error: 'Failed to advance status' });
  }
}
// Flexible status update function with safe logging and ETA update via calculateETA


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

// export async function getConsignmentById(req, res) {
//   console.log('asasa',res)
//   try {
//     const { id } = req.params;
//     const query = `
//       SELECT * FROM consignments WHERE id = $1
//     `;
//     const { rows } = await pool.query(query, [id]);

//     if (rows.length === 0) {
//       return res.status(404).json({ error: 'Consignment not found' });
//     }

//     // Enhance with colors and computations
//     const consignment = rows[0];
//     consignment.statusColor = getStatusColor(consignment.status);

//     // Compute days_until_eta using current date (Dec 05, 2025, as per context)
//     if (consignment.eta) {
//       const etaDate = new Date(consignment.eta);
//       const today = new Date('2025-12-05');  // Fixed current date
//       consignment.days_until_eta = Math.max(0, Math.ceil((etaDate - today) / (1000 * 60 * 60 * 24)));
//     }

//     // Fetch linked orders for alignment/computations (assume orders table has consignment_id FK)
//     const orderQuery = `
//       SELECT id, sender_name AS shipper, receiver_name AS consignee, eta, etd, 
//              qty_delivered AS delivered, total_assigned_qty, status AS order_status
//       FROM orders o 
//       WHERE o.id = ANY($1::int[])  -- Use consignment.orders as array of IDs
//     `;
//     let orderIds = typeof consignment.orders === 'string' ? JSON.parse(consignment.orders) : consignment.orders;
//     if (orderIds.length > 0) {
//       const { rows: linkedOrders } = await pool.query(orderQuery, [orderIds]);
//       if (linkedOrders.length > 0) {
//         // Sync from first order (or aggregate)
//         const firstOrder = linkedOrders[0];
//         consignment.shipper = firstOrder.shipper || consignment.shipper;
//         consignment.consignee = firstOrder.consignee || consignment.consignee;
//         consignment.status = firstOrder.order_status || consignment.status;  // Sync status
//         consignment.etd = firstOrder.etd ? normalizeDate(firstOrder.etd) + 'T00:00:00.000Z' : null;

//         // Compute delivered/pending from orders (removed gross_weight aggregation due to missing column)
//         const totalDelivered = linkedOrders.reduce((sum, o) => sum + (o.delivered || 0), 0);
//         const totalPending = linkedOrders.reduce((sum, o) => sum + (o.total_assigned_qty || 0), 0);
//         consignment.delivered = totalDelivered;
//         consignment.pending = totalPending - totalDelivered;

//         // Update orders array to full objects if needed
//         consignment.orders = linkedOrders.map(o => ({ id: o.id, ...o }));  // Expand
//       }
//     }

//     // Dynamic status update based on ETA (after syncing from orders)
//     if (consignment.eta && consignment.days_until_eta !== undefined) {
//       let originalStatus = consignment.status;
//       if (consignment.days_until_eta <= 0) {
//         consignment.status = 'Delivered';
//       } else if (consignment.days_until_eta <= 7) {
//         consignment.status = 'In Transit';
//       } else if (consignment.days_until_eta <= 30) {
//         consignment.status = 'Submitted';
//       }
//       // Re-apply color after potential update
//       consignment.statusColor = getStatusColor(consignment.status);

//       // Optional: Persist update to DB if status changed (uncomment if needed)
//       // if (consignment.status !== originalStatus) {
//       //   await pool.query('UPDATE consignments SET status = $1, updated_at = NOW() WHERE id = $2', [consignment.status, id]);
//       //   await logToTracking(pool, id, 'status_updated', { 
//       //     newStatus: consignment.status, 
//       //     days_until_eta: consignment.days_until_eta,
//       //     originalStatus 
//       //   });
//       // }
//     }

//     // Enrich containers: Parse and optionally fetch full details
//     if (typeof consignment.containers === 'string') {
//       consignment.containers = JSON.parse(consignment.containers);
//     }
//     // Add from linked orders (flatten nested) - example; expand with actual query if needed
//     // linkedOrders.forEach(order => { /* flatten receivers.containers */ });

//     // Stringify shipping_line if ID (lookup via join)
//     if (typeof consignment.shipping_line === 'number' && consignment.shipping_line > 0) {
//       const lineQuery = 'SELECT name FROM shipping_lines WHERE id = $1';
//       const { rows: lines } = await pool.query(lineQuery, [consignment.shipping_line]);
//       consignment.shipping_line = lines[0]?.name || consignment.shipping_line;
//     }

//     // Remove null status_color (if any)
//     if (consignment.status_color === null || consignment.status_color === undefined) {
//       delete consignment.status_color;
//     }

//     res.json({ data: consignment });
//   } catch (err) {
//     console.error("Error fetching consignment:", err);
//     res.status(500).json({ error: 'Failed to fetch consignment' });
//   }
// }
export async function createConsignment(req, res) {
  console.log("Create Consignment Request Body:", req.body);
  try {
    const data = req.body;
   
    // Map mixed-case input to consistent snake_case
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

    // Validation (assume defined)
    const validationErrors = validateConsignmentFields(normalizedInput);
    if (validationErrors.length > 0) {
      return res.status(400).json({ error: 'Validation failed', details: validationErrors });
    }
    const containerErrors = validateContainers(normalizedInput.containers);
    if (containerErrors.length > 0) {
      return res.status(400).json({ error: 'Array validation failed', details: containerErrors });
    }

// if (!validConsignmentStatuses.includes(currentStatus)) {
//       return res.status(400).json({ error: `Invalid status: ${statu}. Must be one of: ${validConsignmentStatuses.join(', ')}` });
//     }
    // Compute ETA outside tx (resilient)
    let computedETA = normalizedInput.eta;
    if (!computedETA) {
      try {
        computedETA = await calculateETA(pool, normalizedInput.status);
      } catch (etaErr) {
        console.warn(`ETA calculation failed for status ${normalizedInput.status}; using null fallback`, etaErr);
        computedETA = null;
      }
    }

    // Normalize data
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
    };

    // Dynamic insert
    const fields = Object.keys(normalizedData).map(key => key.replace(/([A-Z])/g, '_$1').toLowerCase());
    const values = Object.values(normalizedData);
    const placeholders = fields.map((_, i) => `$${i + 1}`).join(', ');
    const insertQuery = `INSERT INTO consignments (${fields.join(', ')}) VALUES (${placeholders}) RETURNING *`;

    let newConsignment;
    let insertError = null;
    await withTransaction(async (client) => {
      try {
        const { rows } = await client.query(insertQuery, values);
        newConsignment = rows[0];
      } catch (insertErr) {
        insertError = insertErr;
        if (insertErr.code === '22P02') {
          console.warn(`Enum violation for status '${normalizedInput.status}' during creation. Add: ALTER TYPE consignment_status ADD VALUE '${normalizedInput.status}';`);
        } else if (insertErr.code === '23505') {
          console.warn(`Unique violation during creation:`, insertErr);
        } else {
          throw insertErr;
        }
      }
    });

    // Single response point
    if (insertError) {
      if (insertError.code === '22P02') {
        return res.status(409).json({ error: `Status '${normalizedInput.status}' not recognized in DB enum. Admin fix needed.` });
      } else if (insertError.code === '23505') {
        return res.status(409).json({ error: 'Consignment number already exists' });
      } else {
        throw insertError;
      }
    }

    // Post-tx logging/notifications
    try {
      await logToTracking(null, newConsignment.id, 'created', { status: normalizedInput.status, eta: computedETA });
    } catch (trackingErr) {
      console.warn("Tracking log failed:", trackingErr);
    }
    try {
      if (['Created', 'Submitted', 'Drafts Cleared'].includes(normalizedInput.status)) {
        await sendNotification(newConsignment, 'created');
      }
    } catch (notifErr) {
      console.warn("Notification failed:", notifErr);
    }

    // Enhance response
    newConsignment.statusColor = getStatusColor(newConsignment.status);
    console.log("Consignment created with ID:", newConsignment.id);
    res.status(201).json({ message: 'Consignment created successfully', data: newConsignment });
  } catch (err) {
    console.error("Error creating consignment:", err);
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