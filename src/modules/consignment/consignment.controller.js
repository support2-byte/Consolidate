import pool from "../../db/pool.js";
import { withUserAudit } from "../../middleware/dbAudit.js";
import { calculateETA } from "../../services/calculateEta.js";

function safeParseJsonArray(val) {
  if (!val) return [];
  if (Array.isArray(val)) return val;

  try {
    const parsed = JSON.parse(val);
    if (Array.isArray(parsed)) return parsed;
    // If parsed value is a string, wrap in array
    if (typeof parsed === "string") return [parsed];
  } catch {
    // Fallback: treat as comma-separated string or single value
    return val
      .toString()
      .split(",")
      .map((v) => v.trim())
      .filter(Boolean);
  }

  return [];
}

async function updateLinkedContainersStatus(
  client,
  receiverId,
  newReceiverStatus,
  created_by = "system",
) {
  try {
    const receiverQuery = `SELECT containers FROM receivers WHERE id = $1`;
    const recResult = await client.query(receiverQuery, [receiverId]);

    if (recResult.rowCount === 0 || !recResult.rows[0].containers) {
      return;
    }

    let containers = [];
    const rawContainers = recResult.rows[0].containers;

    if (typeof rawContainers === "string") {
      let cleaned = rawContainers.trim();

      try {
        const parsed = JSON.parse(cleaned);
        if (Array.isArray(parsed)) {
          containers = parsed;
        }
      } catch {
        cleaned = cleaned
          .replace(/^["'\[]+|["'\]]+$/g, "")
          .replace(/["']/g, "")
          .replace(/\s+/g, " ");

        if (cleaned.includes(",")) {
          containers = cleaned
            .split(",")
            .map((s) => s.trim())
            .filter(Boolean);
        } else if (cleaned) {
          containers = [cleaned];
        }
      }
    } else if (Array.isArray(rawContainers)) {
      containers = rawContainers;
    }

    if (containers.length === 0) {
      return;
    }

    const cidQuery = `
      SELECT cid
      FROM container_master
      WHERE container_number = ANY($1)
    `;

    const cidResult = await client.query(cidQuery, [containers]);

    const cids = cidResult.rows
      .map((row) => row.cid)
      .filter((cid) => !isNaN(cid));

    if (cids.length === 0) {
      return;
    }

    console.log(
      `[Container Sync Disabled] Receiver ${receiverId} | Status "${newReceiverStatus}" | CIDs: ${cids.join(", ")}`,
    );

    return;
  } catch (err) {
    console.error(
      `Error processing linked containers for receiver ${receiverId}:`,
      err.message,
    );
  }
}

function isValidDate(dateString) {
  if (!dateString) return false;
  const normalized = dateString.toString().split("T")[0]; // Strip time if full ISO
  const date = new Date(normalized);
  return !isNaN(date.getTime()) && normalized.match(/^\d{4}-\d{2}-\d{2}$/);
}

// Validate core consignment fields (required checks with error messages)
function validateConsignmentFields({
  consignment_number,
  status,
  remarks,
  shipper,
  consignee,
  origin,
  destination,
  eform,
  eform_date,
  bank,
  consignment_value,
  paymentType,
  vessel,
  voyage,
  eta,
  seal_no,
  netWeight,
  gross_weight,
  containers,
  orders,
}) {
  const errors = [];
  if (!consignment_number) errors.push("consignment_number");
  // if (!status) errors.push('status');
  // if (!validConsignmentStatuses.includes(status));
  if (!shipper) errors.push("shipper");
  if (!consignee) errors.push("consignee");
  if (!origin) errors.push("origin");
  if (!destination) errors.push("destination");
  if (!eform || !eform.match(/^[A-Z]{3}-\d{6}$/))
    errors.push(`eform (invalid format, got: "${eform}")`);
  if (!isValidDate(eform_date))
    errors.push(`eform_date (got: "${eform_date}")`);
  // if (!bank) errors.push('bank');
  if (
    consignment_value === undefined ||
    consignment_value < 0 ||
    isNaN(consignment_value)
  )
    errors.push("consignment_value (must be non-negative number)");
  // if (!paymentType) errors.push('paymentType');
  // if (!vessel) errors.push('vessel');
  if (!voyage || voyage.length < 3)
    errors.push(`voyage (min 3 chars, got: "${voyage}")`);
  if (eta && !isValidDate(eta)) errors.push(`eta (got: "${eta}")`);
  // if (!shippingLine) errors.push('shippingLine');  // Optional? Adjust if needed
  // if (seal_no && seal_no.length < 3) errors.push(`seal_no (min 3 chars, got: "${seal_no}")`);  // Optional validation
  // if (netWeight === undefined || netWeight < 0 || isNaN(netWeight)) errors.push('netWeight (must be non-negative number)');
  // if (gross_weight === undefined || gross_weight < 0 || isNaN(gross_weight)) errors.push('gross_weight (must be non-negative number)');
  if (!Array.isArray(containers) || containers.length < 1)
    errors.push("containers (at least one required)");
  if (!Array.isArray(orders) || orders.length < 1)
    errors.push("orders (at least one required)"); // Adjust if optional
  return errors;
}

// Validate containers array items
function validateContainers(containers) {
  return containers
    .map((container, index) => {
      const errors = [];
      if (!container.containerNo)
        errors.push(`containers[${index}].containerNo`);
      // if (!container.size) errors.push(`containers[${index}].size`);
      // if (container.numberOfDays !== undefined && (isNaN(container.numberOfDays) || container.numberOfDays < 0)) {
      // errors.push(`containers[${index}].numberOfDays (must be non-negative number)`);
      // }
      return { index, errors };
    })
    .filter((item) => item.errors.length > 0);
}

// Validate orders array items (fixed: assume orders are objects with quantity)
function validateOrders(orders) {
  console.log("Validating orders:", orders);
  return orders
    .map((order, index) => {
      const errors = [];
      if (!order || typeof order !== "object") {
        errors.push(`orders[${index}]: Must be an object`);
      } else if (
        order.quantity === undefined ||
        order.quantity <= 0 ||
        !Number.isInteger(order.quantity)
      ) {
        errors.push(`orders[${index}].quantity (must be positive integer)`);
      }
      // Add more order-specific validations if needed (e.g., order.id, order.status)
      return { index, errors };
    })
    .filter((item) => item.errors.length > 0);
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

// === Status Color Mapping (Used everywhere: consignment, container, receiver cards) ===
export function getStatusColor(status) {
  if (!status) return "#E0E0E0"; // Fallback for null/undefined

  const colors = {
    // Legacy / Initial
    Draft: "#E0E0E0",
    "Drafts Cleared": "#E0E0E0", // Light gray - draft stage
    "Order Created": "#E0E0E0",
    Created: "#E0E0E0",

    // Submission & Early Processing
    Submitted: "#FFEB3B", // Yellow
    "Submitted On Vessel": "#9C27B0", // Purple (key milestone)
    "Customs Cleared": "#4CAF50", // Green

    // In Transit
    "In Transit": "#4CAF50",
    "In Transit On Vessel": "#4CAF50", // Green
    "Shipment In Transit": "#4CAF50",
    "In Transit": "#4CAF50",

    // Processing
    "Under Shipment Processing": "#FF9800", // Orange
    "Shipment Processing": "#FF9800",
    "Under Processing": "#FF9800",

    // Facility & Ready
    "Arrived at Facility": "#795548", // Brown
    "Arrived at Sort Facility": "#795548",
    "Ready for Delivery": "#FFEB3B", // Yellow
    "Cleared for Delivery": "#FFEB3B",

    // Final Destination
    "Arrived at Destination": "#FFEB3B", // Yellow

    // Completion
    Delivered: "#2196F3", // Blue
    "Shipment Delivered": "#2196F3",
    "Partially Delivered": "#FF9800",

    // Holds & Issues
    HOLD: "#FF9800",
    "HOLD for Delivery": "#FF9800",
    "Under Repair": "#FF9800",

    // Loading
    "Ready for Loading": "#FFEB3B",
    "Loaded Into Container": "#2196F3",
    Loaded: "#2196F3",

    // Container-specific
    Available: "#E0E0E0",
    "Assigned to Job": "#FFEB3B",
    "Assigned to Consignment": "#FFEB3B",
    Occupied: "#4CAF50",
    Hired: "#9C27B0",
    "De-linked": "#F44336",
    Returned: "#795548",

    // Terminal
    Cancelled: "#F44336",
  };

  return colors[status] || "#9E9E9E"; // Medium gray fallback for unknown statuses
}
// Aggregate status from linked data
function aggregateConsignmentStatus(linkedOrders, containers, currentStatus) {
  if (["HOLD", "Cancelled"].includes(currentStatus)) return currentStatus; // No aggregation for held/cancelled

  // Shipment/Order aggregation (C → A mapping)
  const orderStatuses = linkedOrders.map((o) => o.order_status).filter(Boolean);
  if (orderStatuses.length === 0) return currentStatus;

  const statusCounts = {};
  orderStatuses.forEach((s) => (statusCounts[s] = (statusCounts[s] || 0) + 1));
  const dominantOrderStatus = Object.keys(statusCounts).reduce((a, b) =>
    statusCounts[a] > statusCounts[b] ? a : b,
  );

  const orderToConsignmentMap = {
    "Order Created": "Drafts Cleared",
    "Ready for Loading": "Drafts Cleared",
    "Loaded Into Container": "Submitted On Vessel",
    "Shipment Processing": "Submitted On Vessel",
    "Shipment In Transit": "In Transit On Vessel",
    "Under Processing": "In Transit On Vessel",
    "Arrived at Sort Facility": "Ready for Delivery",
    "Ready for Delivery": "Ready for Delivery",
    "Shipment Delivered": "Delivered",
    "Partially Delivered": "HOLD for Delivery", // Partial → Hold for rest
  };
  let fromOrders = orderToConsignmentMap[dominantOrderStatus] || currentStatus;

  // Container aggregation (B → A mapping) - Prioritize if conflicting
  const containerStatuses = (containers || [])
    .map((c) => c.status)
    .filter(Boolean);
  if (containerStatuses.length > 0) {
    const containerMap = {
      Available: "Drafts Cleared",
      Hired: "Drafts Cleared",
      Occupied: "Submitted On Vessel",
      "In Transit": "In Transit On Vessel",
      Loaded: "In Transit On Vessel",
      "Assigned to Job": "Submitted On Vessel",
      "Assigned to Consignment": "Submitted On Vessel",
      "De-linked": "HOLD",
      "Under Repair": "HOLD",
      Returned: "HOLD for Delivery",
      "Cleared for Delivery": "Ready for Delivery",
      "Ready for Delivery": "Ready for Delivery",
      "Shipment for Processing": "Submitted On Vessel",
      "Under Processing": "Submitted On Vessel",
    };
    const dominantContainerStatus = containerStatuses.reduce((acc, s) => {
      acc[s] = (acc[s] || 0) + 1;
      return acc;
    }, {});
    const topContainer = Object.keys(dominantContainerStatus).reduce((a, b) =>
      dominantContainerStatus[a] > dominantContainerStatus[b] ? a : b,
    );
    let fromContainers = containerMap[topContainer] || currentStatus;

    // Resolve conflict: Container overrides order if more advanced (e.g., In Transit > Processing)
    const priorityOrder = [
      "Drafts Cleared",
      "Submitted On Vessel",
      "In Transit On Vessel",
      "Ready for Delivery",
      "Delivered",
    ];
    if (
      priorityOrder.indexOf(fromContainers) > priorityOrder.indexOf(fromOrders)
    ) {
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
    .map((item) => {
      let id = null;
      if (typeof item === "number") {
        id = item;
      } else if (typeof item === "object" && item !== null) {
        if ("id" in item && item.id !== null && item.id !== undefined) {
          id = parseInt(item.id, 10);
        } else if ("value" in item || "key" in item) {
          const val = item.value || item.key;
          id = parseInt(val, 10);
        }
      } else if (typeof item === "string") {
        id = parseInt(item, 10);
      }
      return id;
    })
    .filter((id) => Number.isInteger(id) && id > 0);

  return ids;
}

async function tableExists(client, tableName) {
  try {
    const result = await client.query(
      `
      SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = $1
      );
    `,
      [tableName],
    );
    return result.rows[0].exists;
  } catch (err) {
    console.warn(`Error checking table existence for ${tableName}:`, err);
    return false;
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
  console.log("Fetching consignment:", req.params);

  try {
    const { id } = req.params;
    const { autoSync = "false" } = req.query;
    const enableAutoSync = autoSync === "true";
    let orderIds = [];
    const numericId = parseInt(id, 10);
    if (isNaN(numericId) || numericId <= 0) {
      return res.status(400).json({ error: "Invalid consignment ID." });
    }

    const client = await pool.connect();
    let consignment = null;
    let containers = [];

    try {
      await client.query("BEGIN");

      const consRes = await client.query(
        "SELECT * FROM consignments WHERE id = $1",
        [numericId],
      );
      if (consRes.rowCount === 0) {
        await client.query("ROLLBACK");
        return res.status(404).json({ error: "Consignment not found" });
      }
      consignment = consRes.rows[0];

      if (consignment.orders) {
        let rawOrders =
          typeof consignment.orders === "string"
            ? JSON.parse(consignment.orders)
            : consignment.orders;
        orderIds = Array.isArray(rawOrders)
          ? rawOrders
              .map((o) => parseInt(o, 10))
              .filter((o) => !isNaN(o) && o > 0)
          : [];
      }

      const statusRes = await client.query(
        "SELECT order_status, sorting_number FROM statuses WHERE order_status IS NOT NULL",
      );

      const statusPriority = statusRes.rows.reduce((acc, row) => {
        if (row.order_status) {
          acc[row.order_status] = row.sorting_number || 0;
        }
        return acc;
      }, {});

      let linkedOrders = [];
      let minReceiverEta = null;
      let mostAdvancedReceiverStatus = null;

      if (orderIds.length > 0) {
        const orderRes = await client.query(
          `
          SELECT 
            id, 
            sender_name AS shipper, 
            receiver_name AS consignee, 
            eta AS order_eta, 
            etd, 
            qty_delivered AS delivered, 
            total_assigned_qty, 
            status AS order_status
          FROM orders 
          WHERE id = ANY($1::int[])
        `,
          [orderIds],
        );
        linkedOrders = orderRes.rows;

        const receiverRes = await client.query(
          `
          SELECT status, eta 
          FROM receivers 
          WHERE order_id = ANY($1::int[])
        `,
          [orderIds],
        );

        const receivers = receiverRes.rows;

        const validEtas = receivers
          .filter((r) => r.eta)
          .map((r) => new Date(r.eta));
        if (validEtas.length > 0) {
          minReceiverEta = new Date(Math.min(...validEtas));
        }

        mostAdvancedReceiverStatus = receivers.reduce((best, curr) => {
          const p = statusPriority[curr.status] || 0;
          const bp = statusPriority[best?.status] || 0;
          return p > bp ? curr : best;
        }, null)?.status;
      }

      consignment.statusColor = getStatusColor(consignment.status);

      if (linkedOrders.length > 0) {
        const first = linkedOrders[0];
        consignment.shipper = first.shipper || consignment.shipper;
        consignment.consignee = first.consignee || consignment.consignee;
        consignment.etd = first.etd ? normalizeDate(first.etd) : null;

        const totalAssigned = linkedOrders.reduce(
          (sum, o) => sum + (o.total_assigned_qty || 0),
          0,
        );
        const totalDelivered = linkedOrders.reduce(
          (sum, o) => sum + (o.delivered || 0),
          0,
        );
        consignment.delivered = totalDelivered;
        consignment.pending = Math.max(0, totalAssigned - totalDelivered);
        consignment.orders = linkedOrders;
      }

      if (minReceiverEta) {
        consignment.eta = minReceiverEta.toISOString().split("T")[0];
      }

      const today = new Date();
      today.setHours(0, 0, 0, 0);
      if (consignment.eta) {
        const etaDate = new Date(consignment.eta);
        etaDate.setHours(0, 0, 0, 0);
        consignment.days_until_eta = Math.max(
          0,
          Math.ceil((etaDate - today) / 86400000),
        );
      }

      if (
        typeof consignment.shipping_line === "number" &&
        consignment.shipping_line > 0
      ) {
        const { rows: slRows } = await client.query(
          "SELECT name FROM shipping_lines WHERE id = $1",
          [consignment.shipping_line],
        );
        consignment.shipping_line =
          slRows[0]?.name || consignment.shipping_line;
      }

      if (mostAdvancedReceiverStatus && enableAutoSync) {
        const suggested = Object.entries(CONSIGNMENT_TO_STATUS_MAP || {}).find(
          ([_, v]) => v.shipment === mostAdvancedReceiverStatus,
        )?.[0];

        if (suggested && suggested !== consignment.status) {
          consignment.suggested_status = suggested;
          consignment.suggested_status_reason = `Based on receiver status: ${mostAdvancedReceiverStatus}`;
        }
      }

      await client.query("COMMIT");
    } catch (innerErr) {
      await client.query("ROLLBACK");
      throw innerErr;
    } finally {
      client.release();
    }

    if (orderIds.length > 0) {
      const containerClient = await pool.connect();
      try {
        console.log(
          `Fetching containers for consignment ${numericId} with order IDs:`,
          orderIds,
        );

        const containerRes = await containerClient.query(
          `
          SELECT 
            cm.cid AS id,
            cm.container_size          AS size,
            cm.container_number        AS "containerNo",
            cm.container_type          AS "containerType",
            cm.owner_type               AS ownership,
            COALESCE(cm.location, 'N/A') AS location,
            COALESCE(
              (SELECT cs.availability 
              FROM container_status cs 
              WHERE cs.cid = cm.cid 
              ORDER BY cs.created_time DESC 
              LIMIT 1),
              cm.derived_status,
              'Available'
            ) AS derived_status
          FROM container_master cm
          WHERE cm.cid IN (
            SELECT cch.container_id
            FROM container_consignment_history cch
            WHERE cch.consignment_id = $1
          )
          `,
          [numericId],
        );

        containers = containerRes.rows.map((row) => {
          const ds = row.derived_status || "Available";
          return {
            id: row.id,
            size: row.size,
            containerNo: row.containerNo,
            containerType: row.containerType,
            ownership: row.ownership,
            location: row.location,
            derived_status: ds,
            derived_status_color: getStatusColor(ds),
            status: ds,
            statusColor: getStatusColor(ds),
          };
        });

        console.log(
          `Fetched ${containers.length} containers for consignment ${numericId}`,
        );
      } catch (containerErr) {
        console.error(
          `Failed to fetch containers for consignment ${numericId}:`,
          containerErr.message,
          containerErr.stack,
        );
        containers = []; // safe fallback
      } finally {
        containerClient.release();
      }
    } else {
      console.log(
        `No order IDs → skipping containers for consignment ${numericId}`,
      );
    }

    consignment.containers = containers;
    res.json({ data: consignment });
  } catch (err) {
    console.error("Error fetching consignment:", err.stack || err);
    res.status(500).json({ error: "Failed to fetch consignment" });
  }
}

// export async function getConsignmentById(req, res) {
//   try {
//     const { id } = req.params;
//     const { autoSync = "false" } = req.query;
//     const enableAutoSync = autoSync === "true";
//     let orderIds = [];
//     const numericId = parseInt(id, 10);

//     if (isNaN(numericId) || numericId <= 0) {
//       return res.status(400).json({ error: "Invalid consignment ID." });
//     }

//     const client = await pool.connect();
//     let consignment = null;
//     let containers = [];

//     try {
//       await client.query("BEGIN");

//       const consRes = await client.query(
//         "SELECT * FROM consignments WHERE id = $1",
//         [numericId],
//       );

//       if (consRes.rowCount === 0) {
//         await client.query("ROLLBACK");
//         return res.status(404).json({ error: "Consignment not found" });
//       }

//       consignment = consRes.rows[0];

//       if (consignment.orders) {
//         let rawOrders =
//           typeof consignment.orders === "string"
//             ? JSON.parse(consignment.orders)
//             : consignment.orders;
//         orderIds = Array.isArray(rawOrders)
//           ? rawOrders
//               .map((o) => parseInt(o, 10))
//               .filter((o) => !isNaN(o) && o > 0)
//           : [];
//       }

//       const statusRes = await client.query(
//         "SELECT order_status, sorting_number FROM statuses WHERE order_status IS NOT NULL",
//       );

//       const statusPriority = statusRes.rows.reduce((acc, row) => {
//         if (row.order_status) {
//           acc[row.order_status] = row.sorting_number || 0;
//         }
//         return acc;
//       }, {});

//       let linkedOrders = [];
//       let minReceiverEta = null;
//       let mostAdvancedReceiverStatus = null;

//       if (orderIds.length > 0) {
//         const orderRes = await client.query(
//           `SELECT
//             id,
//             sender_name AS shipper,
//             receiver_name AS consignee,
//             eta AS order_eta,
//             etd,
//             qty_delivered AS delivered,
//             total_assigned_qty,
//             status AS order_status
//           FROM orders
//           WHERE id = ANY($1::int[])`,
//           [orderIds],
//         );
//         linkedOrders = orderRes.rows;

//         const receiverRes = await client.query(
//           `SELECT status, eta FROM receivers WHERE order_id = ANY($1::int[])`,
//           [orderIds],
//         );

//         const receivers = receiverRes.rows;

//         const validEtas = receivers
//           .filter((r) => r.eta)
//           .map((r) => new Date(r.eta));
//         if (validEtas.length > 0) {
//           minReceiverEta = new Date(Math.min(...validEtas));
//         }

//         mostAdvancedReceiverStatus = receivers.reduce((best, curr) => {
//           const p = statusPriority[curr.status] || 0;
//           const bp = statusPriority[best?.status] || 0;
//           return p > bp ? curr : best;
//         }, null)?.status;
//       }

//       consignment.statusColor = getStatusColor(consignment.status);

//       if (linkedOrders.length > 0) {
//         const first = linkedOrders[0];
//         consignment.shipper = first.shipper || consignment.shipper;
//         consignment.consignee = first.consignee || consignment.consignee;
//         consignment.etd = first.etd ? normalizeDate(first.etd) : null;

//         const totalAssigned = linkedOrders.reduce(
//           (sum, o) => sum + (o.total_assigned_qty || 0),
//           0,
//         );
//         const totalDelivered = linkedOrders.reduce(
//           (sum, o) => sum + (o.delivered || 0),
//           0,
//         );
//         consignment.delivered = totalDelivered;
//         consignment.pending = Math.max(0, totalAssigned - totalDelivered);
//         consignment.orders = linkedOrders;
//       }

//       if (minReceiverEta) {
//         consignment.eta = minReceiverEta.toISOString().split("T")[0];
//       }

//       const today = new Date();
//       today.setHours(0, 0, 0, 0);
//       if (consignment.eta) {
//         const etaDate = new Date(consignment.eta);
//         etaDate.setHours(0, 0, 0, 0);
//         consignment.days_until_eta = Math.max(
//           0,
//           Math.ceil((etaDate - today) / 86400000),
//         );
//       }

//       if (
//         typeof consignment.shipping_line === "number" &&
//         consignment.shipping_line > 0
//       ) {
//         const { rows: slRows } = await client.query(
//           "SELECT name FROM shipping_lines WHERE id = $1",
//           [consignment.shipping_line],
//         );
//         consignment.shipping_line =
//           slRows[0]?.name || consignment.shipping_line;
//       }

//       if (mostAdvancedReceiverStatus && enableAutoSync) {
//         const suggested = Object.entries(CONSIGNMENT_TO_STATUS_MAP || {}).find(
//           ([_, v]) => v.shipment === mostAdvancedReceiverStatus,
//         )?.[0];

//         if (suggested && suggested !== consignment.status) {
//           consignment.suggested_status = suggested;
//           consignment.suggested_status_reason = `Based on receiver status: ${mostAdvancedReceiverStatus}`;
//         }
//       }

//       await client.query("COMMIT");
//     } catch (innerErr) {
//       await client.query("ROLLBACK");
//       throw innerErr;
//     } finally {
//       client.release();
//     }

//     if (orderIds.length > 0) {
//       const containerClient = await pool.connect();
//       try {
//         const containerRes = await containerClient.query(
//           `SELECT
//             cm.cid                            AS id,
//             cm.container_size                 AS size,
//             cm.container_number               AS "containerNo",
//             cm.container_type                 AS "containerType",
//             cm.owner_type                     AS ownership,
//             COALESCE(cm.status, 'Available')  AS status,
//             COALESCE(
//               (SELECT cs.location
//               FROM container_status cs
//               WHERE cs.cid = cm.cid
//                 AND cs.location IS NOT NULL
//                 AND cs.location != ''
//               ORDER BY cs.created_time DESC
//               LIMIT 1),
//               'N/A'
//             ) AS location
//           FROM container_master cm
//           WHERE cm.cid IN (
//             SELECT DISTINCT cch.container_id
//             FROM container_consignment_history cch
//             WHERE cch.consignment_id = $1
//               AND cch.container_id IS NOT NULL
//           )`,
//           [numericId],
//         );

//         containers = containerRes.rows.map((row) => ({
//           id: row.id,
//           size: row.size,
//           containerNo: row.containerNo,
//           containerType: row.containerType,
//           ownership: row.ownership,
//           location: row.location,
//           status: row.status,
//           statusColor: getStatusColor(row.status),
//         }));
//       } catch (containerErr) {
//         containers = [];
//       } finally {
//         containerClient.release();
//       }
//     }

//     consignment.containers = containers;
//     res.json({ data: consignment });
//   } catch (err) {
//     res.status(500).json({ error: "Failed to fetch consignment" });
//   }
// }

async function sendNotification(consignmentData, event = "created") {
  // e.g., await emailService.send({ to: consignmentData.consignee.email, subject: `Consignment ${consignmentData.consignment_number} ${event}` });
  console.log(
    `Notification sent for consignment ${consignmentData.consignment_number}: ${event}`,
  );
}

async function logToTracking(
  client,
  consignmentId,
  eventType = "unknown",
  logData = {},
) {
  // Validate eventType (required, non-null)
  if (!eventType || typeof eventType !== "string" || eventType.trim() === "") {
    console.error(
      `Invalid eventType '${eventType}' for consignment ${consignmentId} – defaulting to 'unknown_event'`,
    );
    eventType = "unknown_event"; // Fallback to avoid NULL violation
  }

  // Validate against schema CHECK (expand as needed)
  const validEvents = [
    "status_advanced",
    "status_updated",
    "status_auto_updated",
    "updated",
    "order_synced",
  ];
  if (!validEvents.includes(eventType)) {
    console.warn(
      `Event '${eventType}' not in DB CHECK – add to constraint or use valid one`,
    );
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
      action: logData.action || eventType, // Legacy: Store 'action' in details if passed
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
      eventType.trim(), // Ensure non-null string
      oldStatus,
      newStatus,
      offsetDays,
      details,
    ]);

    if (result.rowCount > 0) {
      console.log(
        `✓ Logged '${eventType}' for ${consignmentId} (ID: ${result.rows[0].id})`,
      );
      return { success: true, id: result.rows[0].id };
    } else {
      console.log(`⚠ Duplicate '${eventType}' skipped for ${consignmentId}`);
      return { success: true, skipped: true };
    }
  } catch (error) {
    console.error(`Failed to log '${eventType}' for ${consignmentId}:`, error);
    if (error.code === "23502") {
      console.error("NOT NULL violation on event_type – ensure non-null param");
    } else if (error.code === "23514") {
      console.error(
        `CHECK violation: '${eventType}' not allowed – update DB constraint`,
      );
    }
    return { success: false, error: error.message };
  }
}

async function withTransaction(operation) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const result = await operation(client);
    await client.query("COMMIT");
    return result;
  } catch (err) {
    await client.query("ROLLBACK");
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
      { value: "HOLD", label: "HOLD", color: getStatusColor("HOLD") },
      {
        value: "Cancelled",
        label: "Cancelled",
        color: getStatusColor("Cancelled"),
      },
      {
        value: "Drafts Cleared",
        label: "Drafts Cleared",
        color: getStatusColor("Drafts Cleared"),
      },
      {
        value: "Submitted On Vessel",
        label: "Submitted On Vessel",
        color: getStatusColor("Submitted On Vessel"),
      },
      {
        value: "Customs Cleared",
        label: "Customs Cleared",
        color: getStatusColor("Customs Cleared"),
      },
      {
        value: "Submitted",
        label: "Submitted",
        color: getStatusColor("Submitted"),
      },
      {
        value: "Under Shipment Processing",
        label: "Under Shipment Processing",
        color: getStatusColor("Under Shipment Processing"),
      },
      {
        value: "In Transit",
        label: "In Transit",
        color: getStatusColor("In Transit"),
      },
      {
        value: "Arrived at Facility",
        label: "Arrived at Facility",
        color: getStatusColor("Arrived at Facility"),
      },
      {
        value: "Ready for Delivery",
        label: "Ready for Delivery",
        color: getStatusColor("Ready for Delivery"),
      },
      {
        value: "Arrived at Destination",
        label: "Arrived at Destination",
        color: getStatusColor("Arrived at Destination"),
      },
      {
        value: "Delivered",
        label: "Delivered",
        color: getStatusColor("Delivered"),
      },
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
      dbStatuses = result.rows.map((row) => ({
        ...row,
        label: row.value,
        color: getStatusColor(row.value) || "#000000", // Fallback if unknown
      }));
    } catch (dbErr) {
      console.warn(
        "Failed to fetch DB statuses (non-critical); using full list:",
        dbErr,
      );
      // Continue without DB data—full list still returned
    }

    // Merge: Enhance full list with usage_count from DB
    const statuses = fullStatuses.map((full) => {
      const dbMatch = dbStatuses.find((db) => db.value === full.value);
      return {
        ...full,
        usage_count: dbMatch ? dbMatch.usage_count : 0,
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
        {
          value: "Drafts Cleared",
          label: "Drafts Cleared",
          color: "#E0E0E0",
          usage_count: 0,
        },
      ],
    });
  }
}

export async function getConsignments(req, res) {
  try {
    const {
      page = 1,
      limit = 10,
      order_by = "created_at",
      order = "desc",
      consignment_id = "",
      container_number = "",
      status = "",
    } = req.query;

    const offset = (parseInt(page) - 1) * parseInt(limit);
    const validOrderBys = [
      "id",
      "consignment_number",
      "status",
      "s.name",
      "c.name", // Updated: Use joined aliases for sorting
      "eta",
      "created_at",
      "gross_weight",
      "delivered",
      "pending",
    ];
    const safeOrderBy = validOrderBys.includes(order_by)
      ? order_by === "shipper"
        ? "s.name"
        : order_by === "consignee"
          ? "c.name"
          : order_by
      : "created_at"; // Map frontend keys to joined fields
    const safeOrder = order.toLowerCase() === "asc" ? "ASC" : "DESC";

    let baseQuery = `
      SELECT 
        cons.id,
        cons.consignment_number,
        cons.status,
        COALESCE(shipper_tp.company_name, cons.shipper) AS shipper,
        COALESCE(consignee_tp.company_name, cons.consignee) AS consignee,
        cons.eta,
        cons.created_at,
        cons.gross_weight,
        cons.orders,
        cons.delivered,
        cons.pending
      FROM consignments cons
      LEFT JOIN third_parties shipper_tp
        ON cons.shipper_id = shipper_tp.id
      LEFT JOIN third_parties consignee_tp
        ON cons.consignee_id = consignee_tp.id
    `;
    let whereClauses = [];
    let queryParams = [];

    if (consignment_id.trim()) {
      whereClauses.push(
        `cons.consignment_number ILIKE $${queryParams.length + 1}`,
      );
      queryParams.push(`%${consignment_id.trim()}%`);
    }
    if (container_number.trim()) {
      whereClauses.push(`
        EXISTS (
          SELECT 1
          FROM jsonb_array_elements(cons.containers) container
          WHERE container->>'containerNo' ILIKE $${queryParams.length + 1}
        )
      `);

      queryParams.push(`%${container_number.trim()}%`);
    }

    if (status.trim()) {
      whereClauses.push(`cons.status = $${queryParams.length + 1}`);
      queryParams.push(status.trim());
    }

    let whereClause = "";
    if (whereClauses.length > 0) {
      whereClause = ` WHERE ${whereClauses.join(" AND ")}`;
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
    res.status(500).json({ error: "Failed to fetch consignments" });
  }
}

export async function createConsignment(req, res) {
  try {
    const data = req.body;

    const input = {
      user_id: data.user_id,
      consignment_number: data.consignment_number || data.consignmentNumber,
      remarks: data.remarks || "",
      shipper: data.shipper || "",
      consignee: data.consignee || "",
      shipper_id: data.shipper_id ? parseInt(data.shipper_id) : null,
      consignee_id: data.consignee_id ? parseInt(data.consignee_id) : null,
      shipper_address: data.shipper_address || "",
      consignee_address: data.consignee_address || "",
      origin: data.origin || "",
      destination: data.destination || "",
      eform: data.eform || "",
      eform_date: data.eform_date || data.eformDate,
      bank: data.bank || "",
      bank_id: data.bank_id ? parseInt(data.bank_id) : null,
      consignment_value: data.consignment_value || data.consignmentValue || 0,
      payment_type: data.payment_type || data.paymentType || "Collect",
      vessel: data.vessel ? parseInt(data.vessel) : null,
      voyage: data.voyage || "",
      eta: data.eta?.trim() || null,
      shipping_line: data.shipping_line || data.shippingLine || "",
      seal_no: data.seal_no || data.sealNo || "",
      net_weight: data.net_weight || data.netWeight || 0,
      gross_weight: data.gross_weight || data.grossWeight || 0,
      currency_code: data.currency_code || data.currencyCode || "USD",
      delivered: data.delivered || 0,
      pending: data.pending || 0,
      containers: Array.isArray(data.containers) ? data.containers : [],
      orders: Array.isArray(data.orders)
        ? data.orders.map((id) => parseInt(id))
        : [],
    };

    const validationErrors = validateConsignmentFields(input);
    if (validationErrors.length > 0) {
      return res
        .status(400)
        .json({ error: "Validation failed", details: validationErrors });
    }

    const containerErrors = validateContainers(input.containers);
    if (containerErrors.length > 0) {
      return res.status(400).json({
        error: "Container validation failed",
        details: containerErrors,
      });
    }

    const dbData = {
      consignment_number: input.consignment_number,
      status: "Draft",
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
      eta: input.eta ? normalizeDate(input.eta) : null,
      shipping_line_name: input.shipping_line,
      seal_no: input.seal_no,
      net_weight: input.net_weight,
      gross_weight: input.gross_weight,
      currency_code: input.currency_code,
      delivered: input.delivered,
      pending: input.pending,
      containers: JSON.stringify(input.containers),
      orders: JSON.stringify(input.orders),
    };

    const keys = Object.keys(dbData);
    const values = Object.values(dbData);
    const placeholders = keys.map((_, i) => `$${i + 1}`).join(", ");
    const columns = keys.join(", ");
    const insertQuery = `INSERT INTO consignments (${columns}) VALUES (${placeholders}) RETURNING *`;

    let newConsignment = null;
    let ccNew = [];

    await withTransaction(async (client) => {
      const result = await withUserAudit(req, insertQuery, values);
      newConsignment = result.rows[0];

      if (input.containers.length === 0) return;

      const chValues = [];
      const chPlaceholders = input.containers.map((container, index) => {
        const o = index * 6;
        chValues.push(
          newConsignment.id,
          container.cid,
          new Date(),
          input.eform_date,
          parseInt(input.user_id),
          true,
        );
        return `($${o + 1}, $${o + 2}, $${o + 3}, $${o + 4}, $${o + 5}, $${o + 6})`;
      });

      const chResult = await client.query(
        `INSERT INTO container_consignment_history
           (consignment_id, container_id, assigned_at, created_at, created_by, active)
         VALUES ${chPlaceholders.join(",")}
         RETURNING *`,
        chValues,
      );
      ccNew = chResult.rows;

      if (ccNew.length === 0) return;

      const linkValues = [];
      const linkTuples = ccNew.map((row, index) => {
        const o = index * 2;
        linkValues.push(
          parseInt(row.container_id),
          parseInt(row.consignment_id),
        );
        return `($${o + 1}, $${o + 2})`;
      });
      const orderIds = input.orders.map((id) => parseInt(id));

      await client.query(
        `UPDATE container_assignment_history cah
         SET consignment_id = v.consignment_id::integer
         FROM (VALUES ${linkTuples.join(",")}) AS v(cid, consignment_id)
         WHERE cah.cid = v.cid::integer
           AND cah.action_type = 'ASSIGN'
           AND cah.order_id = ANY($${linkValues.length + 1}::int[])`,
        [...linkValues, orderIds],
      );

      const currentStatusResult = await client.query(
        `SELECT status FROM container_assignment_history WHERE consignment_id = $1 LIMIT 1`,
        [newConsignment.id],
      );
      const currentStatus = currentStatusResult.rows?.[0]?.status || null;

      if (currentStatus) {
        const statusesResult = await client.query(
          `SELECT container_status, sorting_number FROM statuses
           WHERE status = true AND container_status IS NOT NULL
           ORDER BY sorting_number ASC`,
        );
        const statuses = statusesResult.rows;
        const currentIndex = statuses.findIndex(
          (s) => s.container_status === currentStatus,
        );
        const nextStatus =
          currentIndex !== -1 && currentIndex < statuses.length - 1
            ? statuses[currentIndex + 1].container_status
            : null;

        if (nextStatus) {
          await client.query(
            `UPDATE container_assignment_history SET status = $1 WHERE consignment_id = $2`,
            [nextStatus, newConsignment.id],
          );
        }
      }

      const containerIds = ccNew.map((row) => parseInt(row.container_id));
      await client.query(
        `UPDATE container_status SET availability = 'Occupied', created_time = NOW() WHERE cid = ANY($1::int[])`,
        [containerIds],
      );
    });

    res.status(201).json({
      message: "Consignment created successfully",
      data: newConsignment,
    });
  } catch (err) {
    console.error("Error creating consignment:", err);

    if (err.code === "23505") {
      return res
        .status(409)
        .json({ error: "Consignment number already exists" });
    }

    res
      .status(500)
      .json({ error: "Failed to create consignment", details: err.message });
  }
}

export async function updateConsignment(req, res) {
  const { id } = req.params;
  try {
    const data = req.body;

    const input = {
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
      payment_type: data.paymentType || data.payment_type,
      vessel: data.vessel,
      voyage: data.voyage,
      eta: data.eta,
      shipping_line_name: data.shippingLine || data.shipping_line,
      seal_no: data.seal_no || data.sealNo,
      net_weight: data.netWeight || data.net_weight,
      gross_weight: data.gross_weight || data.grossWeight,
      currency_code: data.currency_code || data.currencyCode,
      delivered: data.delivered || 0,
      pending: data.pending || 0,
      containers: data.containers || [],
      orders: data.orders || [],
    };

    const validationErrors = validateConsignmentFields(input);
    if (validationErrors.length > 0) {
      return res
        .status(400)
        .json({ error: "Validation failed", details: validationErrors });
    }

    const containerErrors = validateContainers(input.containers);
    let orderErrors = [];
    if (!Array.isArray(input.orders)) {
      orderErrors = [{ index: -1, errors: ["orders: Must be an array"] }];
    } else if (
      input.orders.length > 0 &&
      input.orders.every((o) => typeof o === "number" && o > 0)
    ) {
      if (input.orders.some((id) => isNaN(id) || id <= 0)) {
        orderErrors = [
          { index: -1, errors: ["orders: All IDs must be positive integers"] },
        ];
      }
    } else {
      orderErrors = validateOrders(input.orders);
    }

    if (containerErrors.length > 0 || orderErrors.length > 0) {
      return res.status(400).json({
        error: "Array validation failed",
        details: [...containerErrors, ...orderErrors],
      });
    }

    const userId = data.user_id || req.user?.id;
    if (!userId) {
      return res
        .status(400)
        .json({ error: "user_id is required for audit trail" });
    }

    const newContainerIds = new Set(
      input.containers
        .map((c) => (c.cid != null ? parseInt(c.cid, 10) : null))
        .filter((cid) => cid != null && !isNaN(cid)),
    );

    const normalizedData = {
      consignment_number: input.consignment_number,
      status: input.status,
      remarks: input.remarks,
      shipper: input.shipper,
      shipper_address: input.shipper_address,
      consignee: input.consignee,
      consignee_address: input.consignee_address,
      origin: input.origin,
      destination: input.destination,
      eform: input.eform,
      eform_date: normalizeDate(input.eform_date),
      bank: input.bank,
      consignment_value: input.consignment_value,
      payment_type: input.payment_type,
      vessel: input.vessel,
      voyage: input.voyage,
      eta: normalizeDate(input.eta),
      shipping_line_name: input.shipping_line_name,
      seal_no: input.seal_no,
      net_weight: input.net_weight,
      gross_weight: input.gross_weight,
      currency_code: input.currency_code,
      delivered: input.delivered,
      pending: input.pending,
      containers: JSON.stringify(input.containers),
      orders: JSON.stringify(input.orders),
    };

    const updateFields = Object.keys(normalizedData);
    const setClause = updateFields
      .map((key, i) => `${key} = $${i + 2}`)
      .join(", ");
    const values = [id, ...updateFields.map((key) => normalizedData[key])];
    const query = `UPDATE consignments SET ${setClause}, updated_at = CURRENT_TIMESTAMP WHERE id = $1 RETURNING *`;

    let updatedConsignment;
    let computedETA = input.eta;
    const containerDiffSummary = { released: [], added: [] };

    await withTransaction(async (client) => {
      if (!computedETA || data.status !== undefined) {
        const statusRow = await client.query(
          `SELECT order_status FROM statuses WHERE consignment_status = $1 AND status = true LIMIT 1`,
          [input.status || data.status],
        );
        const resolvedStatus =
          statusRow.rows[0]?.order_status ?? input.status ?? data.status;
        const etaResult = await calculateETA(client, resolvedStatus);
        computedETA = etaResult.eta;

        const etaIndex = updateFields.indexOf("eta");
        if (etaIndex !== -1) values[etaIndex + 1] = normalizeDate(computedETA);
      }

      const updateResult = await withUserAudit(req, query, values);
      if (updateResult.rowCount === 0) throw new Error("Consignment not found");
      updatedConsignment = updateResult.rows[0];

      const deleteResult = await client.query(
        `DELETE FROM container_consignment_history WHERE consignment_id = $1 RETURNING container_id`,
        [id],
      );
      containerDiffSummary.released = deleteResult.rows.map(
        (r) => r.container_id,
      );

      for (const cid of newContainerIds) {
        const insertResult = await client.query(
          `INSERT INTO container_consignment_history
             (container_id, consignment_id, assigned_at, created_at, created_by, active)
           VALUES ($1, $2, NOW(), NOW(), $3, true)
           RETURNING container_id`,
          [cid, id, userId],
        );
        if (insertResult.rowCount > 0) containerDiffSummary.added.push(cid);
      }

      if (Array.isArray(data.assignments)) {
        const byOrderItem = data.assignments.reduce((acc, a) => {
          if (!a.shippingDetailId) return acc;
          (acc[a.shippingDetailId] ||= []).push(a);
          return acc;
        }, {});

        for (const [orderItemId, itemAssignments] of Object.entries(
          byOrderItem,
        )) {
          const newContainerDetails = itemAssignments.map((a) => ({
            container: { cid: a.containerCid, container_number: a.containerNo },
            assign_weight: a.assignedWeight,
            assign_total_box: a.assignedBoxes,
          }));

          await client.query(
            `UPDATE order_items SET container_details = $1 WHERE id = $2`,
            [JSON.stringify(newContainerDetails), orderItemId],
          );
        }
      }

      if (data.status !== undefined) {
        const logResult = await logToTracking(client, id, "status_updated", {
          newStatus: input.status,
          eta: computedETA,
        });
        if (!logResult.success) {
          console.warn(
            `Tracking log failed for consignment ${id}:`,
            logResult.error,
          );
        }
      }

      if (["In Transit", "Delivered"].includes(input.status)) {
        await sendNotification(updatedConsignment, "updated");
      }
    });

    res.status(200).json({
      message: "Consignment updated",
      data: {
        ...updatedConsignment,
        statusColor: getStatusColor(updatedConsignment.status),
        shipperAddress: updatedConsignment.shipper_address || "",
        consigneeAddress: updatedConsignment.consignee_address || "",
        paymentType: updatedConsignment.payment_type || "",
        shippingLine: updatedConsignment.shipping_line || null,
        netWeight: updatedConsignment.net_weight || "0.00",
        containerDiff: {
          released: containerDiffSummary.released,
          added: containerDiffSummary.added,
          noChange: [...newContainerIds].filter(
            (cid) => !containerDiffSummary.added.includes(cid),
          ),
        },
      },
    });
  } catch (err) {
    console.error("Error updating consignment:", err);

    if (err.message === "Consignment not found") {
      return res.status(404).json({ error: "Consignment not found" });
    }
    if (err.code === "23505") {
      return res
        .status(409)
        .json({ error: "Consignment number already exists" });
    }
    if (err.code === "23503") {
      return res.status(400).json({
        error: "Foreign key violation — container or user ID not found",
        details: err.detail,
      });
    }

    res
      .status(500)
      .json({ error: "Failed to update consignment", details: err.message });
  }
}

// export async function updateConsignment(req, res) {
//   const { id } = req.params;
//   try {
//     const data = req.body;

//     const normalizedInput = {
//       consignment_number: data.consignment_number || data.consignmentNumber,
//       status: data.status,
//       remarks: data.remarks,
//       shipper: data.shipper,
//       shipper_address: data.shipper_address || data.shipperAddress,
//       consignee: data.consignee,
//       consignee_address: data.consignee_address || data.consigneeAddress,
//       origin: data.origin,
//       destination: data.destination,
//       eform: data.eform,
//       eform_date: data.eform_date || data.eformDate,
//       bank: data.bank,
//       consignment_value: data.consignment_value || data.consignmentValue,
//       paymentType: data.paymentType || data.payment_type,
//       vessel: data.vessel,
//       voyage: data.voyage,
//       eta: data.eta,
//       shipping_line_name: data.shippingLine || data.shipping_line,
//       seal_no: data.seal_no || data.sealNo,
//       netWeight: data.netWeight || data.net_weight,
//       gross_weight: data.gross_weight || data.grossWeight,
//       currency_code: data.currency_code || data.currencyCode,
//       delivered: data.delivered || 0,
//       pending: data.pending || 0,
//       containers: data.containers || [],
//       orders: data.orders || [],
//     };

//     const validationErrors = validateConsignmentFields(normalizedInput);
//     if (validationErrors.length > 0) {
//       return res
//         .status(400)
//         .json({ error: "Validation failed", details: validationErrors });
//     }

//     const containerErrors = validateContainers(normalizedInput.containers);
//     let orderErrors = [];
//     if (Array.isArray(normalizedInput.orders)) {
//       if (normalizedInput.orders.every((o) => typeof o === "number" && o > 0)) {
//         if (
//           normalizedInput.orders.length > 0 &&
//           normalizedInput.orders.some((id) => isNaN(id) || id <= 0)
//         ) {
//           orderErrors = [
//             {
//               index: -1,
//               errors: ["orders: All IDs must be positive integers"],
//             },
//           ];
//         }
//       } else {
//         orderErrors = validateOrders(normalizedInput.orders);
//       }
//     } else {
//       orderErrors = [{ index: -1, errors: ["orders: Must be an array"] }];
//     }

//     if (containerErrors.length > 0 || orderErrors.length > 0) {
//       return res.status(400).json({
//         error: "Array validation failed",
//         details: [...containerErrors, ...orderErrors],
//       });
//     }

//     const userId = data.user_id || req.user?.id;
//     if (!userId) {
//       return res
//         .status(400)
//         .json({ error: "user_id is required for audit trail" });
//     }

//     const newContainerIds = new Set(
//       normalizedInput.containers
//         .map((c) => (c.cid != null ? parseInt(c.cid, 10) : null))
//         .filter((cid) => cid != null && !isNaN(cid)),
//     );

//     let computedETA = normalizedInput.eta;

//     const normalizedData = {
//       consignment_number: normalizedInput.consignment_number,
//       status: normalizedInput.status,
//       remarks: normalizedInput.remarks,
//       shipper: normalizedInput.shipper,
//       shipper_address: normalizedInput.shipper_address,
//       consignee: normalizedInput.consignee,
//       consignee_address: normalizedInput.consignee_address,
//       origin: normalizedInput.origin,
//       destination: normalizedInput.destination,
//       eform: normalizedInput.eform,
//       eform_date: normalizeDate(normalizedInput.eform_date),
//       bank: normalizedInput.bank,
//       consignment_value: normalizedInput.consignment_value,
//       payment_type: normalizedInput.paymentType,
//       vessel: normalizedInput.vessel,
//       voyage: normalizedInput.voyage,
//       eta: normalizeDate(computedETA),
//       shipping_line_name: normalizedInput.shipping_line_name,
//       seal_no: normalizedInput.seal_no,
//       net_weight: normalizedInput.netWeight,
//       gross_weight: normalizedInput.gross_weight,
//       currency_code: normalizedInput.currency_code,
//       delivered: normalizedInput.delivered,
//       pending: normalizedInput.pending,
//       containers: JSON.stringify(normalizedInput.containers),
//       orders: JSON.stringify(normalizedInput.orders),
//     };

//     const updateFields = Object.keys(normalizedData).filter(
//       (key) => !["id", "created_at", "updated_at"].includes(key),
//     );
//     const setClauseParts = updateFields.map((key, index) => {
//       const dbKey = key.replace(/([A-Z])/g, "_$1").toLowerCase();
//       return `${dbKey} = $${index + 2}`;
//     });
//     const setClause = setClauseParts.join(", ");
//     const values = [id, ...updateFields.map((key) => normalizedData[key])];
//     const query = `UPDATE consignments SET ${setClause}, updated_at = CURRENT_TIMESTAMP WHERE id = $1 RETURNING *`;

//     let updatedConsignment;
//     let containerDiffSummary = { released: [], added: [] };

//     await withTransaction(async (client) => {
//       if (!computedETA || data.status !== undefined) {
//         const statusRow = await client.query(
//           `SELECT order_status
//             FROM statuses
//             WHERE consignment_status = $1
//               AND status = true
//             LIMIT 1`,
//           [normalizedInput.status || data.status],
//         );

//         const resolvedStatus =
//           statusRow.rows[0]?.order_status ??
//           normalizedInput.status ??
//           data.status;

//         const etaResult = await calculateETA(client, resolvedStatus);
//         computedETA = etaResult.eta;

//         const etaIndex = updateFields.findIndex((f) => f === "eta");
//         if (etaIndex !== -1) {
//           values[etaIndex + 1] = normalizeDate(computedETA);
//         }
//       }

//       const updateResult = await withUserAudit(req, query, values);
//       if (updateResult.rowCount === 0) {
//         throw new Error("Consignment not found");
//       }
//       updatedConsignment = updateResult.rows[0];

//       const activeContainersResult = await client.query(
//         `SELECT id, container_id
//          FROM container_consignment_history
//          WHERE consignment_id = $1 AND active = true`,
//         [id],
//       );

//       const activeContainerIds = new Set(
//         activeContainersResult.rows.map((r) => r.container_id),
//       );

//       const removedContainerIds = [...activeContainerIds].filter(
//         (cid) => !newContainerIds.has(cid),
//       );

//       const addedContainerIds = [...newContainerIds].filter(
//         (cid) => !activeContainerIds.has(cid),
//       );

//       if (removedContainerIds.length > 0) {
//         const deleteFromHistory = await client.query(
//           `DELETE FROM container_consignment_history
//            WHERE consignment_id = $1
//              AND container_id = ANY($2::int[])
//            RETURNING container_id`,
//           [id, removedContainerIds],
//         );
//         containerDiffSummary.released = deleteFromHistory.rows.map(
//           (r) => r.container_id,
//         );

//         await client.query(
//           `UPDATE container_assignment_history
//            SET consignment_id = NULL
//            WHERE cid = ANY($1::int[])
//              AND consignment_id = $2`,
//           [removedContainerIds, id],
//         );

//         await client.query(
//           `UPDATE container_status
//            SET
//              availability = 'Available',
//              created_time = NOW()
//            WHERE cid = ANY($1::int[])`,
//           [removedContainerIds],
//         );
//       }

//       if (addedContainerIds.length > 0) {
//         const ccInsertValues = [];
//         const ccInsertPlaceholders = [];

//         addedContainerIds.forEach((cid, index) => {
//           const offset = index * 6;
//           ccInsertPlaceholders.push(
//             `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6})`,
//           );
//           ccInsertValues.push(
//             parseInt(id),
//             cid,
//             new Date(),
//             new Date(),
//             parseInt(userId),
//             true,
//           );
//         });

//         const ccInsertQuery = `
//           INSERT INTO container_consignment_history
//             (consignment_id, container_id, assigned_at, created_at, created_by, active)
//           VALUES
//             ${ccInsertPlaceholders.join(",")}
//           RETURNING *`;

//         const ccInsertResult = await client.query(
//           ccInsertQuery,
//           ccInsertValues,
//         );
//         const ccNewRows = ccInsertResult.rows;

//         containerDiffSummary.added = ccNewRows.map((r) => r.container_id);

//         if (ccNewRows.length > 0) {
//           const assignTuples = [];
//           const assignValues = [];

//           ccNewRows.forEach((row, index) => {
//             const offset = index * 2;
//             assignTuples.push(`($${offset + 1}, $${offset + 2})`);
//             assignValues.push(
//               parseInt(row.container_id),
//               parseInt(row.consignment_id),
//             );
//           });

//           const orderIds = normalizedInput.orders.map((o) =>
//             typeof o === "number" ? o : parseInt(o),
//           );

//           await client.query(
//             `UPDATE container_assignment_history cah
//              SET consignment_id = v.consignment_id::integer
//              FROM (
//                VALUES ${assignTuples.join(",")}
//              ) AS v(cid, consignment_id)
//              WHERE cah.cid = v.cid::integer
//                AND cah.action_type = 'ASSIGN'
//                AND cah.order_id = ANY($${assignValues.length + 1}::int[])`,
//             [...assignValues, orderIds],
//           );

//           const currentStatusResult = await client.query(
//             `SELECT status
//              FROM container_assignment_history
//              WHERE consignment_id = $1
//              LIMIT 1`,
//             [parseInt(id)],
//           );

//           const currentStatus = currentStatusResult.rows?.[0]?.status || null;

//           const statusesResult = await client.query(`
//             SELECT
//               container_status,
//               sorting_number
//             FROM statuses
//             WHERE status = true
//               AND container_status IS NOT NULL
//             ORDER BY sorting_number ASC
//           `);

//           const statuses = statusesResult.rows;
//           let nextStatus = null;

//           if (currentStatus) {
//             const currentIndex = statuses.findIndex(
//               (s) => s.container_status === currentStatus,
//             );
//             if (currentIndex !== -1 && currentIndex < statuses.length - 1) {
//               nextStatus = statuses[currentIndex + 1].container_status;
//             }
//           }

//           if (nextStatus) {
//             const addedContainerIdInts = ccNewRows.map((r) =>
//               parseInt(r.container_id),
//             );
//             await client.query(
//               `UPDATE container_assignment_history
//                SET status = $1
//                WHERE consignment_id = $2
//                  AND cid = ANY($3::int[])`,
//               [nextStatus, parseInt(id), addedContainerIdInts],
//             );
//           }

//           await client.query(
//             `UPDATE container_status
//              SET
//                availability = 'Occupied',
//                created_time = NOW()
//              WHERE cid = ANY($1::int[])`,
//             [addedContainerIds],
//           );
//         }
//       }

//       if (data.status !== undefined) {
//         const logResult = await logToTracking(client, id, "status_updated", {
//           newStatus: normalizedInput.status,
//           eta: computedETA,
//         });
//         if (!logResult.success) {
//           console.warn(
//             `Tracking log failed for consignment ${id}:`,
//             logResult.error,
//           );
//         }
//       }

//       if (["In Transit", "Delivered"].includes(normalizedInput.status)) {
//         await sendNotification(updatedConsignment, "updated");
//       }
//     });

//     const responseData = {
//       ...updatedConsignment,
//       statusColor: getStatusColor(updatedConsignment.status),
//       shipperAddress: updatedConsignment.shipper_address || "",
//       consigneeAddress: updatedConsignment.consignee_address || "",
//       paymentType: updatedConsignment.payment_type || "",
//       shippingLine: updatedConsignment.shipping_line || null,
//       netWeight: updatedConsignment.net_weight || "0.00",
//       containerDiff: {
//         released: containerDiffSummary.released,
//         added: containerDiffSummary.added,
//         noChange: [...newContainerIds].filter(
//           (cid) => !containerDiffSummary.added.includes(cid),
//         ),
//       },
//     };

//     res.status(200).json({
//       message: "Consignment updated",
//       data: responseData,
//     });
//   } catch (err) {
//     console.error("Error updating consignment:", err);

//     if (err.message === "Consignment not found") {
//       return res.status(404).json({ error: "Consignment not found" });
//     }
//     if (err.code === "23505") {
//       return res
//         .status(409)
//         .json({ error: "Consignment number already exists" });
//     }
//     if (err.code === "23503") {
//       return res.status(400).json({
//         error: "Foreign key violation — container or user ID not found",
//         details: err.detail,
//       });
//     }

//     res.status(500).json({
//       error: "Failed to update consignment",
//       details: err.message,
//     });
//   }
// }

export async function deleteConsignment(req, res) {
  try {
    const { id } = req.params;
    await withTransaction(async (client) => {
      // Log deletion
      await logToTracking(client, id, "deleted", { reason: "user_request" });

      const result = await client.query(
        "DELETE FROM consignments WHERE id = $1 RETURNING id",
        [id],
      );
      if (result.rowCount === 0) {
        throw new Error("Consignment not found");
      }
    });
    res.json({ message: "Consignment deleted" });
  } catch (err) {
    console.error("Error deleting consignment:", err);
    if (err.message === "Consignment not found") {
      return res.status(404).json({ error: "Consignment not found" });
    }
    res.status(500).json({ error: "Failed to delete consignment" });
  }
}

export async function advanceStatus(req, res) {
  let syncOrderIds = [];
  let consignmentEta = null;

  try {
    const numericId = Number(req.params.id);

    if (!Number.isInteger(numericId) || numericId <= 0) {
      return res.status(400).json({
        error: "Invalid consignment ID.",
      });
    }

    const { rows } = await pool.query(
      `SELECT id, status FROM consignments WHERE id = $1`,
      [numericId],
    );

    if (!rows.length) {
      return res.status(404).json({
        error: "Consignment not found",
      });
    }

    const currentStatus = rows[0].status;

    const statusResult = await pool.query(
      `
      SELECT
        id,
        consignment_status,
        order_status,
        container_status,
        days_offset,
        sorting_number
      FROM statuses
      WHERE status = true
      AND consignment_status IS NOT NULL
      ORDER BY sorting_number ASC
      `,
    );

    const statuses = statusResult.rows;

    const currentIndex = statuses.findIndex(
      (s) => s.consignment_status === currentStatus,
    );

    const nextStatusRow = statuses[currentIndex + 1];

    const nextStatus = nextStatusRow.consignment_status;
    const syncedStatus = nextStatusRow.order_status;
    const containerStatus = nextStatusRow.container_status;

    if (nextStatus === "Delivered") {
      consignmentEta = new Date().toISOString().split("T")[0];
    } else {
      const etaResult = await calculateETA(pool, syncedStatus);

      consignmentEta = etaResult?.eta ?? null;
    }

    await withTransaction(async (client) => {
      await client.query(
        `
        UPDATE consignments
        SET status=$1,
            eta=$2,
            updated_at=NOW()
        WHERE id=$3
        `,
        [nextStatus, consignmentEta, numericId],
      );

      if (containerStatus) {
        await client.query(
          `
            UPDATE container_master
            SET status = $1
            WHERE cid IN (
              SELECT DISTINCT cid
              FROM container_assignment_history
              WHERE consignment_id = $2
            )
          `,
          [containerStatus, numericId],
        );

        await client.query(
          `
            UPDATE container_assignment_history
            SET status = $1
            WHERE consignment_id = $2
          `,
          [containerStatus, numericId],
        );
      }
      try {
        await client.query(
          `
          INSERT INTO order_tracking
          (
            order_id,
            sender_id,
            sender_ref,
            receiver_id,
            container_id,
            consignment_number,
            status,
            old_status,
            item_ref,
            eta,
            etd,
            created_by
          )
          SELECT
            cah.order_id,
            ot.sender_id,
            ot.sender_ref,
            cah.receiver_id,
            cah.cid,
            c.consignment_number,
            $1,
            $2,
            oi.item_ref,
            $3,
            $3,
            $4
          FROM container_assignment_history cah
          JOIN consignments c
            ON c.id = cah.consignment_id
          JOIN order_items oi
            ON oi.id = cah.detail_id
          LEFT JOIN LATERAL (
            SELECT sender_id, sender_ref
            FROM order_tracking
            WHERE item_ref = oi.item_ref
            ORDER BY created_time DESC
            LIMIT 1
          ) ot ON TRUE
          WHERE cah.consignment_id = $5
          `,
          [
            syncedStatus,
            currentStatus,
            consignmentEta,
            req.user?.username || req.user?.email || "system",
            numericId,
          ],
        );
      } catch (e) {
        throw e;
      }

      const orderIdsRes = await client.query(
        `
        SELECT jsonb_array_elements(orders::jsonb)->>'id'
        AS order_id

        FROM consignments

        WHERE id=$1
        `,
        [numericId],
      );

      syncOrderIds = orderIdsRes.rows
        .map((r) => parseInt(r.order_id, 10))
        .filter(Boolean);

      if (syncOrderIds.length) {
        await client.query(
          `
          UPDATE orders
            SET status=$1,
            updated_at=NOW()
          WHERE id = ANY($2::int[])
          `,
          [syncedStatus, syncOrderIds],
        );
        await client.query(
          `
          UPDATE receivers
            SET status=$1,
            eta=$2,
            updated_at=NOW()
          WHERE order_id = ANY($3::int[])
          `,
          [syncedStatus, consignmentEta, syncOrderIds],
        );
      }
    });

    return res.json({
      success: true,
      data: {
        previousStatus: currentStatus,
        newStatus: nextStatus,
        syncedStatus,
        eta: consignmentEta,
      },
    });
  } catch (err) {
    console.error("advanceStatus ERROR:", {
      message: err.message,
      code: err.code,
      detail: err.detail,
      hint: err.hint,
      position: err.position,
      stack: err.stack,
    });

    return res.status(500).json({
      error: "Failed",
      details: err.message,
    });
  }
}

export async function changeConsignmentStatus(req, res) {
  let syncOrderIds = [];
  let consignmentEta = null;

  try {
    const numericId = Number(req.params.id);

    if (!Number.isInteger(numericId) || numericId <= 0) {
      return res.status(400).json({
        error: "Invalid consignment ID.",
      });
    }

    const { newStatus, reason, days_offset } = req.body;

    if (!newStatus?.trim()) {
      return res.status(400).json({
        error: "newStatus is required",
      });
    }

    if (days_offset === undefined || days_offset === null) {
      return res.status(400).json({
        error: "days_offset is required",
      });
    }

    const trimmedStatus = newStatus.trim();
    const syncedStatus = trimmedStatus;

    const { rows } = await pool.query(
      `
      SELECT
        id,
        status,
        consignment_number,
        orders
      FROM consignments
      WHERE id = $1
      `,
      [numericId],
    );

    if (!rows.length) {
      return res.status(404).json({
        error: "Consignment not found",
      });
    }

    const consignment = rows[0];
    const currentStatus = consignment.status;

    if (currentStatus === trimmedStatus) {
      return res.status(400).json({
        error: "New status is the same as current status",
      });
    }

    let rawOrders = [];

    try {
      rawOrders = Array.isArray(consignment.orders)
        ? consignment.orders
        : JSON.parse(consignment.orders || "[]");
    } catch {
      rawOrders = [];
    }

    syncOrderIds = rawOrders
      .map((id) => Number(id))
      .filter((id) => Number.isInteger(id) && id > 0);

    await withTransaction(async (client) => {
      if (trimmedStatus === "Delivered") {
        consignmentEta = new Date().toISOString().split("T")[0];
      } else {
        const days = Number(days_offset);

        consignmentEta = new Date(Date.now() + days * 24 * 60 * 60 * 1000)
          .toISOString()
          .split("T")[0];
      }

      const containerStatusResult = await client.query(
        `
        SELECT container_status, order_status, sorting_number
        FROM statuses
        WHERE consignment_status = $1
        `,
        [trimmedStatus],
      );

      let containerStatus =
        containerStatusResult.rows[0]?.container_status ?? null;
      const orderStatus = containerStatusResult.rows[0]?.order_status ?? null;
      const currentSortingNumber =
        containerStatusResult.rows[0]?.sorting_number ?? null;

      if (!containerStatus && currentSortingNumber !== null) {
        const fallbackResult = await client.query(
          `
          SELECT container_status
          FROM statuses
          WHERE sorting_number < $1 AND container_status IS NOT NULL
          ORDER BY sorting_number DESC
          LIMIT 1
          `,
          [currentSortingNumber],
        );

        containerStatus = fallbackResult.rows[0]?.container_status ?? null;
      }

      await client.query(
        `
        UPDATE consignments
        SET
          status = $1,
          eta = $2,
          updated_at = NOW()
        WHERE id = $3
        `,
        [trimmedStatus, consignmentEta, numericId],
      );

      if (containerStatus) {
        await client.query(
          `
            UPDATE container_master
            SET status = $1
            WHERE cid IN (
              SELECT DISTINCT cid
              FROM container_assignment_history
              WHERE consignment_id = $2
            )
          `,
          [containerStatus, numericId],
        );

        await client.query(
          `
            UPDATE container_assignment_history
            SET status = $1
            WHERE consignment_id = $2
            `,
          [containerStatus, numericId],
        );
      }

      await client.query(
        `
        INSERT INTO order_tracking (
          order_id,
          sender_id,
          sender_ref,
          receiver_id,
          container_id,
          consignment_number,
          status,
          old_status,
          item_ref,
          eta,
          etd,
          created_by
        )
        SELECT
          cah.order_id,
          ot.sender_id,
          ot.sender_ref,
          cah.receiver_id,
          cah.cid,
          c.consignment_number,
          $1,
          $2,
          oi.item_ref,
          $3,
          $3,
          $4
        FROM container_assignment_history cah
        JOIN consignments c
          ON c.id = cah.consignment_id
        JOIN order_items oi
          ON oi.id = cah.detail_id
        LEFT JOIN LATERAL (
          SELECT
            sender_id,
            sender_ref
          FROM order_tracking ot
          WHERE ot.item_ref = oi.item_ref
          ORDER BY ot.created_time DESC
          LIMIT 1
        ) ot ON TRUE
        WHERE cah.consignment_id = $5
        `,
        [
          syncedStatus,
          currentStatus,
          consignmentEta,
          req.user?.username || req.user?.email || req.user?.id || "system",
          numericId,
        ],
      );

      await client.query(
        `
        INSERT INTO consignment_tracking (
          consignment_id,
          event_type,
          old_status,
          new_status,
          timestamp,
          details,
          created_at,
          source,
          action
        )
        VALUES (
          $1,
          $2,
          COALESCE($3::varchar,'Unknown'),
          $4,
          NOW(),
          $5::jsonb,
          NOW(),
          'api',
          'status_changed'
        )
        `,
        [
          numericId,
          "status_updated",
          currentStatus,
          trimmedStatus,
          JSON.stringify({
            reason: reason || "Manual status change",
            newEta: consignmentEta,
            daysOffset: Number(days_offset),
            user: req.user?.id || "system",
          }),
        ],
      );

      if (syncOrderIds.length) {
        await client.query(
          `
          UPDATE orders
          SET
            status = $1,
            updated_at = CURRENT_TIMESTAMP
          WHERE id = ANY($2::int[])
          `,
          [syncedStatus, syncOrderIds],
        );

        await client.query(
          `
          UPDATE receivers
          SET
            status = $1,
            eta = $2,
            updated_at = CURRENT_TIMESTAMP
          WHERE order_id = ANY($3::int[])
          `,
          [orderStatus, consignmentEta, syncOrderIds],
        );

        const receiversRes = await client.query(
          `
          SELECT id
          FROM receivers
          WHERE order_id = ANY($1::int[])
          `,
          [syncOrderIds],
        );

        for (const { id: receiverId } of receiversRes.rows) {
          const itemsRes = await client.query(
            `
            SELECT
              id,
              container_details
            FROM order_items
            WHERE receiver_id = $1
            `,
            [receiverId],
          );

          for (const itemRow of itemsRes.rows) {
            const containerDetails = safeParseJsonArray(
              itemRow.container_details,
            ).map((entry) =>
              entry?.container?.cid
                ? {
                    ...entry,
                    status: syncedStatus,
                  }
                : entry,
            );

            await client.query(
              `
              UPDATE order_items
              SET
                container_details = $1::jsonb,
                updated_at = NOW()
              WHERE id = $2
              `,
              [JSON.stringify(containerDetails), itemRow.id],
            );
          }

          await updateLinkedContainersStatus(
            client,
            receiverId,
            syncedStatus,
            "system",
          );
        }
      }
    });

    try {
      const updated = await pool.query(
        "SELECT * FROM consignments WHERE id = $1",
        [numericId],
      );

      await sendNotification(
        updated.rows[0],
        `status_changed_to_${trimmedStatus}`,
        {
          reason: reason || "Manual change",
          syncedOrders: syncOrderIds.length,
          syncedStatus,
          previousStatus: currentStatus,
        },
      );
    } catch {}

    return res.json({
      success: true,
      message: `Status changed to "${trimmedStatus}"`,
      data: {
        previousStatus: currentStatus,
        newStatus: trimmedStatus,
        syncedStatus,
        newEta: consignmentEta,
        daysOffset: Number(days_offset),
        affectedOrders: syncOrderIds.length,
      },
    });
  } catch (err) {
    console.error("Error changing status:", err);

    return res.status(500).json({
      error: "Failed to change status",
      details: err.message,
    });
  }
}
