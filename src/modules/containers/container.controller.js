import pool from "../../db/pool.js";
import { withUserAudit } from "../../middleware/dbAudit.js";
import { calculateETA } from "../../services/calculateEta.js";

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

function isValidDate(dateString) {
  if (!dateString) return false;
  const normalized = dateString.toString().split("T")[0]; // Strip time if full ISO
  const date = new Date(normalized);
  return !isNaN(date.getTime()) && normalized.match(/^\d{4}-\d{2}-\d{2}$/);
}

function normalizeDate(dateString) {
  if (!dateString) return null;
  const normalized = dateString.toString().split("T")[0];
  return isValidDate(normalized) ? normalized : null;
}

function validateSocFields({
  location,
  manufacture_date,
  purchase_date,
  purchase_price,
  purchase_from,
  owned_by,
  available_at,
}) {
  const errors = [];
  if (!location) errors.push("location");
  if (!isValidDate(manufacture_date))
    errors.push(`manufacture_date (got: "${manufacture_date}")`);
  if (!isValidDate(purchase_date))
    errors.push(`purchase_date (got: "${purchase_date}")`);
  if (!purchase_price) errors.push("purchase_price");
  if (!purchase_from) errors.push("purchase_from");
  if (!owned_by) errors.push("owned_by");
  if (!available_at) errors.push("available_at"); // Treat as string from dropdown, not date
  return errors;
}

function validateCocFields({
  hire_start_date,
  hire_end_date,
  hired_by,
  return_date,
  free_days,
  place_of_loading,
  place_of_destination,
}) {
  const errors = [];
  if (!isValidDate(hire_start_date))
    errors.push(`hire_start_date (got: "${hire_start_date}")`);
  if (!isValidDate(hire_end_date))
    errors.push(`hire_end_date (got: "${hire_end_date}")`);
  if (!hired_by) errors.push("hired_by");
  if (free_days === undefined || isNaN(free_days))
    errors.push("free_days (must be number)");
  if (!place_of_loading) errors.push("place_of_loading");
  if (!place_of_destination) errors.push("place_of_destination");
  if (return_date && !isValidDate(return_date))
    errors.push(`return_date (got: "${return_date}")`);
  return errors;
}

// Helper for status color mapping (for dynamic options)
function getStatusColor(status) {
  const colors = {
    Available: "success",
    Returned: "success",
    "In Transit": "warning",
    Loaded: "warning",
    Occupied: "warning",
    Hired: "warning",
    Arrived: "error",
    "Under Repair": "error",
    "De-Linked": "info",
    Cleared: "info",
    "Assigned to Job": "warning",
  };
  return colors[status] || "default";
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
        WHERE cm.status = 'Available'
      ) sub
      WHERE derived_status IS NOT NULL
      ORDER BY value
    `;

    const result = await pool.query(query);
    let statuses = result.rows.map((row) => ({
      ...row,
      color: getStatusColor(row.value),
    }));

    //     // Hardcode full list if query returns < 5 (or always, for consistency)
    if (statuses.length < 5) {
      statuses = [
        {
          value: "Available",
          label: "Available",
          color: getStatusColor("Available"),
        },
        { value: "Hired", label: "Hired", color: getStatusColor("Hired") },
        {
          value: "Occupied",
          label: "Occupied",
          color: getStatusColor("Occupied"),
        },
        {
          value: "In Transit",
          label: "In Transit",
          color: getStatusColor("In Transit"),
        },
        { value: "Loaded", label: "Loaded", color: getStatusColor("Loaded") },
        {
          value: "Assigned to Job",
          label: "Assigned to Job",
          color: getStatusColor("Assigned to Job"),
        },
        {
          value: "Arrived",
          label: "Arrived",
          color: getStatusColor("Arrived"),
        },
        {
          value: "De-Linked",
          label: "De-Linked",
          color: getStatusColor("De-Linked"),
        },
        {
          value: "Under Repair",
          label: "Under Repair",
          color: getStatusColor("Under Repair"),
        },
        {
          value: "Returned",
          label: "Returned",
          color: getStatusColor("Returned"),
        },
        {
          value: "Cleared",
          label: "Cleared",
          color: getStatusColor("Cleared"),
        },

        // Add more if needed
      ];
    }

    // Remove duplicates if mixing dynamic + hardcoded
    statuses = statuses.filter(
      (s, index, self) => index === self.findIndex((t) => t.value === s.value),
    );

    res.json(statuses);
  } catch (err) {
    console.error("Error fetching statuses:", err);
    res.status(500).json({ error: "Failed to fetch statuses" });
  }
}

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
    res.status(500).json({ error: "Failed to fetch locations" });
  }
}

export async function getSizes(req, res) {
  try {
    const query =
      "SELECT DISTINCT container_size AS value, container_size AS label FROM container_master WHERE container_size IS NOT NULL ORDER BY value";
    const result = await pool.query(query);
    res.json(result.rows);
  } catch (err) {
    console.error("Error fetching sizes:", err);
    res.status(500).json({ error: "Failed to fetch sizes" });
  }
}

export async function getTypes(req, res) {
  try {
    // Hardcode the full list to match frontend options
    const types = [
      { value: "RF", label: "RF" },
      { value: "HC", label: "HC" },
      { value: "Ft", label: "FT" },
      { value: "Tank", label: "Tank" },
    ];
    res.json(types);
  } catch (err) {
    console.error("Error fetching types:", err);
    res.status(500).json({ error: "Failed to fetch types" });
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
    res.status(500).json({ error: "Failed to fetch ownership types" });
  }
}

export async function createContainer(req, res) {
  let client;
  try {
    client = await pool.connect();
    await client.query("BEGIN");

    const {
      container_number,
      container_size,
      container_type,
      owner_type,
      remarks,
      created_by,
      // SOC (owned)
      // location, availability, manufacture_date, purchase_date, purchase_price, purchase_from, owned_by, available_at, currency,
      location,
      availability,
      derived_status,
      manufacture_date,
      purchase_date,
      purchase_price,
      purchase_from,
      owned_by,
      available_at,
      currency,
      // COC (hired)
      hire_start_date,
      hire_end_date,
      hired_by,
      return_date,
      free_days,
      place_of_loading,
      place_of_destination,
    } = req.body;

    // Validation
    if (!container_number || !container_size || !container_type) {
      return res
        .status(400)
        .json({ error: "Container number, size, and type required" });
    }
    if (!["soc", "coc"].includes(owner_type)) {
      return res
        .status(400)
        .json({ error: 'owner_type must be "soc" or "coc"' });
    }

    let validationErrors = [];
    if (owner_type === "soc") {
      validationErrors = validateSocFields({
        location,
        manufacture_date,
        purchase_date,
        purchase_price,
        purchase_from,
        owned_by,
        available_at,
      });
    } else {
      validationErrors = validateCocFields({
        hire_start_date,
        hire_end_date,
        hired_by,
        return_date,
        free_days,
        place_of_loading,
        place_of_destination,
      });
    }

    if (validationErrors.length > 0) {
      console.warn("Validation failed for fields:", validationErrors);
      return res.status(400).json({
        error: `${owner_type.toUpperCase()} fields missing or invalid`,
        details: validationErrors.join(", "),
      });
    }

    if (currency && !/^[A-Z]{3}$/.test(currency)) {
      return res
        .status(400)
        .json({ error: "Currency must be 3-letter code (e.g., USD)" });
    }

    // Normalize dates (skip available_at since it's now string)
    const normManufactureDate = normalizeDate(manufacture_date);
    const normPurchaseDate = normalizeDate(purchase_date);
    const normHireStartDate = normalizeDate(hire_start_date);
    const normHireEndDate = normalizeDate(hire_end_date);
    const normReturnDate = normalizeDate(return_date);

    if (owner_type === "soc" && (!normManufactureDate || !normPurchaseDate)) {
      return res
        .status(400)
        .json({ error: "Normalized dates invalid for SOC. Use YYYY-MM-DD." });
    }
    if (owner_type === "coc" && (!normHireStartDate || !normHireEndDate)) {
      return res
        .status(400)
        .json({ error: "Normalized dates invalid for COC. Use YYYY-MM-DD." });
    }

    const checkQuery =
      "SELECT cid FROM container_master WHERE container_number = $1";
    const checkResult = await client.query(checkQuery, [container_number]);
    if (checkResult.rowCount > 0) {
      await client.query("ROLLBACK");
      return res.status(409).json({ error: "Container number already exists" });
    }

    const masterQuery = `
      INSERT INTO container_master (container_number, container_size, container_type, owner_type, remarks, status, created_by)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      RETURNING cid
    `;
    const masterValues = [
      container_number,
      container_size,
      container_type,
      owner_type,
      remarks || "",
      derived_status || availability || "Available",
      created_by,
    ];
    const masterResult = await client.query(masterQuery, masterValues);
    const cid = masterResult.rows[0].cid;

    await client.query(
      "INSERT INTO container_status (cid, location, availability, status_notes, created_by) VALUES ($1, $2, $3, $4, $5)",
      [
        cid,
        location,
        derived_status || availability || "Available",
        "Initial creation",
        created_by || "system",
      ],
    );

    if (owner_type === "soc") {
      console.log("Inserting SOC with available_at (string):", available_at);
      await client.query(
        "INSERT INTO container_purchase_details (cid, manufacture_date, purchase_date, purchase_price, purchase_from, owned_by, available_at, currency, created_by) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        [
          cid,
          normManufactureDate,
          normPurchaseDate,
          purchase_price,
          purchase_from,
          owned_by,
          available_at,
          currency || "USD",
          created_by,
        ],
      );
    } else {
      await client.query(
        "INSERT INTO container_hire_details (cid, hire_start_date, hire_end_date, hired_by, return_date, free_days, place_of_loading, place_of_destination, created_by) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        [
          cid,
          normHireStartDate,
          normHireEndDate,
          hired_by,
          normReturnDate,
          free_days,
          place_of_loading,
          place_of_destination,
          created_by,
        ],
      );
    }

    await client.query("COMMIT");

    console.log("Created new container:", { cid });
    res.status(201).json({ message: "Container created", cid });
  } catch (err) {
    if (client) {
      await client.query("ROLLBACK");
    }
    console.error("pool error:", err.message);
    if (err.code === "23505") {
      return res.status(409).json({ error: "Container number already exists" });
    }
    if (err.code === "23514") {
      return res.status(400).json({
        error:
          "Invalid value for constrained field (e.g., owner_type or availability)",
      });
    }
    if (err.code === "22007" || err.code === "22008") {
      return res
        .status(400)
        .json({ error: "Invalid date format. Use YYYY-MM-DD." });
    }
    res
      .status(500)
      .json({ error: err.message || "Failed to create container" });
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
    await client.query("BEGIN");

    const { cid } = req.params;
    const updates = req.body;

    const n = (v) => (v === "" || v === undefined ? null : v);
    const created_by = updates.created_by || req.user?.id || "system";

    const current = await client.query(
      `SELECT cid, owner_type FROM container_master WHERE cid = $1`,
      [cid],
    );

    if (current.rowCount === 0) {
      await client.query("ROLLBACK");
      return res.status(404).json({ error: "Container not found" });
    }

    if (
      updates.owner_type &&
      updates.owner_type !== current.rows[0].owner_type
    ) {
      await client.query("ROLLBACK");
      return res
        .status(400)
        .json({ error: "Cannot change owner_type manually" });
    }

    await client.query(
      `UPDATE container_master
       SET
         container_number    = $1,
         container_size      = $2,
         container_type      = $3,
         remarks             = $4,
         available_at        = $5,
         location            = $6,
         derived_status      = $7,
         updated_at          = NOW()
       WHERE cid = $8`,
      [
        n(updates.container_number),
        n(updates.container_size),
        n(updates.container_type),
        n(updates.remarks),
        n(updates.available_at),
        n(updates.location),
        n(updates.derived_status),
        cid,
      ],
    );

    await client.query(
      "UPDATE container_status SET location = $1 WHERE cid = $2",
      [n(updates.location), cid],
    );

    const purchaseExists = await client.query(
      `SELECT pid FROM container_purchase_details WHERE cid = $1`,
      [cid],
    );

    const purchaseParams = [
      n(updates.manufacture_date),
      n(updates.purchase_date),
      n(updates.purchase_price) ?? 0,
      n(updates.purchase_from),
      n(updates.owned_by),
      n(updates.available_at),
      n(updates.currency),
      created_by,
    ];

    if (purchaseExists.rowCount > 0) {
      await client.query(
        `UPDATE container_purchase_details
         SET
           manufacture_date = $1,
           purchase_date    = $2,
           purchase_price   = $3,
           purchase_from    = $4,
           owned_by         = $5,
           available_at     = $6,
           currency         = $7,
           created_by       = $8
         WHERE cid = $9`,
        [...purchaseParams, cid],
      );
    } else {
      await client.query(
        `INSERT INTO container_purchase_details
           (cid, manufacture_date, purchase_date, purchase_price,
            purchase_from, owned_by, available_at, currency, created_by)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
        [cid, ...purchaseParams],
      );
    }

    const hireExists = await client.query(
      `SELECT hid FROM container_hire_details WHERE cid = $1`,
      [cid],
    );

    const hireParams = [
      n(updates.hire_start_date),
      n(updates.hire_end_date),
      n(updates.hired_by),
      n(updates.return_date),
      n(updates.free_days),
      n(updates.place_of_loading),
      n(updates.place_of_destination),
      created_by,
    ];

    if (hireExists.rowCount > 0) {
      await client.query(
        `UPDATE container_hire_details
         SET
           hire_start_date    = $1,
           hire_end_date      = $2,
           hired_by           = $3,
           return_date        = $4,
           free_days          = $5,
           place_of_loading   = $6,
           place_of_destination = $7,
           created_by         = $8
         WHERE cid = $9`,
        [...hireParams, cid],
      );
    } else {
      await client.query(
        `INSERT INTO container_hire_details
           (cid, hire_start_date, hire_end_date, hired_by,
            return_date, free_days, place_of_loading, place_of_destination, created_by)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
        [cid, ...hireParams],
      );
    }

    await client.query("COMMIT");

    return res.status(200).json({
      success: true,
      message: "Container updated successfully",
    });
  } catch (err) {
    if (client) await client.query("ROLLBACK");
    console.error("Container update failed:", err);
    return res.status(500).json({
      error: "Failed to update container",
      details: err.message,
    });
  } finally {
    if (client) client.release();
  }
}

export const getAllContainers = async (req, res) => {
  try {
    const query = `
      SELECT
        cm.cid,
        cm.container_number,
        cm.container_size,
        cm.container_type,
        cm.owner_type,
        cm.created_time,
        COALESCE(cs.location, '') AS location,
        COALESCE(cs.availability, 'Available') AS current_status,
        CASE
          WHEN lc.active = false THEN COALESCE(cm.status, '')
          ELSE COALESCE(cas.status, cm.status, '')
        END AS assignment_status,
        COALESCE(cs.status_notes, '') AS status_notes,
        CASE
          WHEN lc.active = false OR lc.active IS NULL THEN ''
          ELSE COALESCE(lc.consignment_number, '')
        END AS consignment_number
      FROM container_master cm
      LEFT JOIN LATERAL (
        SELECT location, availability, status_notes
        FROM container_status
        WHERE cid = cm.cid
        ORDER BY sid DESC
        LIMIT 1
      ) cs ON true
      LEFT JOIN LATERAL (
        SELECT c.consignment_number, cch.active
        FROM container_consignment_history cch
        JOIN consignments c ON c.id = cch.consignment_id
        WHERE cch.container_id = cm.cid
        ORDER BY cch.id DESC
        LIMIT 1
      ) lc ON true
      LEFT JOIN LATERAL (
        SELECT cah.status
        FROM container_assignment_history cah
        WHERE cah.cid = cm.cid
          AND cah.action_type = 'ASSIGN'
        ORDER BY cah.id DESC
        LIMIT 1
      ) cas ON true
      ORDER BY cm.created_time DESC
    `;
    const result = await pool.query(query);

    const data = result.rows.map((row) => ({
      ...row,
      assignment_status:
        row.assignment_status === "Available" ? "" : row.assignment_status,
    }));

    return res.status(200).json({ success: true, data });
  } catch (err) {
    console.error("Error fetching containers:", err);
    return res.status(500).json({
      success: false,
      message: "Something went wrong!",
    });
  }
};
export async function getContainerById(req, res) {
  try {
    const { cid } = req.params;
    const { includeOrder = "false" } = req.query;
    let query = `
      SELECT 
        cm.*, 
        cs.location, 
        CASE 
          WHEN cs.availability IS NOT NULL THEN cs.availability
          WHEN chd.hire_end_date IS NULL AND chd.hire_start_date IS NOT NULL THEN 'Hired'
          WHEN chd.hire_end_date > CURRENT_DATE THEN 'Occupied'
          ELSE 'Available'
        END as derived_status,
        cpd.manufacture_date::text, 
        cpd.purchase_date::text, 
        cpd.purchase_price, 
        cpd.purchase_from, 
        cpd.owned_by, 
        cpd.available_at, 
        cpd.currency,
        chd.hire_start_date::text, 
        chd.hire_end_date::text, 
        chd.hired_by, 
        chd.return_date::text, 
        chd.free_days, 
        chd.place_of_loading, 
        chd.place_of_destination
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
      WHERE cm.cid = $1
    `;

    if (includeOrder === "true") {
      query += `,
        o.id as associated_order_id,
        o.booking_ref as associated_booking_ref,
        o.status as associated_order_status,
        o.place_of_loading as order_place_of_loading,
        o.final_destination as order_final_destination,
        o.created_at as order_created_at
      `;
      fromClause = fromClause.replace(
        "WHERE",
        `
        LEFT JOIN orders o ON o.associated_container = cm.container_number AND o.status != 'Cancelled'
        WHERE
      `,
      );
    }

    query += ` ${fromClause}`;

    const rowsResult = await pool.query(query, [cid]);
    const rows = rowsResult.rows;

    if (rows.length === 0) {
      return res.status(404).json({ error: "Container not found" });
    }

    console.log("Fetched container:", rows[0].container_number);
    res.json(rows[0]);
  } catch (err) {
    console.error("pool error:", err.message);
    res.status(500).json({ error: err.message || "Failed to fetch container" });
  }
}
// Updated Usage History endpoint - Combines container_status and container_assignment_history for comprehensive usage
// export async function getUsageHistory(req, res) {
//   try {
//     const { cid } = req.params;
//     if (!cid || isNaN(parseInt(cid))) {
//       return res.status(400).json({ error: 'Valid CID is required' });
//     }
//     const containerId = parseInt(cid);

//     // Union query to combine status changes and assignment events
//     const historyQuery = `
//       -- Status changes from container_status
//       SELECT
//         cs.created_time as event_time,
//         'STATUS_CHANGE' as event_type,
//         cs.availability as event_status,
//         NULL as assigned_qty,
//         NULL as action_type,
//         cs.location as location,
//         cs.status_notes as notes,
//         cs.created_by as changed_by,
//         NULL as previous_status,
//         NULL as order_id,
//         NULL as receiver_id,
//         NULL as detail_id,
//         cm.container_number,
//         cm.owner_type,
//         cpd.owned_by,
//         chd.hired_by,
//         o.id as job_id,
//         o.booking_ref as job_no,
//         o.place_of_loading as pol,
//         o.final_destination as pod,
//         o.created_at as start_date,
//         o.updated_at as end_date,
//         o.status as order_status
//       FROM container_status cs
//       JOIN container_master cm ON cs.cid = cm.cid
//       LEFT JOIN container_purchase_details cpd ON cm.cid = cpd.cid
//       LEFT JOIN container_hire_details chd ON cm.cid = chd.cid
//       LEFT JOIN orders o ON o.associated_container = cm.container_number
//         AND o.status != 'Cancelled'
//       WHERE cs.cid = $1

//       UNION ALL

//       -- Assignment events from container_assignment_history
//       SELECT
//         cah.created_at as event_time,
//         'ASSIGNMENT' as event_type,
//         cah.status as event_status,
//         cah.assigned_qty,
//         cah.action_type,
//         NULL as location,  -- Assignments may not have location; could enhance if needed
//         cah.notes,
//         cah.changed_by,
//         cah.previous_status,
//         cah.order_id,
//         cah.receiver_id,
//         cah.detail_id,
//         cm.container_number,
//         cm.owner_type,
//         cpd.owned_by,
//         chd.hired_by,
//         cah.order_id as job_id,  -- Reuse order_id as job_id
//         o.booking_ref as job_no,
//         o.place_of_loading as pol,
//         o.final_destination as pod,
//         o.created_at as start_date,
//         o.updated_at as end_date,
//         o.status as order_status
//       FROM container_assignment_history cah
//       JOIN container_master cm ON cah.cid = cm.cid
//       LEFT JOIN container_purchase_details cpd ON cm.cid = cpd.cid
//       LEFT JOIN container_hire_details chd ON cm.cid = chd.cid
//       LEFT JOIN orders o ON cah.order_id = o.id
//         AND o.status != 'Cancelled'
//       WHERE cah.cid = $1

//       ORDER BY event_time DESC
//     `;

//     const result = await pool.query(historyQuery, [containerId]);
//     const history = result.rows;

//     // Format for frontend (group by job if possible; enhance with event details)
//     const formattedHistory = history.map(row => {
//       const eventSummary = row.event_type === 'ASSIGNMENT'
//         ? `${row.action_type} ${row.assigned_qty || 0} items (Prev: ${row.previous_status || 'N/A'})`
//         : `Status: ${row.event_status} ${row.location ? `at ${row.location}` : ''}`;

//       return {
//         eventTime: row.event_time.toISOString().split('T')[0],  // YYYY-MM-DD
//         eventType: row.event_type,
//         eventSummary: eventSummary,
//         jobNo: row.job_no || `JOB-${row.event_time.toISOString().split('T')[0].replace(/-/g, '')}`,
//         pol: row.pol || (row.owner_type === 'soc' ? 'Self Depot' : 'Vendor Depot'),
//         pod: row.pod || 'Destination Depot',
//         startDate: row.start_date ? row.start_date.toISOString().split('T')[0] : row.event_time.toISOString().split('T')[0],
//         endDate: row.end_date ? row.end_date.toISOString().split('T')[0] : row.event_time.toISOString().split('T')[0],
//         statusProgression: [row.event_status],
//         linkedOrders: row.job_no ? `ORD-${row.job_id}` : 'N/A',
//         remarks: row.notes || eventSummary,
//         changedBy: row.changed_by,
//         orderId: row.order_id,
//         receiverId: row.receiver_id,
//         detailId: row.detail_id
//       };
//     });

//     // Optional: Group by job/order for timeline view (if multiple events per job)
//     const groupedHistory = {};
//     formattedHistory.forEach(entry => {
//       const key = entry.jobNo;
//       if (!groupedHistory[key]) {
//         groupedHistory[key] = [];
//       }
//       groupedHistory[key].push(entry);
//     });

//     console.log(`Fetched ${formattedHistory.length} combined history events for container ${containerId} (grouped into ${Object.keys(groupedHistory).length} jobs)`);
//     res.json({
//       rawEvents: formattedHistory,  // Detailed event list
//       groupedByJob: groupedHistory  // Aggregated by job for easier UI rendering
//     });
//   } catch (err) {
//     console.error("Error fetching usage history:", err);
//     res.status(500).json({ error: 'Failed to fetch usage history', details: err.message });
//   }
// }

export async function getUsageHistory(req, res) {
  try {
    const { cid } = req.params;
    if (!cid || isNaN(parseInt(cid))) {
      return res.status(400).json({ error: "Valid CID is required" });
    }
    const containerId = parseInt(cid);

    const historyQuery = `
      SELECT 
        cs.created_time as event_time,
        'STATUS_CHANGE' as event_type,
        cs.availability as event_status,
        NULL as assigned_qty,
        NULL as assigned_weight_kg,
        NULL as action_type,
        NULL as loaded_at,
        cs.location as location,
        cs.status_notes as notes,
        cs.created_by as changed_by,
        NULL as previous_status,
        NULL as order_id,
        NULL as receiver_id,
        NULL as detail_id,
        NULL as consignment_number,
        cm.container_number,
        cm.owner_type,
        cpd.owned_by,
        chd.hired_by,
        NULL as item_ref,
        NULL as job_id,
        NULL as job_no,
        NULL as form_no,
        NULL as pol,
        NULL as pod,
        NULL as start_date,
        NULL as end_date,
        NULL as order_status,
        NULL as consignment_status,
        NULL as shipper_name,
        NULL as consignee_name
      FROM container_status cs
      JOIN container_master cm ON cs.cid = cm.cid
      LEFT JOIN container_purchase_details cpd ON cm.cid = cpd.cid
      LEFT JOIN container_hire_details chd ON cm.cid = chd.cid
      WHERE cs.cid = $1

      UNION ALL

      SELECT 
        cah.created_at as event_time,
        'ASSIGNMENT' as event_type,
        cah.status as event_status,
        cah.assigned_qty,
        cah.assigned_weight_kg,
        cah.action_type,
        cah.loaded_at,
        NULL as location,
        cah.notes,
        cah.changed_by,
        cah.previous_status,
        cah.order_id,
        cah.receiver_id,
        cah.detail_id,
        con.consignment_number as consignment_number,
        cm.container_number,
        cm.owner_type,
        cpd.owned_by,
        chd.hired_by,
        oi.item_ref,
        cah.order_id as job_id,
        o.booking_ref as job_no,
        o.rgl_booking_number as form_no,
        o.place_of_loading as pol,
        o.final_destination as pod,
        o.created_at as start_date,
        o.updated_at as end_date,
        o.status as order_status,
        con.status as consignment_status,
        con.shipper as shipper_name,
        con.consignee as consignee_name
        FROM container_assignment_history cah
        JOIN container_master cm ON cah.cid = cm.cid
        LEFT JOIN container_purchase_details cpd ON cm.cid = cpd.cid
        LEFT JOIN container_hire_details chd ON cm.cid = chd.cid
        LEFT JOIN orders o 
          ON cah.order_id = o.id 
          AND o.status != 'Cancelled'
        LEFT JOIN consignments con
          ON con.orders @> to_jsonb(cah.order_id)
        LEFT JOIN container_consignment_history cch
          ON cch.consignment_id = con.id
          AND cch.container_id = cah.cid
        LEFT JOIN order_items oi ON oi.id = cah.detail_id
        WHERE cah.cid = $1
          AND con.id IS NOT NULL

        ORDER BY event_time DESC`;

    const statusHistoryQuery = `
      SELECT 
        sid,
        cid,
        location,
        availability,
        created_by,
        created_time,
        status_notes
      FROM container_status
      WHERE cid = $1
      ORDER BY created_time DESC, sid DESC
    `;

    const [historyResult, statusHistoryResult] = await Promise.all([
      pool.query(historyQuery, [containerId]),
      pool.query(statusHistoryQuery, [containerId]),
    ]);

    const history = historyResult.rows;
    const statusHistory = statusHistoryResult.rows;

    const formattedHistory = history.map((row) => {
      const eventSummary =
        row.event_type === "ASSIGNMENT"
          ? `${row.action_type} ${row.assigned_qty || 0} items (${row.assigned_weight_kg || 0} kg) (Prev: ${row.previous_status || "N/A"})`
          : `Status: ${row.event_status} ${row.location ? `at ${row.location}` : ""}`;

      return {
        eventTime: row.event_time.toISOString().split("T")[0],
        eventType: row.event_type,
        consignmentStatus: row.consignment_status || null,
        shipperName: row.shipper_name || null,
        consigneeName: row.consignee_name || null,
        eventSummary,
        assignedQty: Number(row.assigned_qty || 0),
        assignedWeightKg: Number(row.assigned_weight_kg || 0),
        loadedAt: row.loaded_at
          ? row.loaded_at.toISOString().split("T")[0]
          : null,
        orderId: row.order_id,
        bookingRef: row.job_no,
        formNo: row.form_no,
        consignmentNo: row.consignment_number || null,
        pol:
          row.pol || (row.owner_type === "soc" ? "Self Depot" : "Vendor Depot"),
        pod: row.pod || "Destination Depot",
        startDate: row.start_date
          ? row.start_date.toISOString().split("T")[0]
          : row.event_time.toISOString().split("T")[0],
        endDate: row.end_date
          ? row.end_date.toISOString().split("T")[0]
          : row.event_time.toISOString().split("T")[0],
        jobNo:
          row.job_no ||
          `JOB-${row.event_time.toISOString().split("T")[0].replace(/-/g, "")}`,
        statusProgression: [row.event_status],
        linkedOrders: row.job_no ? `ORD-${row.job_id}` : "N/A",
        remarks: row.notes || eventSummary,
        changedBy: row.changed_by,
        receiverId: row.receiver_id,
        detailId: row.detail_id,
        itemRef: row.item_ref,
      };
    });

    const groupedByConsignment = {};

    formattedHistory
      .filter((e) => e.eventType === "ASSIGNMENT" && e.assignedQty > 0)
      .forEach((event) => {
        const key =
          event.consignmentNo || `ORDER-${event.bookingRef || event.orderId}`;

        if (!groupedByConsignment[key]) {
          groupedByConsignment[key] = {
            consignmentNo: event.consignmentNo || null,
            bookingRef: event.bookingRef,
            formNo: event.formNo,
            pol: event.pol,
            pod: event.pod,
            loadedAt: event.loadedAt,
            orders: [],
          };
        }

        groupedByConsignment[key].orders.push(event);
      });

    const formattedStatusHistory = statusHistory.map((row) => ({
      sid: row.sid,
      cid: row.cid,
      location: row.location,
      status: row.availability,
      createdBy: row.created_by || "System",
      createdTime: row.created_time,
      notes: row.status_notes,
    }));

    console.log(
      `Fetched ${formattedHistory.length} combined events and ${formattedStatusHistory.length} status events for container ${containerId}`,
    );

    res.json({
      rawEvents: formattedHistory,
      groupedByConsignment,
      containerStatusHistory: {
        totalRecords: formattedStatusHistory.length,
        events: formattedStatusHistory,
        summary: {
          uniqueStatuses: [
            ...new Set(formattedStatusHistory.map((s) => s.status)),
          ],
          firstStatus:
            formattedStatusHistory[formattedStatusHistory.length - 1]?.status ||
            "N/A",
          latestStatus: formattedStatusHistory[0]?.status || "N/A",
          totalLocations: [
            ...new Set(formattedStatusHistory.map((s) => s.location)),
          ].length,
        },
      },
    });
  } catch (err) {
    console.error("Error fetching usage history:", err);
    res
      .status(500)
      .json({ error: "Failed to fetch usage history", details: err.message });
  }
}

export async function deleteContainer(req, res) {
  try {
    const { cid } = req.params;
    const result = await pool.query(
      "UPDATE container_master SET status = 0 WHERE cid = $1 RETURNING *",
      [cid],
    );

    if (result.rowCount === 0) {
      return res.status(404).json({ error: "Container not found" });
    }

    console.log("Deactivated container:", cid);
    res.json({ message: "Container deactivated" });
  } catch (err) {
    console.error("pool error:", err.message);
    res
      .status(500)
      .json({ error: err.message || "Failed to delete container" });
  }
}

export const getContainerAssignments = async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        cc.id,
        cc.container_id,
        cm.container_number,
        cc.consignment_id,
        cs.consignment_number,
        cc.assigned_at,
        cc.released_at,
        cc.created_at,
        COALESCE(NULLIF(cu.name, ''), cu.email) AS created_by,
        COALESCE(NULLIF(mu.name, ''), mu.email) AS released_by,
        cc.active
      FROM container_consignment_history cc
      INNER JOIN container_master cm
          ON cm.cid = cc.container_id
      INNER JOIN consignments cs
          ON cs.id = cc.consignment_id
      INNER JOIN users cu
          ON cu.id = cc.created_by
      LEFT JOIN users mu
          ON mu.id = cc.released_by
      ORDER BY cc.assigned_at DESC, cc.id DESC;
    `);

    return res.status(200).json({
      success: true,
      count: result.rows.length,
      data: result.rows,
    });
  } catch (error) {
    console.error("Error fetching container assignments:", error);

    return res.status(500).json({
      success: false,
      error: "Failed to fetch container assignments",
      details: error.message,
    });
  }
};

export const releaseContainer = async (req, res) => {
  try {
    const { id } = req.params;
    const { release_date, user_id } = req.body;

    if (!release_date) {
      return res.status(400).json({
        success: false,
        message: "Release date is required",
      });
    }

    let releasedAssignment = null;

    await withTransaction(async (client) => {
      const query = `
        UPDATE container_consignment_history
        SET
          released_at = $1,
          active = false,
          released_by = $2
        WHERE id = $3
          AND released_at IS NULL
        RETURNING *
      `;

      const result = await client.query(query, [release_date, user_id, id]);

      if (result.rowCount === 0) {
        throw new Error("ASSIGNMENT_NOT_FOUND");
      }

      releasedAssignment = result.rows[0];

      await client.query(
        `
        UPDATE container_master
          SET status = 'Available'
        WHERE cid = $1
        `,
        [releasedAssignment.container_id],
      );

      const consignment = await client.query(
        "SELECT destination FROM consignments WHERE id = $1",
        [releasedAssignment.consignment_id],
      );

      await client.query(
        `
        UPDATE container_status
          SET availability = 'Available', location = $1
        WHERE cid = $2
        `,
        [consignment.rows[0].destination, releasedAssignment.container_id],
      );
    });

    return res.status(200).json({
      success: true,
      message: "Container released successfully",
      data: releasedAssignment,
    });
  } catch (error) {
    if (error.message === "ASSIGNMENT_NOT_FOUND") {
      return res.status(404).json({
        success: false,
        message: "Assignment not found or already released",
      });
    }

    console.error("Error releasing container:", error);

    return res.status(500).json({
      success: false,
      message: "Failed to release container",
      error: error.message,
    });
  }
};

export async function getAllContainersForConsignment(req, res) {
  console.log("getAllContainers called with query:", req.query);
  try {
    const {
      container_number,
      container_size,
      container_type,
      owner_type,
      status = "",
      location,
      page = 1,
      limit = 100,
      includeOrder = "false",
    } = req.query;
    const offset = (parseInt(page) - 1) * parseInt(limit);

    let whereClause = `
      NOT EXISTS (
        SELECT 1
        FROM container_consignment_history cch
        WHERE cch.container_id = cm.cid
          AND cch.active = true
      )
    `;
    let baseValues = [];

    if (container_number) {
      whereClause += ` AND cm.container_number ILIKE $${baseValues.length + 1}`;
      baseValues.push(`%${container_number}%`);
    }
    if (container_size) {
      whereClause += ` AND cm.container_size = $${baseValues.length + 1}`;
      baseValues.push(container_size);
    }
    if (container_type) {
      whereClause += ` AND cm.container_type = $${baseValues.length + 1}`;
      baseValues.push(container_type);
    }
    if (owner_type) {
      whereClause += ` AND cm.owner_type = $${baseValues.length + 1}`;
      baseValues.push(owner_type);
    }
    if (location) {
      whereClause += ` AND cs.location = $${baseValues.length + 1}`;
      baseValues.push(location);
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

      LEFT JOIN container_purchase_details cpd
        ON cm.cid = cpd.cid

      LEFT JOIN container_hire_details chd
        ON cm.cid = chd.cid

      LEFT JOIN LATERAL (
        SELECT cch.active
        FROM container_consignment_history cch
        WHERE cch.container_id = cm.cid
        ORDER BY cch.id DESC
        LIMIT 1
      ) cch ON true
    `;

    let selectClause = `
      SELECT 
        cm.cid, cm.container_number, cm.container_size, cm.container_type, cm.owner_type, cm.remarks, cm.status,
        cs.location as location,
        CASE 
          WHEN cs.availability IS NOT NULL THEN cs.availability
          WHEN chd.hire_end_date IS NULL AND chd.hire_start_date IS NOT NULL THEN 'Hired'
          WHEN chd.hire_end_date > CURRENT_DATE THEN 'Occupied'
          ELSE 'Available'
        END as derived_status,
        cpd.manufacture_date, cpd.purchase_date, cpd.purchase_price, cpd.purchase_from, cpd.owned_by, cpd.available_at, cpd.currency,
        chd.hire_start_date, chd.hire_end_date, chd.hired_by, chd.return_date, chd.free_days, chd.place_of_loading, chd.place_of_destination,
        cm.created_time
    `;

    let orderJoin = "";
    if (includeOrder === "true") {
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
    let statusWhere = "";

    if (status && status !== "") {
      // Filter by specific status
      const statusParamIndex = baseValues.length + 1;
      statusWhere = `WHERE derived_status = $${statusParamIndex}`;
      fullParams.push(status);
    }

    // Add limit and offset
    const limitParamIndex = fullParams.length + 1;
    const offsetParamIndex = limitParamIndex + 1;
    fullParams.push(parseInt(limit), parseInt(offset)); // FIXED: Ensure offset is int

    let fullQuery;
    if (status && status !== "") {
      fullQuery = `
        WITH container_summary AS (${innerQuery})
        SELECT * FROM container_summary 
        ${statusWhere}
        ORDER BY created_time DESC
        LIMIT $${limitParamIndex} OFFSET $${offsetParamIndex}
      `;
    } else {
      // No status filter: show all
      fullQuery = `
        WITH container_summary AS (${innerQuery})
        SELECT * FROM container_summary 
        ORDER BY created_time DESC
        LIMIT $${limitParamIndex} OFFSET $${offsetParamIndex}
      `;
    }

    // FIXED: Count query - Reuse innerQuery, apply status filter outside CTE
    let countParams = [...baseValues];
    let countStatusWhere = "";
    if (status && status !== "") {
      const countStatusIndex = baseValues.length + 1;
      countStatusWhere = `WHERE derived_status = $${countStatusIndex}`;
      countParams.push(status);
    }
    const countQuery = `
      WITH container_summary AS (${innerQuery})
      SELECT COUNT(*) as total FROM container_summary
      ${countStatusWhere}
    `;

    console.log("Generated Query:", fullQuery); // Add logging for debugging
    console.log("Generated Count Query:", countQuery);
    console.log("Full Params:", fullParams);
    console.log("Count Params:", countParams);

    const rowsResult = await pool.query(fullQuery, fullParams);
    const countResult = await pool.query(countQuery, countParams);

    const rows = rowsResult.rows;

    console.log(
      "Fetched containers:",
      rows.length,
      "Total:",
      parseInt(countResult.rows[0].total),
      "Filters:",
      { ...req.query, status },
    );
    res.json({
      data: rows,
      total: parseInt(countResult.rows[0].total || 0),
      page: parseInt(page),
      limit: parseInt(limit),
    });
  } catch (err) {
    console.error("pool error:", err.message, "Query params:", req.query);
    res
      .status(500)
      .json({ error: err.message || "Failed to fetch containers" });
  }
}

export async function getUnassignedOrders(req, res) {
  try {
    const { cid } = req.params;

    if (!cid || isNaN(parseInt(cid))) {
      return res.status(400).json({ error: "Valid CID is required" });
    }

    const containerId = parseInt(cid);

    const query = `
      SELECT
        cah.created_at          AS event_time,
        'ASSIGNMENT'            AS event_type,
        cah.status              AS event_status,
        cah.assigned_qty,
        cah.assigned_weight_kg,
        cah.action_type,
        cah.loaded_at,
        cah.notes,
        cah.changed_by,
        cah.previous_status,
        cah.order_id,
        cah.receiver_id,
        cah.detail_id,
        cm.container_number,
        cm.owner_type,
        o.booking_ref           AS job_no,
        o.rgl_booking_number    AS form_no,
        o.place_of_loading      AS pol,
        o.final_destination     AS pod,
        o.created_at            AS start_date,
        o.updated_at            AS end_date,
        o.id                    AS job_id,
        oi.item_ref
      FROM container_assignment_history cah
      JOIN container_master cm ON cah.cid = cm.cid
      LEFT JOIN orders o
        ON cah.order_id = o.id
        AND o.status != 'Cancelled'
      LEFT JOIN order_items oi ON oi.receiver_id = cah.receiver_id
      WHERE cah.cid = $1
        AND o.id IS NOT NULL
        AND cah.assigned_qty > 0
        AND NOT EXISTS (
          SELECT 1 FROM consignments c
          WHERE c.orders @> to_jsonb(cah.order_id)
        )
      ORDER BY cah.created_at DESC
    `;

    const result = await pool.query(query, [containerId]);

    const orders = result.rows.map((row) => {
      const eventSummary = `${row.action_type || "ASSIGNMENT"} ${row.assigned_qty || 0} items (${row.assigned_weight_kg || 0} kg) — Prev: ${row.previous_status || "N/A"}`;

      return {
        eventTime: row.event_time
          ? row.event_time.toISOString().split("T")[0]
          : null,
        eventType: row.event_type,
        eventSummary,
        assignedQty: Number(row.assigned_qty || 0),
        assignedWeightKg: Number(row.assigned_weight_kg || 0),
        actionType: row.action_type || null,
        previousStatus: row.previous_status || null,
        loadedAt: row.loaded_at
          ? row.loaded_at.toISOString().split("T")[0]
          : null,
        orderId: row.order_id || null,
        bookingRef: row.job_no || null,
        formNo: row.form_no || null,
        pol:
          row.pol || (row.owner_type === "soc" ? "Self Depot" : "Vendor Depot"),
        pod: row.pod || "Destination Depot",
        startDate: row.start_date
          ? row.start_date.toISOString().split("T")[0]
          : row.event_time?.toISOString().split("T")[0] || null,
        endDate: row.end_date
          ? row.end_date.toISOString().split("T")[0]
          : row.event_time?.toISOString().split("T")[0] || null,
        linkedOrders: row.job_no ? `ORD-${row.job_id}` : "N/A",
        remarks: row.notes || eventSummary,
        changedBy: row.changed_by || "System",
        receiverId: row.receiver_id || null,
        detailId: row.detail_id || null,
        itemRef: row.item_ref || null,
      };
    });

    console.log(
      `[unassigned-orders] container ${containerId}: ${orders.length} events without a consignment`,
    );

    return res.json({ total: orders.length, orders });
  } catch (err) {
    console.error("Error fetching unassigned orders:", err);
    return res.status(500).json({
      error: "Failed to fetch unassigned orders",
      details: err.message,
    });
  }
}

export async function updateContainerStatus(req, res) {
  let client;

  try {
    client = await pool.connect();
    await client.query("BEGIN");

    const { cid } = req.params;

    const {
      current_status,
      derived_status,
      location,
      container_status,
      created_by = req.user?.email || "system",
    } = req.body;

    if (derived_status || location) {
      await client.query(
        `
        UPDATE container_status
        SET
          availability = COALESCE($1, availability),
          location = COALESCE($2, location),
          status_notes = $3,
          created_by = $4,
          created_time = NOW()
        WHERE cid = $5
        `,
        [
          derived_status,
          location,
          `Updated to ${derived_status} at ${location}`,
          created_by,
          cid,
        ],
      );
    }

    if (container_status && current_status) {
      const statusResult = await client.query(
        `
        SELECT order_status, days_offset
        FROM statuses
        WHERE container_status = $1
        `,
        [container_status],
      );

      const order_status = statusResult.rows[0]?.order_status ?? null;
      const days_offset = statusResult.rows[0]?.days_offset ?? null;

      let eta = null;
      let daysUntil = null;
      if (order_status) {
        const result = await calculateETA(client, order_status);
        eta = result.eta;
        daysUntil = result.daysUntil;
      }

      await client.query(
        `
          UPDATE container_master
          SET status = $1
          WHERE cid = $2
        `,
        [container_status, cid],
      );

      const assignmentResult = await client.query(
        `
          UPDATE container_assignment_history
          SET status = $1
          WHERE cid = $2
            AND action_type = 'ASSIGN'
            AND status = $3
          RETURNING receiver_id
        `,
        [container_status, cid, current_status],
      );

      const receiverIds = [
        ...new Set(
          assignmentResult.rows.map((row) => row.receiver_id).filter(Boolean),
        ),
      ];

      for (const receiverId of receiverIds) {
        const receiverResult = await client.query(
          `
          SELECT
            id,
            receiver_ref,
            status
          FROM receivers
          WHERE id = $1
          `,
          [receiverId],
        );

        const receiver = receiverResult.rows[0];

        if (!receiver) continue;

        const oldStatus = receiver.status || null;

        const itemsResult = await client.query(
          `
          SELECT
            oi.order_id,
            oi.sender_id,
            oi.receiver_id,
            oi.item_ref,
            s.sender_ref,
            s.consignment_number
          FROM order_items oi
          LEFT JOIN senders s
            ON s.id = oi.sender_id
          WHERE oi.receiver_id = $1
          `,
          [receiverId],
        );

        for (const item of itemsResult.rows) {
          await client.query(
            `
            INSERT INTO order_tracking (
              order_id,
              sender_id,
              sender_ref,
              receiver_id,
              receiver_ref,
              container_id,
              consignment_number,
              status,
              old_status,
              item_ref,
              created_by,
              created_time,
              eta,
              etd
            )
            VALUES (
              $1,
              $2,
              $3,
              $4,
              $5,
              $6,
              $7,
              $8,
              $9,
              $10,
              $11,
              NOW(),
              $12,
              $13
            )
            `,
            [
              item.order_id,
              item.sender_id,
              item.sender_ref,
              item.receiver_id,
              receiver.receiver_ref,
              cid,
              item.consignment_number,
              container_status,
              oldStatus,
              item.item_ref,
              created_by,
              eta,
              eta,
            ],
          );
        }

        await client.query(
          `
          UPDATE receivers
            SET status = $1,
                eta = $2
          WHERE id = $3
          `,
          [order_status, eta, receiverId],
        );
      }
    }

    await client.query("COMMIT");

    return res.status(200).json({
      success: true,
      message: "Container status updated successfully",
    });
  } catch (err) {
    if (client) {
      await client.query("ROLLBACK");
    }

    console.error("Container status update failed:", err);

    return res.status(500).json({
      error: "Failed to update container status",
      details: err.message,
    });
  } finally {
    if (client) {
      client.release();
    }
  }
}
