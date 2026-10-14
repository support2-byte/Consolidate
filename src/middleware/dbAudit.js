// utils/dbAudit.js
import pool from "../db/pool.js"; // your pool import

/**
 * Execute a query while automatically injecting created_by / updated_by
 * @param {Object} req - Express request object (must have req.user)
 * @param {string} query - The SQL query string
 * @param {Array} params - Query parameters (without audit fields)
 * @param {Object} options - Optional config
 * @returns {Promise} Result from pool.query
 */

export async function withUserAudit(
  req,
  client,
  query,
  params = [],
  options = {},
) {
  const userEmail = req?.user?.email || "unknown-user";

  if (query.trim().toUpperCase().startsWith("INSERT")) {
    const insertMatch = query.match(
      /INSERT INTO\s+\w+\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)/i,
    );
    if (insertMatch) {
      const columns = insertMatch[1].split(",").map((c) => c.trim());
      const valuesPlaceholders = insertMatch[2].split(",").map((v) => v.trim());

      if (!columns.includes("created_by")) {
        columns.push("created_by", "updated_by", "created_at", "updated_at");
        valuesPlaceholders.push(
          `$${params.length + 1}`,
          `$${params.length + 2}`,
          "NOW()",
          "NOW()",
        );
        params.push(userEmail, userEmail);
      }

      query = query
        .replace(
          /VALUES\s*\([^)]+\)/i,
          `VALUES (${valuesPlaceholders.join(", ")})`,
        )
        .replace(/\([^)]+\)/, `(${columns.join(", ")})`);
    }
  } else if (query.trim().toUpperCase().startsWith("UPDATE")) {
    if (!query.toLowerCase().includes("updated_by")) {
      params.push(userEmail);
      query = query.replace(
        /\bWHERE\b/i,
        `, updated_by = $${params.length} WHERE`,
      );
    }
  }

  // No BEGIN/COMMIT/ROLLBACK/connect/release here anymore —
  // the caller owns the transaction lifecycle.
  return client.query(query, params);
}

// utils/withAudit.js
export function addAuditInfo(req, setClause = "") {
  const user = req.user?.id || req.user?.email || "unknown";
  return {
    set: setClause ? `${setClause}, ` : "",
    user,
    extra: setClause.includes("updated_by")
      ? ""
      : `updated_by = '${user}', updated_at = NOW()`,
  };
}
// Convenience wrapper for single queries without transaction
export async function auditQuery(req, query, params = []) {
  const userEmail = req?.user?.email || "unknown-user";

  let finalQuery = query;
  let finalParams = [...params];

  if (query.trim().toUpperCase().startsWith("INSERT")) {
    // similar logic as above...
    // (you can extract common logic to a separate function if needed)
  } else if (query.trim().toUpperCase().startsWith("UPDATE")) {
    if (!query.toLowerCase().includes("updated_by")) {
      finalQuery = query.replace(
        /WHERE/i,
        `SET updated_by = $${params.length + 1}, updated_at = NOW() WHERE`,
      );
      finalParams.push(userEmail);
    }
  }

  return pool.query(finalQuery, finalParams);
}
