// utils/dbAudit.js
import pool from '../db/pool.js'; // your pool import

/**
 * Execute a query while automatically injecting created_by / updated_by
 * @param {Object} req - Express request object (must have req.user)
 * @param {string} query - The SQL query string
 * @param {Array} params - Query parameters (without audit fields)
 * @param {Object} options - Optional config
 * @returns {Promise} Result from pool.query
 */
export async function withUserAudit(req, query, params = [], options = {}) {    
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const userEmail = req?.user?.email || 'unknown-user';

    // For INSERT queries – add created_by & updated_by
    if (query.trim().toUpperCase().startsWith('INSERT')) {
      // Find the VALUES part
      const insertMatch = query.match(/INSERT INTO\s+\w+\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)/i);
      if (insertMatch) {
        const columns = insertMatch[1].split(',').map(c => c.trim());
        const valuesPlaceholders = insertMatch[2].split(',').map(v => v.trim());

        // Append audit columns if not already present
        if (!columns.includes('created_by')) {
          columns.push('created_by', 'updated_by', 'created_at', 'updated_at');
          valuesPlaceholders.push(`$${params.length + 1}`, `$${params.length + 2}`, 'NOW()', 'NOW()');
          params.push(userEmail, userEmail);
        }

        // Rebuild query
        query = query.replace(
          /VALUES\s*\([^)]+\)/i,
          `VALUES (${valuesPlaceholders.join(', ')})`
        ).replace(
          /\([^)]+\)/,
          `(${columns.join(', ')})`
        );
      }
    }

    // For UPDATE queries – always set updated_by & updated_at
    else if (query.trim().toUpperCase().startsWith('UPDATE')) {
      if (!query.toLowerCase().includes('updated_by')) {
        // Append before WHERE or at the end
        if (query.toLowerCase().includes('where')) {
          query = query.replace(
            /WHERE/i,
            `SET updated_by = $${params.length + 1}, updated_at = NOW() WHERE`
          );
        } else {
          query += ` SET updated_by = $${params.length + 1}, updated_at = NOW()`;
        }
        params.push(userEmail);
      }
    }

    const result = await client.query(query, params);

    await client.query('COMMIT');
    return result;
  } catch (err) {
    if (client) await client.query('ROLLBACK');
    throw err;
  } finally {
    if (client) client.release();
  }
}

// utils/withAudit.js
export function addAuditInfo(req, setClause = '') {
  const user = req.user?.id || req.user?.email || 'unknown';
  return {
    set: setClause ? `${setClause}, ` : '',
    user,
    extra: setClause.includes('updated_by') ? '' : `updated_by = '${user}', updated_at = NOW()`
  };
}
// Convenience wrapper for single queries without transaction
export async function auditQuery(req, query, params = []) {
  const userEmail = req?.user?.email || 'unknown-user';

  let finalQuery = query;
  let finalParams = [...params];

  if (query.trim().toUpperCase().startsWith('INSERT')) {
    // similar logic as above...
    // (you can extract common logic to a separate function if needed)
  } else if (query.trim().toUpperCase().startsWith('UPDATE')) {
    if (!query.toLowerCase().includes('updated_by')) {
      finalQuery = query.replace(
        /WHERE/i,
        `SET updated_by = $${params.length + 1}, updated_at = NOW() WHERE`
      );
      finalParams.push(userEmail);
    }
  }

  return pool.query(finalQuery, finalParams);
}