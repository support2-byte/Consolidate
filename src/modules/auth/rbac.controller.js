// src/controllers/rbac.controller.js
import pool from '../../db/pool.js';

// Helper: Compute final effective permissions (role + overrides)
function computeEffectivePermissions(rolePerms, overrides) {
  const effective = { ...rolePerms };

  Object.entries(overrides).forEach(([module, actions]) => {
    if (!effective[module]) effective[module] = [];

    Object.entries(actions).forEach(([action, granted]) => {
      if (granted) {
        if (!effective[module].includes(action)) {
          effective[module].push(action);
        }
      } else {
        effective[module] = effective[module].filter(a => a !== action);
      }
    });
  });

  return effective;
}

// ────────────────────────────────────────────────────────────────
// GET /auth/rbac/my-permissions
// Current user's aggregated permissions
// ────────────────────────────────────────────────────────────────
export async function getMyPermissions(req, res) {
  if (!req.user) {
    return res.status(401).json({ success: false, error: 'Unauthorized' });
  }

  const userId = req.user.id;

  try {
    // Get the user's role ID (integer) and the corresponding role name (string)
    const userRes = await pool.query(`
      SELECT 
        u.role AS role_id,
        COALESCE(r.name, 'unknown') AS role_name
      FROM users u
      LEFT JOIN roles r ON r.id = u.role
      WHERE u.id = $1
    `, [userId]);

    if (userRes.rows.length === 0) {
      return res.json({ success: true, permissions: {}, role: 'none' });
    }

    const { role_id, role_name } = userRes.rows[0];

    if (!role_name || role_name === 'unknown') {
      console.warn(`[getMyPermissions] No valid role name for user ${userId} (role_id: ${role_id})`);
      return res.json({ success: true, permissions: {}, role: 'none' });
    }

    // Fetch permissions using the role name (string)
    const permRes = await pool.query(`
      SELECT 
        m.code AS module,
        COALESCE(
          ARRAY_AGG(DISTINCT pa.code ORDER BY pa.code),
          '{}'
        ) AS actions
      FROM role_permissions rp
      JOIN modules m ON m.id = rp.module_id
      JOIN permission_actions pa ON pa.id = rp.action_id
      JOIN roles r ON r.id = rp.role_id
      WHERE r.name = $1
      GROUP BY m.code
      ORDER BY m.code
    `, [role_name]);

    const permissions = {};
    permRes.rows.forEach(row => {
      permissions[row.module] = row.actions;
    });

    console.log(
      "[getMyPermissions] Success for user",
      userId,
      "role_id:", role_id,
      "role_name:", role_name,
      "permissions:", permissions
    );

    res.json({
      success: true,
      permissions,
      role: role_name   // return string like "staff" instead of 4
    });
  } catch (err) {
    console.error('[getMyPermissions] ERROR:', {
      message: err.message,
      stack: err.stack,
      code: err.code,
      detail: err.detail,
      userId
    });
    res.status(500).json({ success: false, error: 'Failed to load permissions' });
  }
}
// ────────────────────────────────────────────────────────────────
// GET /admin/rbac/modules
// List all modules (for UI)
// ────────────────────────────────────────────────────────────────
export async function getModules(req, res) {
  try {
    const { rows } = await pool.query(`
      SELECT id, code, name, description
      FROM modules
      ORDER BY name
    `);
    res.json({ success: true, modules: rows });
  } catch (err) {
    console.error('[getModules] Error:', err.message);
    res.status(500).json({ success: false, error: 'Failed to fetch modules' });
  }
}


export async function getRoles(req, res) {
 try {
    const result = await pool.query(`
      SELECT 
        id,
        name,
        COALESCE(description, name) AS label
      FROM roles
      ORDER BY id ASC
    `);

    res.json({
      success: true,
      roles: result.rows,   // [{ id: 1, name: "admin", label: "Administrator" }, ...]
    });
  } catch (err) {
    console.error("[GET /auth/roles] Error:", err.message);
    res.status(500).json({
      success: false,
      error: "Failed to fetch roles",
    });
  }
}
// ────────────────────────────────────────────────────────────────
// GET /admin/rbac/actions
// List all actions
// ────────────────────────────────────────────────────────────────
export async function getActions(req, res) {
  try {
    const { rows } = await pool.query(`
      SELECT id, code, name
      FROM permission_actions
      ORDER BY name
    `);
    res.json({ success: true, actions: rows });
  } catch (err) {
    console.error('[getActions] Error:', err.message);
    res.status(500).json({ success: false, error: 'Failed to fetch actions' });
  }
}

// ────────────────────────────────────────────────────────────────
// GET /admin/rbac/roles/:roleName/permissions
// Get permissions for a role
// ────────────────────────────────────────────────────────────────
export async function getRolePermissions(req, res) {
  const { roleName } = req.params;

  if (!roleName) {
    return res.status(400).json({ 
      success: false, 
      error: 'Role name is required in URL parameter' 
    });
  }

  try {
    // 1. Find role by name
    const roleRes = await pool.query(
      'SELECT id, name FROM roles WHERE name = $1',
      [roleName.trim()]
    );

    if (roleRes.rows.length === 0) {
      return res.status(404).json({ 
        success: false, 
        error: `Role '${roleName}' not found` 
      });
    }

    const { id: roleId, name: foundRoleName } = roleRes.rows[0];

    // 2. Fetch permissions
    const permRes = await pool.query(`
      SELECT 
        m.code AS module,
        COALESCE(
          ARRAY_AGG(DISTINCT pa.code ORDER BY pa.code),
          '{}'
        ) AS actions
      FROM role_permissions rp
      JOIN modules m ON m.id = rp.module_id
      JOIN permission_actions pa ON pa.id = rp.action_id
      WHERE rp.role_id = $1
      GROUP BY m.code
      ORDER BY m.code
    `, [roleId]);

    const permissions = {};
    permRes.rows.forEach(row => {
      permissions[row.module] = row.actions;
    });

    console.log(
      `[getRolePermissions] Success for role '${foundRoleName}' (id ${roleId}):`,
      `${permRes.rowCount} modules with permissions`
    );

    // Response – consistent and informative
    res.json({
      success: true,
      role: foundRoleName,
      roleId,
      permissions,               // { "users": ["view", "create"], "orders": ["view"] }
      permissionCount: permRes.rowCount,
    });

  } catch (err) {
    console.error('[getRolePermissions] Error:', {
      message: err.message,
      code: err.code,
      detail: err.detail,
      roleName,
      stack: err.stack?.substring(0, 300),
    });

    if (err.code === '23505' || err.code === '42703') {
      // Unique violation or column not found – likely schema issue
      return res.status(500).json({ 
        success: false, 
        error: 'Database schema error – contact admin' 
      });
    }

    res.status(500).json({ 
      success: false, 
      error: 'Failed to fetch role permissions' 
    });
  }
}

// ────────────────────────────────────────────────────────────────
// POST /admin/rbac/roles/:roleName/permissions
// Update role permissions (bulk)
// Body: { permissions: [{ module: string, actions: string[] }] }
// ────────────────────────────────────────────────────────────────
export async function updateRolePermissions(req, res) {
  const { roleName } = req.params;
  const { permissions } = req.body;

  if (!Array.isArray(permissions)) {
    return res.status(400).json({ success: false, error: 'Invalid format: permissions must be array' });
  }

  try {
    const roleRes = await pool.query('SELECT id FROM roles WHERE name = $1', [roleName]);
    if (roleRes.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Role not found' });
    }

    const roleId = roleRes.rows[0].id;

    await pool.query('BEGIN');

    // Clear existing
    await pool.query('DELETE FROM role_permissions WHERE role_id = $1', [roleId]);

    // Insert new
    for (const { module, actions } of permissions) {
      if (!Array.isArray(actions)) continue;

      const modRes = await pool.query('SELECT id FROM modules WHERE code = $1', [module]);
      if (modRes.rows.length === 0) continue;
      const moduleId = modRes.rows[0].id;

      for (const actionCode of actions) {
        const actRes = await pool.query('SELECT id FROM permission_actions WHERE code = $1', [actionCode]);
        if (actRes.rows.length === 0) continue;
        const actionId = actRes.rows[0].id;

        await pool.query(
          'INSERT INTO role_permissions (role_id, module_id, action_id) VALUES ($1, $2, $3)',
          [roleId, moduleId, actionId]
        );
      }
    }

    await pool.query('COMMIT');
    res.json({ success: true, message: `Permissions updated for ${roleName}` });
  } catch (err) {
    await pool.query('ROLLBACK');
    console.error('[updateRolePermissions] Error:', err.message);
    res.status(500).json({ success: false, error: 'Failed to update permissions' });
  }
}

// ────────────────────────────────────────────────────────────────
// GET /admin/users/:id/permissions
// User's role permissions + overrides
// ────────────────────────────────────────────────────────────────
export async function getUserPermissions(req, res) {
  const { userId } = req.params;

  try {
    // 1. Get user + role
    const userRes = await pool.query(`
      SELECT 
        u.id, u.email, u.name,
        u.role AS role_id,
        COALESCE(r.name, 'unknown') AS role_name
      FROM users u
      LEFT JOIN roles r ON r.id = u.role
      WHERE u.id = $1
    `, [userId]);

    if (userRes.rows.length === 0) {
      return res.status(404).json({ success: false, error: "User not found" });
    }

    const user = userRes.rows[0];

    // 2. Role-based permissions
    const rolePermRes = await pool.query(`
      SELECT 
        m.code AS module,
        COALESCE(ARRAY_AGG(DISTINCT pa.code ORDER BY pa.code), '{}') AS actions
      FROM role_permissions rp
      JOIN modules m ON m.id = rp.module_id
      JOIN permission_actions pa ON pa.id = rp.action_id
      WHERE rp.role_id = $1
      GROUP BY m.code
    `, [user.role_id]);

    const rolePermissions = {};
    rolePermRes.rows.forEach(r => {
      rolePermissions[r.module] = r.actions;
    });

    // 3. User overrides (this is the critical part)
    const overrideRes = await pool.query(`
      SELECT 
        m.code AS module,
        pa.code AS action,
        upo.granted
      FROM user_permission_overrides upo
      JOIN modules m ON m.id = upo.module_id
      JOIN permission_actions pa ON pa.id = upo.action_id
      WHERE upo.user_id = $1
      ORDER BY m.code, pa.code
    `, [userId]);

    const overrides = {};
    overrideRes.rows.forEach(row => {
      if (!overrides[row.module]) overrides[row.module] = {};
      overrides[row.module][row.action] = row.granted;
    });

    // Debug log – see what was actually fetched
    console.log(`[getUserPermissions] User ${userId}:`, {
      role: user.role_name,
      rolePermissionsCount: Object.keys(rolePermissions).length,
      overridesCount: Object.keys(overrides).length,
      overridesData: overrides
    });

    // 4. Effective (combine role + overrides)
    const effective = {};
    const allModules = new Set([...Object.keys(rolePermissions), ...Object.keys(overrides)]);
    for (const mod of allModules) {
      const roleActs = rolePermissions[mod] || [];
      const ovr = overrides[mod] || {};
      effective[mod] = roleActs.filter(act => ovr[act] !== false); // override false removes it
      if (ovr) {
        Object.keys(ovr).forEach(act => {
          if (ovr[act] === true && !effective[mod].includes(act)) {
            effective[mod].push(act);
          }
        });
      }
    }

    res.json({
      success: true,
      rolePermissions,
      overrides,
      effective,
      role: user.role_name,
      user: {
        id: user.id,
        email: user.email,
        name: user.name
      }
    });
  } catch (err) {
    console.error("[getUserPermissions] Error:", err.message, err.stack);
    res.status(500).json({ success: false, error: "Failed to fetch permissions" });
  }
}
// GET /auth/admin/permissions/all
// Returns full list of modules and possible actions
export async function getAllPossiblePermissions(req, res) {
  try {
    const modulesRes = await pool.query(`
      SELECT code AS module
      FROM modules
      ORDER BY code
    `);

    const actionsRes = await pool.query(`
      SELECT code AS action
      FROM permission_actions
      ORDER BY code
    `);

    const modules = modulesRes.rows.map(r => r.module);
    const actions = actionsRes.rows.map(r => r.action);

    res.json({
      success: true,
      modules,
      actions,
      // optional: full matrix if you prefer
      allPermissions: modules.map(mod => ({
        module: mod,
        possibleActions: actions
      }))
    });
  } catch (err) {
    console.error('[getAllPossiblePermissions] Error:', err.message);
    res.status(500).json({ success: false, error: 'Failed to load permission catalog' });
  }
}
// ────────────────────────────────────────────────────────────────
// POST /admin/users/:id/permissions
// Update single override
// Body: { module: string, action: string, granted: boolean }
// ────────────────────────────────────────────────────────────────
export async function updateUserPermission(req, res) {
  const { userId } = req.params;
  const { module, action, granted } = req.body;

  if (!module || !action || granted === undefined) {
    return res.status(400).json({ success: false, error: "module, action, and granted are required" });
  }

  try {
    // 1. Find module_id
    const moduleRes = await pool.query(
      "SELECT id FROM modules WHERE code = $1",
      [module.trim()]
    );
    if (moduleRes.rows.length === 0) {
      return res.status(400).json({ success: false, error: `Module "${module}" not found` });
    }
    const moduleId = moduleRes.rows[0].id;

    // 2. Find action_id
    const actionRes = await pool.query(
      "SELECT id FROM permission_actions WHERE code = $1",
      [action.trim()]
    );
    if (actionRes.rows.length === 0) {
      return res.status(400).json({ success: false, error: `Action "${action}" not found` });
    }
    const actionId = actionRes.rows[0].id;

    // 3. Upsert override (insert or update if exists)
    await pool.query(`
      INSERT INTO user_permission_overrides (user_id, module_id, action_id, granted)
      VALUES ($1, $2, $3, $4)
      ON CONFLICT (user_id, module_id, action_id)
      DO UPDATE SET granted = $4, updated_at = NOW()
    `, [userId, moduleId, actionId, granted]);

    res.json({ success: true });
  } catch (err) {
    console.error("[updateUserPermission] Error:", err.message, err.stack);
    res.status(500).json({ success: false, error: "Failed to update permission" });
  }
}