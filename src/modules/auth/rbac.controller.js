import pool from "../../db/pool.js";
import { getEffectivePermissions } from "../../services/getEffectivePermissions.js";
import logger from "../../services/logger.js";

export async function getMyPermissions(req, res) {
  try {
    const permissions = await getEffectivePermissions(
      req.user.id,
      req.user.roleId,
    );

    return res.status(200).json({
      success: true,
      data: {
        roleId: req.user.roleId,
        roleName: req.user.roleName,
        permissions,
      },
    });
  } catch (err) {
    logger.error("Failed to fetch current user permissions", {
      userId: req.user?.id,
      error: err,
    });
    return res.status(500).json({
      success: false,
      error: "INTERNAL_ERROR",
    });
  }
}

export async function getModules(req, res) {
  try {
    const result = await pool.query(
      `SELECT id, code, name, description, created_at
       FROM modules
       ORDER BY name ASC`,
    );

    return res.status(200).json({
      success: true,
      data: result.rows,
    });
  } catch (err) {
    logger.error("Failed to fetch modules", {
      error: err,
    });
    return res.status(500).json({
      success: false,
      error: "INTERNAL_ERROR",
    });
  }
}

export async function getActions(req, res) {
  try {
    const result = await pool.query(
      `SELECT id, code, name, created_at
       FROM permission_actions
       ORDER BY name ASC`,
    );

    return res.status(200).json({
      success: true,
      data: result.rows,
    });
  } catch (err) {
    logger.error("Failed to fetch permission actions", {
      error: err,
    });
    return res.status(500).json({
      success: false,
      error: "INTERNAL_ERROR",
    });
  }
}

export async function getRoles(req, res) {
  try {
    const result = await pool.query(
      `SELECT id, name, description, created_at, updated_at
       FROM roles
       ORDER BY name ASC`,
    );

    return res.status(200).json({
      success: true,
      data: result.rows,
    });
  } catch (err) {
    logger.error("Failed to fetch roles", {
      error: err,
    });
    return res.status(500).json({
      success: false,
      error: "INTERNAL_ERROR",
    });
  }
}

export async function getRolePermissions(req, res) {
  try {
    const { roleName } = req.params;

    const roleResult = await pool.query(
      "SELECT id, name FROM roles WHERE name = $1",
      [roleName],
    );

    const role = roleResult.rows[0];

    if (!role) {
      logger.warn("Role not found", {
        roleName,
      });
      return res.status(404).json({
        success: false,
        error: "ROLE_NOT_FOUND",
      });
    }

    const permsResult = await pool.query(
      `SELECT m.id AS module_id, m.code AS module_code, m.name AS module_name,
              pa.id AS action_id, pa.code AS action_code, pa.name AS action_name
       FROM role_permissions rp
       JOIN modules m ON m.id = rp.module_id
       JOIN permission_actions pa ON pa.id = rp.action_id
       WHERE rp.role_id = $1
       ORDER BY m.name ASC, pa.name ASC`,
      [role.id],
    );

    return res.status(200).json({
      success: true,
      data: {
        roleId: role.id,
        roleName: role.name,
        permissions: permsResult.rows,
      },
    });
  } catch (err) {
    logger.error("Failed to fetch role permissions", {
      roleName: req.params.roleName,
      error: err,
    });
    return res.status(500).json({
      success: false,
      error: "INTERNAL_ERROR",
    });
  }
}

export async function updateRolePermissions(req, res) {
  const client = await pool.connect();

  try {
    const { roleName } = req.params;
    const { permissions } = req.body;

    if (!Array.isArray(permissions)) {
      logger.warn("Invalid role permissions payload", {
        actorId: req.user?.id,
      });
      return res.status(400).json({
        success: false,
        error: "VALIDATION_ERROR",
        message: "permissions must be an array of { moduleId, actionId }",
      });
    }

    const roleResult = await pool.query(
      "SELECT id FROM roles WHERE name = $1",
      [roleName],
    );

    const role = roleResult.rows[0];

    if (!role) {
      return res.status(404).json({
        success: false,
        error: "ROLE_NOT_FOUND",
      });
    }

    for (const p of permissions) {
      if (!p.moduleId || !p.actionId) {
        logger.warn("Invalid permission entry", {
          actorId: req.user?.id,
          roleName,
        });
        return res.status(400).json({
          success: false,
          error: "VALIDATION_ERROR",
          message: "Each permission requires moduleId and actionId",
        });
      }
    }

    await client.query("BEGIN");

    await client.query("DELETE FROM role_permissions WHERE role_id = $1", [
      role.id,
    ]);

    for (const p of permissions) {
      await client.query(
        `INSERT INTO role_permissions (role_id, module_id, action_id)
         VALUES ($1, $2, $3)
         ON CONFLICT (role_id, module_id, action_id) DO NOTHING`,
        [role.id, p.moduleId, p.actionId],
      );
    }

    await client.query("COMMIT");

    logger.info("Role permissions updated", {
      actorId: req.user?.id,
      roleName,
      permissionCount: permissions.length,
    });

    return res.status(200).json({
      success: true,
      message: "Role permissions updated",
    });
  } catch (err) {
    await client.query("ROLLBACK");
    logger.error("Failed to update role permissions", {
      actorId: req.user?.id,
      roleName: req.params.roleName,
      error: err,
    });
    return res.status(500).json({
      success: false,
      error: "INTERNAL_ERROR",
    });
  } finally {
    client.release();
  }
}

export async function getUserPermissions(req, res) {
  try {
    const { userId } = req.params;

    const userResult = await pool.query(
      `SELECT u.id, u.role AS role_id, r.name AS role_name
       FROM users u
       JOIN roles r ON r.id = u.role
       WHERE u.id = $1`,
      [userId],
    );

    const user = userResult.rows[0];

    if (!user) {
      logger.warn("User not found", {
        userId,
      });
      return res.status(404).json({
        success: false,
        error: "USER_NOT_FOUND",
      });
    }

    const rolePermsResult = await pool.query(
      `SELECT m.id AS module_id, m.code AS module_code,
              pa.id AS action_id, pa.code AS action_code
       FROM role_permissions rp
       JOIN modules m ON m.id = rp.module_id
       JOIN permission_actions pa ON pa.id = rp.action_id
       WHERE rp.role_id = $1`,
      [user.role_id],
    );

    const overridesResult = await pool.query(
      `SELECT m.id AS module_id, m.code AS module_code,
              pa.id AS action_id, pa.code AS action_code, upo.granted
       FROM user_permission_overrides upo
       JOIN modules m ON m.id = upo.module_id
       JOIN permission_actions pa ON pa.id = upo.action_id
       WHERE upo.user_id = $1`,
      [userId],
    );

    return res.status(200).json({
      success: true,
      data: {
        userId: user.id,
        roleId: user.role_id,
        roleName: user.role_name,
        rolePermissions: rolePermsResult.rows,
        overrides: overridesResult.rows,
      },
    });
  } catch (err) {
    logger.error("Failed to fetch user permissions", {
      userId: req.params.userId,
      error: err,
    });
    return res.status(500).json({
      success: false,
      error: "INTERNAL_ERROR",
    });
  }
}

export async function updateUserPermission(req, res) {
  try {
    const { userId } = req.params;
    const { moduleId, actionId, granted } = req.body;

    if (!moduleId || !actionId) {
      logger.warn("Permission update validation failed", {
        actorId: req.user?.id,
      });
      return res.status(400).json({
        success: false,
        error: "VALIDATION_ERROR",
        message: "moduleId and actionId are required",
      });
    }

    const userResult = await pool.query("SELECT id FROM users WHERE id = $1", [
      userId,
    ]);

    if (userResult.rows.length === 0) {
      logger.warn("User not found", {
        userId,
      });
      return res.status(404).json({
        success: false,
        error: "USER_NOT_FOUND",
      });
    }

    if (granted === null) {
      await pool.query(
        `DELETE FROM user_permission_overrides
         WHERE user_id = $1 AND module_id = $2 AND action_id = $3`,
        [userId, moduleId, actionId],
      );

      logger.info("Permission override removed", {
        actorId: req.user?.id,
        userId,
        moduleId,
        actionId,
      });

      return res.status(200).json({
        success: true,
        message: "Override removed, reverted to role default",
      });
    }

    if (typeof granted !== "boolean") {
      logger.warn("Invalid permission override value", {
        actorId: req.user?.id,
      });
      return res.status(400).json({
        success: false,
        error: "VALIDATION_ERROR",
        message: "granted must be true, false, or null",
      });
    }

    await pool.query(
      `INSERT INTO user_permission_overrides (user_id, module_id, action_id, granted, updated_at)
       VALUES ($1, $2, $3, $4, now())
       ON CONFLICT (user_id, module_id, action_id)
       DO UPDATE SET granted = $4, updated_at = now()`,
      [userId, moduleId, actionId, granted],
    );

    logger.info("Permission override updated", {
      actorId: req.user?.id,
      userId,
      moduleId,
      actionId,
      granted,
    });

    return res.status(200).json({
      success: true,
      message: "Permission override updated",
    });
  } catch (err) {
    logger.error("Failed to update user permission", {
      actorId: req.user?.id,
      userId: req.params.userId,
      error: err,
    });
    return res.status(500).json({
      success: false,
      error: "INTERNAL_ERROR",
    });
  }
}

export async function getAllPossiblePermissions(req, res) {
  try {
    const modulesResult = await pool.query(
      `SELECT id, code, name, description FROM modules ORDER BY name ASC`,
    );

    const actionsResult = await pool.query(
      `SELECT id, code, name FROM permission_actions ORDER BY name ASC`,
    );

    return res.status(200).json({
      success: true,
      data: {
        modules: modulesResult.rows,
        actions: actionsResult.rows,
      },
    });
  } catch (err) {
    logger.error("Failed to fetch permission catalog", {
      error: err,
    });
    return res.status(500).json({
      success: false,
      error: "INTERNAL_ERROR",
    });
  }
}
