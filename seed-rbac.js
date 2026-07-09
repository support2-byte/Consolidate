import pkg from "pg";
import dotenv from "dotenv";
dotenv.config();

const { Pool } = pkg;
import pool from "./src/db/pool.js";
import logger from "./src/services/logger.js";

const ROLES = {
  ADMIN: "admin",
  MANAGER: "manager",
  STAFF: "staff",
  VIEWER: "viewer",
};

const ACTIONS = [
  { code: "view", name: "View / Read" },
  { code: "create", name: "Create / Add" },
  { code: "edit", name: "Edit / Update" },
  { code: "delete", name: "Delete / Remove" },
];

const ROLE_MODULES = {
  [ROLES.ADMIN]: [
    "dashboard",
    "customers",
    "vendors",
    "containers",
    "orders",
    "consignments",
    "tracking",
    "users",
    "settings",
    "payment-types",
    "categories",
    "vessels",
    "places",
    "banks",
    "third-parties",
    "eta-setup",
    "barcode-print",
    "notifications",
    "permissions",
  ],
  [ROLES.MANAGER]: [
    "dashboard",
    "customers",
    "vendors",
    "containers",
    "orders",
    "consignments",
    "tracking",
    "users",
  ],
  [ROLES.STAFF]: ["dashboard", "orders", "consignments", "tracking"],
  [ROLES.VIEWER]: [
    "dashboard",
    "tracking",
    "orders",
    "containers",
    "consignments",
  ],
};

async function seed() {
  try {
    logger.info("Starting RBAC seeding...");

    for (const action of ACTIONS) {
      await pool.query(
        `INSERT INTO permission_actions (code, name)
         VALUES ($1, $2)
         ON CONFLICT (code) DO NOTHING`,
        [action.code, action.name],
      );
    }

    logger.info("Permission actions seeded.");

    for (const roleKey in ROLES) {
      await pool.query(
        `INSERT INTO roles (name)
         VALUES ($1)
         ON CONFLICT (name) DO NOTHING`,
        [ROLES[roleKey]],
      );
    }

    logger.info("Roles seeded.");

    const allModules = new Set();
    Object.values(ROLE_MODULES).forEach((mods) =>
      mods.forEach((m) => allModules.add(m)),
    );

    for (const mod of allModules) {
      const displayName = mod
        .split("-")
        .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
        .join(" ");

      await pool.query(
        `INSERT INTO modules (code, name)
         VALUES ($1, $2)
         ON CONFLICT (code) DO NOTHING`,
        [mod, displayName],
      );
    }

    logger.info("Modules seeded.", {
      modules: Array.from(allModules),
    });

    for (const roleKey in ROLE_MODULES) {
      const roleName = ROLES[roleKey];

      const roleRes = await pool.query(`SELECT id FROM roles WHERE name = $1`, [
        roleName,
      ]);

      const roleId = roleRes.rows[0]?.id;

      if (!roleId) {
        logger.warn(`Role not found: ${roleName}`);
        continue;
      }

      for (const modCode of ROLE_MODULES[roleKey]) {
        const modRes = await pool.query(
          `SELECT id FROM modules WHERE code = $1`,
          [modCode],
        );

        const modId = modRes.rows[0]?.id;

        if (!modId) {
          logger.warn(`Module not found: ${modCode}`);
          continue;
        }

        let actionsToAssign = [];

        if (roleName === ROLES.ADMIN) {
          actionsToAssign = ["view", "create", "edit", "delete"];
        } else if (roleName === ROLES.MANAGER) {
          actionsToAssign = ["view", "create", "edit"];
        } else if (roleName === ROLES.STAFF || roleName === ROLES.VIEWER) {
          actionsToAssign = ["view"];
        }

        for (const actionCode of actionsToAssign) {
          const actionRes = await pool.query(
            `SELECT id FROM permission_actions WHERE code = $1`,
            [actionCode],
          );

          const actionId = actionRes.rows[0]?.id;

          if (!actionId) {
            logger.warn(`Action not found: ${actionCode}`);
            continue;
          }

          await pool.query(
            `INSERT INTO role_permissions (role_id, module_id, action_id)
             VALUES ($1, $2, $3)
             ON CONFLICT DO NOTHING`,
            [roleId, modId, actionId],
          );
        }
      }
    }

    logger.info("RBAC seeding completed successfully!");
    process.exit(0);
  } catch (err) {
    logger.error("RBAC seeding failed.", {
      message: err.message,
      stack: err.stack,
    });

    process.exit(1);
  }
}

seed();
