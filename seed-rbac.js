// rbac-seed.js
import pkg from "pg";
import dotenv from "dotenv";
dotenv.config();

const { Pool } = pkg;
import pool from "./src/db/pool.js"; // Correct path from root

const ROLES = {
  ADMIN: "admin",
  MANAGER: "manager",
  STAFF: "staff",
  VIEWER: "viewer",
};

const ACTIONS = [
  { code: 'view', name: 'View / Read' },
  { code: 'create', name: 'Create / Add' },
  { code: 'edit', name: 'Edit / Update' },
  { code: 'delete', name: 'Delete / Remove' },
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
    "users",              // manage users
    "settings",           // full settings
    "payment-types",
    "categories",
    "vessels",
    "places",
    "banks",
    "third-parties",
    "eta-setup",
    "barcode-print",
    "notifications"       // ← ADDED HERE for admin
  ],
  [ROLES.MANAGER]: [
    "dashboard",
    "customers",
    "vendors",
    "containers",
    "orders",
    "consignments",
    "tracking",
    "users",              // can see / maybe limited edit
    // "notifications"    // ← optional: add if managers should see it
  ],
  [ROLES.STAFF]: [
    "dashboard",
    "orders",
    "consignments",
    "tracking",
  ],
  [ROLES.VIEWER]: [
    "dashboard",
    "tracking",           // read-only
  ],
};

async function seed() {
  try {
    console.log("Starting RBAC seeding...");

    // 1. Insert all permission actions
    for (const action of ACTIONS) {
      await pool.query(
        `INSERT INTO permission_actions (code, name) 
         VALUES ($1, $2) 
         ON CONFLICT (code) DO NOTHING`,
        [action.code, action.name]
      );
    }
    console.log("Permission actions seeded.");

    // 2. Insert roles
    for (const roleKey in ROLES) {
      await pool.query(
        `INSERT INTO roles (name) 
         VALUES ($1) 
         ON CONFLICT (name) DO NOTHING`,
        [ROLES[roleKey]]
      );
    }
    console.log("Roles seeded.");

    // 3. Insert unique modules from ROLE_MODULES
    const allModules = new Set();
    Object.values(ROLE_MODULES).forEach(mods => mods.forEach(m => allModules.add(m)));

    for (const mod of allModules) {
      const displayName = mod
        .split('-')
        .map(word => word.charAt(0).toUpperCase() + word.slice(1))
        .join(' ');

      await pool.query(
        `INSERT INTO modules (code, name) 
         VALUES ($1, $2) 
         ON CONFLICT (code) DO NOTHING`,
        [mod, displayName]
      );
    }
    console.log("Modules seeded:", Array.from(allModules).join(", "));

    // 4. Assign permissions to roles
    for (const roleKey in ROLE_MODULES) {
      const roleName = ROLES[roleKey];

      const roleRes = await pool.query(
        `SELECT id FROM roles WHERE name = $1`,
        [roleName]
      );
      const roleId = roleRes.rows[0]?.id;
      if (!roleId) {
        console.warn(`Role not found: ${roleName}`);
        continue;
      }

      for (const modCode of ROLE_MODULES[roleKey]) {
        const modRes = await pool.query(
          `SELECT id FROM modules WHERE code = $1`,
          [modCode]
        );
        const modId = modRes.rows[0]?.id;
        if (!modId) {
          console.warn(`Module not found: ${modCode}`);
          continue;
        }

        // Define actions per role
        let actionsToAssign = [];
        if (roleName === ROLES.ADMIN) {
          actionsToAssign = ["view", "create", "edit", "delete"];
        } else if (roleName === ROLES.MANAGER) {
          actionsToAssign = ["view", "create", "edit"];
        } else if (roleName === ROLES.STAFF) {
          actionsToAssign = ["view"];
        } else if (roleName === ROLES.VIEWER) {
          actionsToAssign = ["view"];
        }

        for (const actionCode of actionsToAssign) {
          const actionRes = await pool.query(
            `SELECT id FROM permission_actions WHERE code = $1`,
            [actionCode]
          );
          const actionId = actionRes.rows[0]?.id;
          if (!actionId) {
            console.warn(`Action not found: ${actionCode}`);
            continue;
          }

          await pool.query(
            `INSERT INTO role_permissions (role_id, module_id, action_id)
             VALUES ($1, $2, $3)
             ON CONFLICT DO NOTHING`,
            [roleId, modId, actionId]
          );
        }
      }
    }

    console.log("RBAC seeding completed successfully!");
    process.exit(0);
  } catch (err) {
    console.error("Seeding failed:", err.message);
    console.error(err.stack);
    process.exit(1);
  }
}

seed();
