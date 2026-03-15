// src/routes/auth.routes.js
import { Router } from "express";

import {
  register,
  login,
  me,
  logout,
  getUsers,
  createUser,
  updateUser,
  adminForceResetPassword,
} from "../auth/auth.controller.js";

import {
  getMyPermissions,
  getModules,
  getActions,
  getRolePermissions,
  updateRolePermissions,
  getUserPermissions,
  updateUserPermission,
  getAllPossiblePermissions,
  getRoles,
  getNotifications,
  getNotificationById,
  updateNotification,
  // createNotificationType,
  createNotification,
} from "../auth/rbac.controller.js";

import { requireAuth, requireRole } from "../auth/auth.middleware.js";

const router = Router();

// ────────────────────────────────────────────────────────────────
// Public / Auth routes (no auth required)
// ────────────────────────────────────────────────────────────────
router.post("/register", register);
router.post("/login", login);
router.post("/logout", logout);

// ────────────────────────────────────────────────────────────────
// Authenticated user routes
// ────────────────────────────────────────────────────────────────
router.get("/me", requireAuth, me);
router.get("/rbac/my-permissions", requireAuth, getMyPermissions);

// ────────────────────────────────────────────────────────────────
// Admin-only: User Management
// ────────────────────────────────────────────────────────────────
router.get("/users", requireAuth, requireRole("admin"), getUsers);
router.get("/roles", requireAuth, requireRole("admin"), getRoles);

router.post("/users", requireAuth, requireRole("admin"), createUser);
router.put("/users/:id", requireAuth, requireRole("admin"), updateUser);
router.delete("/users/:id", requireAuth, requireRole("admin"), async (req, res) => {
  // Add delete logic here if not already in auth.controller
  try {
    await pool.query("DELETE FROM users WHERE id = $1", [req.params.id]);
    res.json({ success: true, message: "User deleted" });
  } catch (err) {
    res.status(500).json({ success: false, error: "Failed to delete user" });
  }
});

// Admin password reset
router.post(
  "/admin/reset-user-password",
  requireAuth,
  // requireRole("admin"),
  adminForceResetPassword
);
router.post(
  "/admin/users/:id/reset-password",
  requireAuth,
  requireRole("admin"),
  adminForceResetPassword
);

// ────────────────────────────────────────────────────────────────
// Admin-only: RBAC - Global Modules & Actions
// ────────────────────────────────────────────────────────────────
router.get("/admin/rbac/modules", requireAuth, requireRole("admin"), getModules);
router.get("/admin/rbac/actions", requireAuth, requireRole("admin"), getActions);
// In your router file (e.g., adminRoutes.js)
router.get('/admin/notifications', getNotifications);
router.get('/admin/notifications/:id', getNotificationById);
router.patch('/admin/notifications/:id', updateNotification);
router.post('/admin/notifications', createNotification); // optional
// router.post('/admin/notifications', createNotification);
// ────────────────────────────────────────────────────────────────
// Admin-only: Role Permission Management
// ────────────────────────────────────────────────────────────────
router.get(
  "/admin/rbac/roles/:roleName/permissions",
  requireAuth,
  requireRole("admin"),
  getRolePermissions
);
router.post(
  "/admin/rbac/roles/:roleName/permissions",
  requireAuth,
  requireRole("admin"),
  updateRolePermissions
);
router.get("/admin/permissions/all", requireAuth, requireRole("admin"), getAllPossiblePermissions);
// ────────────────────────────────────────────────────────────────
// Admin-only: Per-User Permission Overrides
// ────────────────────────────────────────────────────────────────
router.get(
  "/admin/users/:userId/permissions",
  requireAuth,
  requireRole("admin"),
  getUserPermissions
);
router.post(
  "/admin/users/:userId/permissions",
  requireAuth,
  requireRole("admin"),
  updateUserPermission
);

export default router;