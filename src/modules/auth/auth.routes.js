import { Router } from "express";

import {
  register,
  login,
  logout,
  refreshToken,
  me,
  getUsers,
  createUser,
  updateUser,
  deleteUser,
  adminForceResetPassword,
  logoutAll,
} from "../auth/auth.controller.js";

import {
  getMyPermissions,
  getModules,
  getActions,
  getRoles,
  getRolePermissions,
  updateRolePermissions,
  getUserPermissions,
  updateUserPermission,
  getAllPossiblePermissions,
} from "../auth/rbac.controller.js";

import { requireAuth, requirePermission } from "../auth/auth.middleware.js";

const router = Router();

/**
 * @swagger
 * /auth/register:
 *   post:
 *     summary: Register a new user
 *     tags: [Auth]
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/RegisterRequest'
 *     responses:
 *       201:
 *         description: User registered successfully
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/User'
 *       400:
 *         description: Invalid input
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/ErrorResponse'
 */
router.post("/register", register);

/**
 * @swagger
 * /auth/login:
 *   post:
 *     summary: Log in and receive auth cookies
 *     tags: [Auth]
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/LoginRequest'
 *     responses:
 *       200:
 *         description: Login successful, sets accessToken cookie
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/User'
 *       401:
 *         description: Invalid credentials
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/ErrorResponse'
 */
router.post("/login", login);

/**
 * @swagger
 * /auth/refresh:
 *   post:
 *     summary: Refresh the access token using the refresh token cookie
 *     tags: [Auth]
 *     responses:
 *       200:
 *         description: New access token issued
 *       401:
 *         description: Invalid or expired refresh token
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/ErrorResponse'
 */
router.post("/refresh", refreshToken);

/**
 * @swagger
 * /auth/logout:
 *   post:
 *     summary: Log out the current session/device
 *     tags: [Auth]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: Logged out successfully
 *       401:
 *         description: Not authenticated
 */
router.post("/logout", requireAuth, logout);

/**
 * @swagger
 * /auth/me:
 *   get:
 *     summary: Get the currently authenticated user
 *     tags: [Auth]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: Current user
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/User'
 *       401:
 *         description: Not authenticated
 */
router.get("/me", requireAuth, me);

/**
 * @swagger
 * /auth/rbac/my-permissions:
 *   get:
 *     summary: Get the current user's permissions
 *     tags: [RBAC]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: List of permission keys for the current user
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items: { type: string }
 *       401:
 *         description: Not authenticated
 */
router.get("/rbac/my-permissions", requireAuth, getMyPermissions);

/**
 * @swagger
 * /auth/users:
 *   get:
 *     summary: List all users
 *     tags: [Users]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: List of users
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/User'
 *       403:
 *         description: Missing users.view permission
 */
router.get("/users", requireAuth, requirePermission("users.view"), getUsers);

/**
 * @swagger
 * /auth/users:
 *   post:
 *     summary: Create a new user
 *     tags: [Users]
 *     security:
 *       - cookieAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/CreateUserRequest'
 *     responses:
 *       201:
 *         description: User created
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/User'
 *       403:
 *         description: Missing users.create permission
 */
router.post(
  "/users",
  requireAuth,
  requirePermission("users.create"),
  createUser,
);

/**
 * @swagger
 * /auth/users/{id}:
 *   put:
 *     summary: Update a user
 *     tags: [Users]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/UpdateUserRequest'
 *     responses:
 *       200:
 *         description: User updated
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/User'
 *       403:
 *         description: Missing users.edit permission
 *       404:
 *         description: User not found
 */
router.put(
  "/users/:id",
  requireAuth,
  requirePermission("users.edit"),
  updateUser,
);

/**
 * @swagger
 * /auth/users/{id}:
 *   delete:
 *     summary: Delete a user
 *     tags: [Users]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: User deleted
 *       403:
 *         description: Missing users.delete permission
 *       404:
 *         description: User not found
 */
router.delete(
  "/users/:id",
  requireAuth,
  requirePermission("users.delete"),
  deleteUser,
);

/**
 * @swagger
 * /auth/admin/users/{id}/reset-password:
 *   post:
 *     summary: Admin force-reset a user's password
 *     tags: [Users]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Password reset successfully
 *       403:
 *         description: Missing users.edit permission
 *       404:
 *         description: User not found
 */
router.post(
  "/admin/users/:id/reset-password",
  requireAuth,
  requirePermission("users.edit"),
  adminForceResetPassword,
);

/**
 * @swagger
 * /auth/roles:
 *   get:
 *     summary: List all roles
 *     tags: [RBAC]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: List of role names
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items: { type: string }
 */
router.get("/roles", requireAuth, getRoles);

/**
 * @swagger
 * /auth/rbac/modules:
 *   get:
 *     summary: List all RBAC modules
 *     tags: [RBAC]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: List of modules
 *       403:
 *         description: Missing rbac.view permission
 */
router.get(
  "/rbac/modules",
  requireAuth,
  requirePermission("permissions.view"),
  getModules,
);

/**
 * @swagger
 * /auth/rbac/actions:
 *   get:
 *     summary: List all RBAC actions
 *     tags: [RBAC]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: List of actions
 *       403:
 *         description: Missing rbac.view permission
 */
router.get(
  "/rbac/actions",
  requireAuth,
  requirePermission("permissions.view"),
  getActions,
);

/**
 * @swagger
 * /auth/rbac/permissions:
 *   get:
 *     summary: List all possible permissions
 *     tags: [RBAC]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: List of all permission keys
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items: { type: string }
 *       403:
 *         description: Missing rbac.view permission
 */
router.get(
  "/rbac/permissions",
  requireAuth,
  requirePermission("permissions.view"),
  getAllPossiblePermissions,
);

/**
 * @swagger
 * /auth/rbac/roles/{roleName}/permissions:
 *   get:
 *     summary: Get permissions assigned to a role
 *     tags: [RBAC]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: roleName
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Role permissions
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/RolePermissions'
 *       403:
 *         description: Missing roles.view permission
 *       404:
 *         description: Role not found
 */
router.get(
  "/rbac/roles/:roleName/permissions",
  requireAuth,
  requirePermission("permissions.view"),
  getRolePermissions,
);

/**
 * @swagger
 * /auth/rbac/roles/{roleName}/permissions:
 *   put:
 *     summary: Update permissions assigned to a role
 *     tags: [RBAC]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: roleName
 *         required: true
 *         schema: { type: string }
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               permissions:
 *                 type: array
 *                 items: { type: string }
 *     responses:
 *       200:
 *         description: Role permissions updated
 *       403:
 *         description: Missing roles.edit permission
 *       404:
 *         description: Role not found
 */
router.put(
  "/rbac/roles/:roleName/permissions",
  requireAuth,
  requirePermission("permissions.edit"),
  updateRolePermissions,
);

/**
 * @swagger
 * /auth/users/{userId}/permissions:
 *   get:
 *     summary: Get a specific user's permission overrides
 *     tags: [Users]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: userId
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: User permission overrides
 *       403:
 *         description: Missing users.view permission
 *       404:
 *         description: User not found
 */
router.get(
  "/users/:userId/permissions",
  requireAuth,
  requirePermission("users.view"),
  getUserPermissions,
);

/**
 * @swagger
 * /auth/users/{userId}/permissions:
 *   put:
 *     summary: Update a specific user's permission overrides
 *     tags: [Users]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: userId
 *         required: true
 *         schema: { type: string }
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               permissions:
 *                 type: array
 *                 items: { type: string }
 *     responses:
 *       200:
 *         description: User permissions updated
 *       403:
 *         description: Missing users.edit permission
 *       404:
 *         description: User not found
 */
router.put(
  "/users/:userId/permissions",
  requireAuth,
  requirePermission("users.edit"),
  updateUserPermission,
);

/**
 * @swagger
 * /auth/logout-all:
 *   post:
 *     summary: Log out all sessions/devices for the current user
 *     tags: [Auth]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: All sessions logged out
 *       401:
 *         description: Not authenticated
 */
router.post("/logout-all", requireAuth, logoutAll);

export default router;
