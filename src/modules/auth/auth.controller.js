import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";
import crypto from "crypto";
import pool from "../../db/pool.js";
import { getEffectivePermissions } from "../../services/getEffectivePermissions.js";
import logger from "../../services/logger.js";

const JWT_SECRET = process.env.JWT_SECRET;
const ACCESS_TOKEN_TTL = "15m";
const REFRESH_TOKEN_TTL_DAYS = 30;
const ACCESS_TOKEN_MAX_AGE_MS = 15 * 60 * 1000;

export async function register(req, res) {
  try {
    const { email, password, name } = req.body;

    if (!email || !password) {
      logger.warn("Register attempt with missing fields");
      return res.status(400).json({
        success: false,
        error: "VALIDATION_ERROR",
        message: "Email and password are required",
      });
    }

    const existing = await pool.query("SELECT id FROM users WHERE email = $1", [
      email,
    ]);

    if (existing.rows.length > 0) {
      logger.warn("Register attempt with already-taken email", { email });
      return res.status(409).json({
        success: false,
        error: "EMAIL_TAKEN",
        message: "An account with this email already exists",
      });
    }

    const passwordHash = await bcrypt.hash(password, 12);

    const result = await pool.query(
      `INSERT INTO users (email, password_hash, name)
       VALUES ($1, $2, $3)
       RETURNING id, email, name, role, active, created_at`,
      [email, passwordHash, name || null],
    );

    logger.info("User registered", { userId: result.rows[0].id, email });

    return res.status(201).json({
      success: true,
      data: result.rows[0],
    });
  } catch (err) {
    logger.error("Register failed", { message: err.message, stack: err.stack });
    return res.status(500).json({
      success: false,
      error: "INTERNAL_ERROR",
    });
  }
}

export async function login(req, res) {
  try {
    const { email, password } = req.body;

    if (!email || !password) {
      logger.warn("Login attempt with missing fields");
      return res.status(400).json({
        success: false,
        error: "VALIDATION_ERROR",
        message: "Email and password are required",
      });
    }

    const userResult = await pool.query(
      `SELECT u.id, u.email, u.password_hash, u.active, u.role AS role_id, r.name AS role_name
       FROM users u
       JOIN roles r ON r.id = u.role
       WHERE u.email = $1`,
      [email],
    );

    const user = userResult.rows[0];

    if (!user) {
      logger.warn("Login attempt for unknown email", { email });
      return res.status(401).json({
        success: false,
        error: "INVALID_CREDENIALS".replace("ENIAL", "ENTIAL"),
      });
    }

    if (!user.active) {
      logger.warn("Login attempt for disabled account", { userId: user.id });
      return res.status(403).json({
        success: false,
        error: "ACCOUNT_DISABLED",
      });
    }

    const passwordMatches = await bcrypt.compare(password, user.password_hash);

    if (!passwordMatches) {
      logger.warn("Login attempt with invalid password", { userId: user.id });
      return res.status(401).json({
        success: false,
        error: "INVALID_CREDENTIALS",
      });
    }

    const permissions = await getEffectivePermissions(user.id, user.role_id);

    const accessToken = jwt.sign(
      {
        id: user.id,
        email: user.email,
        roleId: user.role_id,
        roleName: user.role_name,
        permissions,
      },
      JWT_SECRET,
      { algorithm: "HS256", expiresIn: ACCESS_TOKEN_TTL },
    );

    const refreshToken = crypto.randomBytes(48).toString("hex");
    const expiresAt = new Date(
      Date.now() + REFRESH_TOKEN_TTL_DAYS * 24 * 60 * 60 * 1000,
    );

    await pool.query(
      `INSERT INTO refresh_token (user_id, token, expires_at)
       VALUES ($1, $2, $3)`,
      [user.id, refreshToken, expiresAt],
    );

    res.cookie("accessToken", accessToken, {
      httpOnly: true,
      secure: true,
      sameSite: "strict",
      maxAge: ACCESS_TOKEN_MAX_AGE_MS,
    });

    res.cookie("refreshToken", refreshToken, {
      httpOnly: true,
      secure: true,
      sameSite: "strict",
      maxAge: REFRESH_TOKEN_TTL_DAYS * 24 * 60 * 60 * 1000,
    });

    logger.info("User logged in", {
      userId: user.id,
      roleName: user.role_name,
    });

    return res.status(200).json({
      success: true,
      data: {
        id: user.id,
        email: user.email,
        roleName: user.role_name,
        permissions,
      },
    });
  } catch (err) {
    logger.error("Login failed", { message: err.message, stack: err.stack });
    return res.status(500).json({
      success: false,
      error: "INTERNAL_ERROR",
    });
  }
}

export async function refreshToken(req, res) {
  try {
    const incomingToken = req.cookies?.refreshToken;

    if (!incomingToken) {
      logger.warn("Refresh attempt with no token present");
      return res.status(401).json({
        success: false,
        error: "REFRESH_TOKEN_MISSING",
      });
    }

    const tokenResult = await pool.query(
      `SELECT rt.id, rt.user_id, rt.expires_at, u.email, u.active, u.role AS role_id, r.name AS role_name
       FROM refresh_token rt
       JOIN users u ON u.id = rt.user_id
       JOIN roles r ON r.id = u.role
       WHERE rt.token = $1`,
      [incomingToken],
    );

    const tokenRow = tokenResult.rows[0];

    if (!tokenRow) {
      logger.warn("Refresh attempt with invalid/unknown token");
      return res.status(401).json({
        success: false,
        error: "INVALID_REFRESH_TOKEN",
      });
    }

    if (new Date(tokenRow.expires_at) < new Date()) {
      await pool.query("DELETE FROM refresh_token WHERE id = $1", [
        tokenRow.id,
      ]);
      logger.info("Refresh token expired, removed", {
        userId: tokenRow.user_id,
      });
      return res.status(401).json({
        success: false,
        error: "REFRESH_TOKEN_EXPIRED",
      });
    }

    if (!tokenRow.active) {
      await pool.query("DELETE FROM refresh_token WHERE id = $1", [
        tokenRow.id,
      ]);
      logger.warn("Refresh attempt for disabled account", {
        userId: tokenRow.user_id,
      });
      return res.status(403).json({
        success: false,
        error: "ACCOUNT_DISABLED",
      });
    }

    const newRefreshToken = crypto.randomBytes(48).toString("hex");
    const newExpiresAt = new Date(
      Date.now() + REFRESH_TOKEN_TTL_DAYS * 24 * 60 * 60 * 1000,
    );

    await pool.query(
      `UPDATE refresh_token SET token = $1, expires_at = $2 WHERE id = $3`,
      [newRefreshToken, newExpiresAt, tokenRow.id],
    );

    const permissions = await getEffectivePermissions(
      tokenRow.user_id,
      tokenRow.role_id,
    );

    const accessToken = jwt.sign(
      {
        id: tokenRow.user_id,
        email: tokenRow.email,
        roleId: tokenRow.role_id,
        roleName: tokenRow.role_name,
        permissions,
      },
      JWT_SECRET,
      { algorithm: "HS256", expiresIn: ACCESS_TOKEN_TTL },
    );

    res.cookie("accessToken", accessToken, {
      httpOnly: true,
      secure: true,
      sameSite: "strict",
      maxAge: ACCESS_TOKEN_MAX_AGE_MS,
    });

    res.cookie("refreshToken", newRefreshToken, {
      httpOnly: true,
      secure: true,
      sameSite: "strict",
      maxAge: REFRESH_TOKEN_TTL_DAYS * 24 * 60 * 60 * 1000,
    });

    logger.debug("Access token refreshed", { userId: tokenRow.user_id });

    return res.status(200).json({
      success: true,
      data: {
        id: tokenRow.user_id,
        email: tokenRow.email,
        roleName: tokenRow.role_name,
        permissions,
      },
    });
  } catch (err) {
    logger.error("Token refresh failed", {
      message: err.message,
      stack: err.stack,
    });
    return res.status(500).json({
      success: false,
      error: "INTERNAL_ERROR",
    });
  }
}

export async function logout(req, res) {
  try {
    const incomingToken = req.cookies?.refreshToken;

    if (incomingToken) {
      await pool.query("DELETE FROM refresh_token WHERE token = $1", [
        incomingToken,
      ]);
    }

    res.clearCookie("accessToken", {
      httpOnly: true,
      secure: true,
      sameSite: "strict",
    });

    res.clearCookie("refreshToken", {
      httpOnly: true,
      secure: true,
      sameSite: "strict",
    });

    logger.info("User logged out", { userId: req.user?.id });

    return res.status(200).json({
      success: true,
      message: "Logged out",
    });
  } catch (err) {
    logger.error("Logout failed", { message: err.message, stack: err.stack });
    return res.status(500).json({
      success: false,
      error: "INTERNAL_ERROR",
    });
  }
}

export async function me(req, res) {
  try {
    const result = await pool.query(
      `SELECT u.id, u.email, u.name, u.active, u.created_at, u.role AS role_id, r.name AS role_name
       FROM users u
       JOIN roles r ON r.id = u.role
       WHERE u.id = $1`,
      [req.user.id],
    );

    const user = result.rows[0];

    if (!user) {
      logger.warn("Profile lookup for missing user", { userId: req.user.id });
      return res.status(404).json({
        success: false,
        error: "USER_NOT_FOUND",
      });
    }

    return res.status(200).json({
      success: true,
      data: {
        id: user.id,
        email: user.email,
        name: user.name,
        active: user.active,
        createdAt: user.created_at,
        roleId: user.role_id,
        roleName: user.role_name,
        permissions: req.user.permissions,
      },
    });
  } catch (err) {
    logger.error("Fetching current user failed", {
      userId: req.user?.id,
      message: err.message,
      stack: err.stack,
    });
    return res.status(500).json({
      success: false,
      error: "INTERNAL_ERROR",
    });
  }
}

export async function getUsers(req, res) {
  try {
    const page = Math.max(parseInt(req.query.page, 10) || 1, 1);
    const limit = Math.min(
      Math.max(parseInt(req.query.limit, 10) || 20, 1),
      100,
    );
    const offset = (page - 1) * limit;
    const search = req.query.search?.trim();
    const roleId = req.query.roleId ? parseInt(req.query.roleId, 10) : null;

    const conditions = [];
    const params = [];

    if (search) {
      params.push(`%${search}%`);
      conditions.push(
        `(u.email ILIKE $${params.length} OR u.name ILIKE $${params.length})`,
      );
    }

    if (roleId) {
      params.push(roleId);
      conditions.push(`u.role = $${params.length}`);
    }

    const whereClause = conditions.length
      ? `WHERE ${conditions.join(" AND ")}`
      : "";

    const countResult = await pool.query(
      `SELECT COUNT(*) FROM users u ${whereClause}`,
      params,
    );

    const total = parseInt(countResult.rows[0].count, 10);

    params.push(limit, offset);

    const usersResult = await pool.query(
      `SELECT u.id, u.email, u.name, u.active, u.created_at, u.role AS role_id, r.name AS role_name
       FROM users u
       JOIN roles r ON r.id = u.role
       ${whereClause}
       ORDER BY u.id DESC
       LIMIT $${params.length - 1} OFFSET $${params.length}`,
      params,
    );

    logger.debug("Fetched user list", { page, limit, total });

    return res.status(200).json({
      success: true,
      data: usersResult.rows,
      pagination: {
        page,
        limit,
        total,
        totalPages: Math.ceil(total / limit),
      },
    });
  } catch (err) {
    logger.error("Fetching users failed", {
      message: err.message,
      stack: err.stack,
    });
    return res.status(500).json({
      success: false,
      error: "INTERNAL_ERROR",
    });
  }
}

export async function createUser(req, res) {
  try {
    const { email, password, name, roleId, active } = req.body;

    if (!email || !password || !roleId) {
      logger.warn("Create user attempt with missing fields", {
        actorId: req.user?.id,
      });
      return res.status(400).json({
        success: false,
        error: "VALIDATION_ERROR",
        message: "Email, password and roleId are required",
      });
    }

    const roleResult = await pool.query("SELECT id FROM roles WHERE id = $1", [
      roleId,
    ]);

    if (roleResult.rows.length === 0) {
      logger.warn("Create user attempt with invalid role", {
        actorId: req.user?.id,
        roleId,
      });
      return res.status(400).json({
        success: false,
        error: "INVALID_ROLE",
      });
    }

    const existing = await pool.query("SELECT id FROM users WHERE email = $1", [
      email,
    ]);

    if (existing.rows.length > 0) {
      logger.warn("Create user attempt with already-taken email", {
        actorId: req.user?.id,
        email,
      });
      return res.status(409).json({
        success: false,
        error: "EMAIL_TAKEN",
      });
    }

    const passwordHash = await bcrypt.hash(password, 12);

    const result = await pool.query(
      `INSERT INTO users (email, password_hash, name, role, active)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING id, email, name, role AS role_id, active, created_at`,
      [email, passwordHash, name || null, roleId, active ?? true],
    );

    logger.info("User created by admin", {
      actorId: req.user?.id,
      newUserId: result.rows[0].id,
      roleId,
    });

    return res.status(201).json({
      success: true,
      data: result.rows[0],
    });
  } catch (err) {
    logger.error("Create user failed", {
      actorId: req.user?.id,
      message: err.message,
      stack: err.stack,
    });
    return res.status(500).json({
      success: false,
      error: "INTERNAL_ERROR",
    });
  }
}

export async function updateUser(req, res) {
  try {
    const { id } = req.params;
    const { name, email, roleId, active } = req.body;

    const updates = [];
    const params = [];

    if (email !== undefined) {
      params.push(email);
      updates.push(`email = $${params.length}`);
    }

    if (name !== undefined) {
      params.push(name);
      updates.push(`name = $${params.length}`);
    }

    if (roleId !== undefined) {
      const roleResult = await pool.query(
        "SELECT id FROM roles WHERE id = $1",
        [roleId],
      );

      if (roleResult.rows.length === 0) {
        logger.warn("Update user attempt with invalid role", {
          actorId: req.user?.id,
          targetUserId: id,
          roleId,
        });
        return res.status(400).json({
          success: false,
          error: "INVALID_ROLE",
        });
      }

      params.push(roleId);
      updates.push(`role = $${params.length}`);
    }

    if (active !== undefined) {
      params.push(active);
      updates.push(`active = $${params.length}`);
    }

    if (updates.length === 0) {
      logger.warn("Update user attempt with no fields", {
        actorId: req.user?.id,
        targetUserId: id,
      });
      return res.status(400).json({
        success: false,
        error: "VALIDATION_ERROR",
        message: "No fields provided to update",
      });
    }

    updates.push(`updated_at = now()`);
    params.push(id);

    const result = await pool.query(
      `UPDATE users SET ${updates.join(", ")}
       WHERE id = $${params.length}
       RETURNING id, email, name, role AS role_id, active, created_at, updated_at`,
      params,
    );

    if (result.rows.length === 0) {
      logger.warn("Update user attempt for missing user", {
        actorId: req.user?.id,
        targetUserId: id,
      });
      return res.status(404).json({
        success: false,
        error: "USER_NOT_FOUND",
      });
    }

    logger.info("User updated", {
      actorId: req.user?.id,
      targetUserId: id,
      fieldsUpdated: Object.keys(req.body || {}),
    });

    return res.status(200).json({
      success: true,
      data: result.rows[0],
    });
  } catch (err) {
    if (err.code === "23505") {
      logger.warn("Update user failed due to duplicate email", {
        actorId: req.user?.id,
        targetUserId: req.params?.id,
      });
      return res.status(409).json({
        success: false,
        error: "EMAIL_TAKEN",
      });
    }

    logger.error("Update user failed", {
      actorId: req.user?.id,
      targetUserId: req.params?.id,
      message: err.message,
      stack: err.stack,
    });
    return res.status(500).json({
      success: false,
      error: "INTERNAL_ERROR",
    });
  }
}

export async function deleteUser(req, res) {
  try {
    const { id } = req.params;

    if (req.user.id === parseInt(id, 10)) {
      logger.warn("User attempted to delete own account", {
        userId: req.user.id,
      });
      return res.status(400).json({
        success: false,
        error: "CANNOT_DELETE_SELF",
      });
    }

    await pool.query("DELETE FROM refresh_token WHERE user_id = $1", [id]);

    const result = await pool.query(
      "DELETE FROM users WHERE id = $1 RETURNING id",
      [id],
    );

    if (result.rows.length === 0) {
      logger.warn("Delete user attempt for missing user", {
        actorId: req.user?.id,
        targetUserId: id,
      });
      return res.status(404).json({
        success: false,
        error: "USER_NOT_FOUND",
      });
    }

    logger.info("User deleted", { actorId: req.user?.id, targetUserId: id });

    return res.status(200).json({
      success: true,
      message: "User deleted",
    });
  } catch (err) {
    logger.error("Delete user failed", {
      actorId: req.user?.id,
      targetUserId: req.params?.id,
      message: err.message,
      stack: err.stack,
    });
    return res.status(500).json({
      success: false,
      error: "INTERNAL_ERROR",
    });
  }
}

export async function adminForceResetPassword(req, res) {
  try {
    const { id } = req.params;
    const { newPassword } = req.body;

    if (!newPassword || newPassword.length < 8) {
      logger.warn("Admin password reset attempt with invalid password length", {
        actorId: req.user?.id,
        targetUserId: id,
      });
      return res.status(400).json({
        success: false,
        error: "VALIDATION_ERROR",
        message: "newPassword must be at least 8 characters",
      });
    }

    const passwordHash = await bcrypt.hash(newPassword, 12);

    const result = await pool.query(
      `UPDATE users SET password_hash = $1, updated_at = now()
       WHERE id = $2
       RETURNING id, email`,
      [passwordHash, id],
    );

    if (result.rows.length === 0) {
      logger.warn("Admin password reset attempt for missing user", {
        actorId: req.user?.id,
        targetUserId: id,
      });
      return res.status(404).json({
        success: false,
        error: "USER_NOT_FOUND",
      });
    }

    await pool.query("DELETE FROM refresh_token WHERE user_id = $1", [id]);

    logger.info("Password reset by admin, sessions revoked", {
      actorId: req.user?.id,
      targetUserId: id,
    });

    return res.status(200).json({
      success: true,
      message: "Password reset",
    });
  } catch (err) {
    logger.error("Admin password reset failed", {
      actorId: req.user?.id,
      targetUserId: req.params?.id,
      message: err.message,
      stack: err.stack,
    });
    return res.status(500).json({
      success: false,
      error: "INTERNAL_ERROR",
    });
  }
}

export async function logoutAll(req, res) {
  try {
    await pool.query("DELETE FROM refresh_token WHERE user_id = $1", [
      req.user.id,
    ]);

    res.clearCookie("accessToken", {
      httpOnly: true,
      secure: true,
      sameSite: "strict",
    });

    res.clearCookie("refreshToken", {
      httpOnly: true,
      secure: true,
      sameSite: "strict",
    });

    return res.status(200).json({
      success: true,
      message: "Logged out from all devices",
    });
  } catch (err) {
    console.error("[LOGOUT_ALL]", err);
    return res.status(500).json({
      success: false,
      error: "INTERNAL_ERROR",
    });
  }
}
