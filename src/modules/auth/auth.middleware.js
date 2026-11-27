import jwt from "jsonwebtoken";
import { promisify } from "util";
import pool from "../../db/pool.js";
import { getEffectivePermissions } from "../../services/getEffectivePermissions.js";
import { signAccessToken, setAccessCookie } from "./auth.session.js";

const JWT_SECRET = process.env.JWT_SECRET;

if (!JWT_SECRET) {
  console.error("[FATAL] JWT_SECRET is missing.");
  process.exit(1);
}

const jwtVerify = promisify(jwt.verify);

const ALLOWED_ALGORITHMS = ["HS256"];

const CACHE_TTL_MS = 60_000;
const SLIDE_AFTER_SECONDS = 60;

const permissionsCache = new Map();
const userCache = new Map();

async function getEffectivePermissionsCached(userId, roleId) {
  const key = `${userId}:${roleId}`;
  const cached = permissionsCache.get(key);
  if (cached && cached.expiresAt > Date.now()) {
    return cached.permissions;
  }

  const permissions = await getEffectivePermissions(userId, roleId);
  permissionsCache.set(key, {
    permissions,
    expiresAt: Date.now() + CACHE_TTL_MS,
  });
  return permissions;
}

async function getUserRecordCached(userId) {
  const cached = userCache.get(userId);
  if (cached && cached.expiresAt > Date.now()) {
    return cached.record;
  }

  const { rows } = await pool.query(
    `SELECT u.id, u.email, u.active, u.role AS role_id, r.name AS role_name
     FROM users u
     JOIN roles r ON r.id = u.role
     WHERE u.id = $1`,
    [userId],
  );

  const record = rows[0] || null;
  userCache.set(userId, { record, expiresAt: Date.now() + CACHE_TTL_MS });
  return record;
}

export function invalidatePermissionsCache(userId, roleId) {
  if (userId && roleId) {
    permissionsCache.delete(`${userId}:${roleId}`);
    userCache.delete(userId);
    return;
  }
  permissionsCache.clear();
  userCache.clear();
}

function authError(code) {
  const err = new Error(code);
  err.code = code;
  return err;
}

async function authenticate(req, res) {
  const token = req.cookies?.accessToken;
  if (!token) throw authError("NO_TOKEN");

  const decoded = await jwtVerify(token, JWT_SECRET, {
    algorithms: ALLOWED_ALGORITHMS,
  });

  const record = await getUserRecordCached(decoded.id);
  if (!record || !record.active) throw authError("ACCOUNT_DISABLED");

  const permissions = await getEffectivePermissionsCached(
    record.id,
    record.role_id,
  );

  req.user = {
    id: record.id,
    email: record.email,
    roleId: record.role_id,
    roleName: record.role_name,
    permissions,
  };

  const ageSeconds = Math.floor(Date.now() / 1000) - (decoded.iat || 0);
  if (ageSeconds >= SLIDE_AFTER_SECONDS) {
    setAccessCookie(res, signAccessToken(req.user));
  }
}

export async function requireAuth(req, res, next) {
  try {
    await authenticate(req, res);
    next();
  } catch (err) {
    if (err.code === "NO_TOKEN") {
      return res.status(401).json({
        success: false,
        error: "UNAUTHENTICATED",
        message: "Access token missing",
      });
    }

    if (err.code === "ACCOUNT_DISABLED") {
      return res.status(401).json({
        success: false,
        error: "ACCOUNT_DISABLED",
      });
    }

    if (err.name === "TokenExpiredError") {
      return res.status(401).json({
        success: false,
        error: "TOKEN_EXPIRED",
      });
    }

    return res.status(401).json({
      success: false,
      error: "INVALID_TOKEN",
    });
  }
}

export async function optionalAuth(req, res, next) {
  try {
    req.user = null;
    await authenticate(req, res);
  } catch {
    req.user = null;
  }
  next();
}

export const requirePermission = (...permissions) => {
  return (req, res, next) => {
    if (!req.user) {
      return res.status(401).json({
        success: false,
        error: "UNAUTHENTICATED",
      });
    }

    const userPermissions = req.user.permissions || [];

    const allowed = permissions.some((permission) =>
      userPermissions.includes(permission),
    );

    if (!allowed) {
      return res.status(403).json({
        success: false,
        error: "FORBIDDEN",
        message: "Insufficient permissions",
        required: permissions,
      });
    }

    next();
  };
};

export const requireRole = (...roles) => {
  const allowedRoles = roles.map((role) => role.toLowerCase());

  return (req, res, next) => {
    if (!req.user) {
      return res.status(401).json({
        success: false,
        error: "UNAUTHENTICATED",
      });
    }

    if (!allowedRoles.includes(req.user.roleName?.toLowerCase())) {
      return res.status(403).json({
        success: false,
        error: "FORBIDDEN",
        message: "Insufficient permissions",
      });
    }

    next();
  };
};

export const requireSelfOrPermission = (paramName, ...permissions) => {
  return (req, res, next) => {
    if (!req.user) {
      return res.status(401).json({
        success: false,
        error: "UNAUTHENTICATED",
      });
    }

    const targetId = parseInt(req.params[paramName], 10);

    if (req.user.id === targetId) {
      return next();
    }

    const userPermissions = req.user.permissions || [];

    const allowed = permissions.some((permission) =>
      userPermissions.includes(permission),
    );

    if (!allowed) {
      return res.status(403).json({
        success: false,
        error: "FORBIDDEN",
        message: "Insufficient permissions",
        required: permissions,
      });
    }

    next();
  };
};
