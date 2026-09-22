import jwt from "jsonwebtoken";
import { promisify } from "util";
import { getEffectivePermissions } from "../../services/getEffectivePermissions.js";

const JWT_SECRET = process.env.JWT_SECRET;

if (!JWT_SECRET) {
  console.error("[FATAL] JWT_SECRET is missing.");
  process.exit(1);
}

const jwtVerify = promisify(jwt.verify);

const ALLOWED_ALGORITHMS = ["HS256"];

const permissionsCache = new Map();
const PERMISSIONS_CACHE_TTL_MS = 60_000;

async function getEffectivePermissionsCached(userId, roleId) {
  const key = `${userId}:${roleId}`;
  const cached = permissionsCache.get(key);
  if (cached && cached.expiresAt > Date.now()) {
    return cached.permissions;
  }

  const permissions = await getEffectivePermissions(userId, roleId);
  permissionsCache.set(key, {
    permissions,
    expiresAt: Date.now() + PERMISSIONS_CACHE_TTL_MS,
  });
  return permissions;
}

export function invalidatePermissionsCache(userId, roleId) {
  if (userId && roleId) {
    permissionsCache.delete(`${userId}:${roleId}`);
    return;
  }
  permissionsCache.clear();
}

export async function requireAuth(req, res, next) {
  try {
    const token = req.cookies?.accessToken;

    if (!token) {
      return res.status(401).json({
        success: false,
        error: "UNAUTHENTICATED",
        message: "Access token missing",
      });
    }

    const decoded = await jwtVerify(token, JWT_SECRET, {
      algorithms: ALLOWED_ALGORITHMS,
    });

    const permissions = await getEffectivePermissionsCached(
      decoded.id,
      decoded.roleId,
    );

    req.user = {
      id: decoded.id,
      email: decoded.email,
      roleId: decoded.roleId,
      roleName: decoded.roleName,
      permissions,
    };

    next();
  } catch (err) {
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
    const token = req.cookies?.accessToken;

    if (!token) {
      req.user = null;
      return next();
    }

    const decoded = await jwtVerify(token, JWT_SECRET, {
      algorithms: ALLOWED_ALGORITHMS,
    });

    const permissions = await getEffectivePermissionsCached(
      decoded.id,
      decoded.roleId,
    );

    req.user = {
      id: decoded.id,
      email: decoded.email,
      roleId: decoded.roleId,
      roleName: decoded.roleName,
      permissions,
    };

    next();
  } catch {
    req.user = null;
    next();
  }
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
