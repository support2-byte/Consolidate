import jwt from "jsonwebtoken";
import { promisify } from "util";

const JWT_SECRET = process.env.JWT_SECRET;

if (!JWT_SECRET) {
  console.error("[FATAL] JWT_SECRET is missing.");
  process.exit(1);
}

const jwtVerify = promisify(jwt.verify);

const ALLOWED_ALGORITHMS = ["HS256"];

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

    req.user = {
      id: decoded.id,
      email: decoded.email,
      roleId: decoded.roleId,
      roleName: decoded.roleName,
      permissions: decoded.permissions || [],
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

    req.user = {
      id: decoded.id,
      email: decoded.email,
      roleId: decoded.roleId,
      roleName: decoded.roleName,
      permissions: decoded.permissions || [],
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
