// middleware/auth.js
import jwt from 'jsonwebtoken';
import { promisify } from 'util';

const JWT_SECRET = process.env.JWT_SECRET;
if (!JWT_SECRET) {
  console.error('[FATAL] JWT_SECRET is not set in environment variables');
  process.exit(1);
}

const jwtVerify = promisify(jwt.verify);
const ALLOWED_ALGORITHMS = ['HS256', 'HS384', 'HS512'];

// ✅ Fixed: matches your actual roles table
// id=1 superadmin, id=2 admin, id=3 manager, id=4 user
const ROLE_MAP = { 1: 'superadmin', 2: 'admin', 3: 'manager', 4: 'user' };

export const requireRole = (...requiredRoles) => (req, res, next) => {
  if (!req.user) {
    return res.status(401).json({ success: false, error: 'Unauthorized' });
  }

  let userRoleStr;

  if (typeof req.user.role === 'string') {
    userRoleStr = req.user.role.toLowerCase();
  } else if (typeof req.user.role === 'number') {
    userRoleStr = (ROLE_MAP[req.user.role] || 'unknown').toLowerCase();
  } else {
    userRoleStr = 'unknown';
  }

  // ✅ Support multiple roles: requireRole('admin', 'superadmin')
  const allowed = requiredRoles.map(r => r.toLowerCase());
  if (!allowed.includes(userRoleStr)) {
    return res.status(403).json({
      success: false,
      error: 'Forbidden',
      message: `Insufficient permissions - requires one of: ${requiredRoles.join(', ')}`,
    });
  }

  next();
};

export function requireAuth(req, res, next) {
  const token = req.cookies?.token;

  if (!token) {
    return res.status(401).json({
      success: false,
      error: 'Unauthenticated',
      message: 'No authentication token provided',
    });
  }

  jwtVerify(token, JWT_SECRET, { algorithms: ALLOWED_ALGORITHMS })
    .then((decoded) => {
      if (!decoded || typeof decoded !== 'object') {
        return res.status(401).json({ success: false, error: 'Invalid token payload' });
      }

      if (!decoded.id && !decoded.sub && !decoded.userId) {
        return res.status(401).json({ success: false, error: 'Token missing user identifier' });
      }

      req.user = {
        id: decoded.id || decoded.sub || decoded.userId,
        email: decoded.email,
        role: decoded.role,
      };

      return next();
    })
    .catch((err) => {
      console.error('[Auth Middleware] Token verification failed', {
        name: err.name,
        message: err.message,
        tokenPrefix: token.substring(0, 10) + '...',
      });

      let status = 401;
      let message = 'Authentication failed';

      switch (err.name) {
        case 'TokenExpiredError':
          message = 'Session expired. Please log in again.';
          break;
        case 'NotBeforeError':
          message = 'Token not yet valid.';
          break;
        case 'JsonWebTokenError':
          message = 'Invalid authentication token.';
          break;
        default:
          status = 500;
          message = 'Internal authentication error';
      }

      return res.status(status).json({ success: false, error: 'Unauthenticated', message });
    });
}

export function optionalAuth(req, res, next) {
  const token = req.cookies?.token;
  if (!token) {
    req.user = null;
    return next();
  }

  jwtVerify(token, JWT_SECRET, { algorithms: ALLOWED_ALGORITHMS })
    .then(decoded => {
      req.user = {
        id: decoded.id || decoded.sub || decoded.userId,
        email: decoded.email,
        role: decoded.role,
      };
      next();
    })
    .catch(() => {
      req.user = null;
      next();
    });
}
