// middleware/auth.js
import jwt from 'jsonwebtoken';
import { promisify } from 'util';

// Recommended: Load from environment at startup (never hard-code in production)
const JWT_SECRET = process.env.JWT_SECRET;
if (!JWT_SECRET) {
  console.error('[FATAL] JWT_SECRET is not set in environment variables');
  process.exit(1); // Crash in production if missing
}

const jwtVerify = promisify(jwt.verify);

// Optional: Define allowed algorithms (prevents downgrade attacks)
const ALLOWED_ALGORITHMS = ['HS256', 'HS384', 'HS512'];

// Blacklist or short-lived tokens pattern can be added later (Redis/memcache)
// In requireRole middleware

export const requireRole = (requiredRole) => (req, res, next) => {
  if (!req.user) {
    return res.status(401).json({ success: false, error: 'Unauthorized' });
  }

  let userRoleStr;

  if (typeof req.user.role === 'string') {
    userRoleStr = req.user.role.toLowerCase();
  } else if (typeof req.user.role === 'number') {
    const roleMap = { 1: 'admin', 2: 'manager', 3: 'staff', 4: 'viewer' };
    userRoleStr = (roleMap[req.user.role] || 'unknown').toLowerCase();
  } else {
    userRoleStr = 'unknown';
  }

  if (userRoleStr !== requiredRole.toLowerCase()) {
    return res.status(403).json({
      success: false,
      error: 'Forbidden',
      message: `Insufficient permissions - requires ${requiredRole}`,
    });
  }

  next();
};

export function requireAuth(req, res, next) {
  // 1. Get token from cookie (most common in your setup)
  const token = req.cookies?.token;

  if (!token) {
    return res.status(401).json({
      success: false,
      error: 'Unauthenticated',
      message: 'No authentication token provided',
    });
  }

  // 2. Verify token
  jwtVerify(token, JWT_SECRET, {
    algorithms: ALLOWED_ALGORITHMS,
    // Optional: issuer, audience checks if you use them
    // issuer: 'your-app',
    // audience: 'your-app-frontend',
  })
    .then((decoded) => {
      // 3. Basic token shape validation
      if (!decoded || typeof decoded !== 'object') {
        return res.status(401).json({
          success: false,
          error: 'Invalid token payload',
        });
      }

      // 4. Required claims check (adjust according to your token structure)
      if (!decoded.id && !decoded.sub && !decoded.userId) {
        return res.status(401).json({
          success: false,
          error: 'Token missing user identifier',
        });
      }

      // Optional: check expiration manually if needed (already done by jwt.verify)
      // if (decoded.exp && Date.now() >= decoded.exp * 1000) { ... }

      // 5. Attach user to request (standard practice)
      req.user = {
        id: decoded.id || decoded.sub || decoded.userId,
        email: decoded.email,
        role: decoded.role,           // if you store role in token
        // ... other useful claims you include
      };

      // Optional: log successful auth (for audit/security monitoring)
      // logger.info(`Authenticated user: ${req.user.id} (${req.user.email})`);

      return next();
    })
    .catch((err) => {
      console.error('[Auth Middleware] Token verification failed', {
        name: err.name,
        message: err.message,
        // Do NOT log full token in production
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

      return res.status(status).json({
        success: false,
        error: 'Unauthenticated',
        message,
      });
    });
}

// Optional: variant for optional authentication (useful for public + logged-in routes)
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