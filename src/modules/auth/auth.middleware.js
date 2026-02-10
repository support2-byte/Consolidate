// middleware/auth.js
import jwt from "jsonwebtoken";

const JWT_SECRET = process.env.JWT_SECRET || "fallback-secret-for-dev-only-change-me";

export function requireAuth(req, res, next) {
  const token = req.cookies?.token;

  if (!token) {
    return res.status(401).json({
      error: "Unauthenticated",
      message: "No authentication token provided",
    });
  }

  try {
    const decoded = jwt.verify(token, JWT_SECRET);

    // Optional: you can add extra checks
    if (!decoded.email && !decoded.sub) {
      return res.status(401).json({ error: "Token missing user identifier" });
    }

    req.user = decoded; // { id, email, sub?, role?, ... }
    next();
  } catch (err) {
    console.error("[Auth] Token verification failed:", {
      error: err.message,
      token: token.substring(0, 20) + "...", // partial for security
    });

    if (err.name === "TokenExpiredError") {
      return res.status(401).json({ error: "Token expired" });
    }
    if (err.name === "JsonWebTokenError") {
      return res.status(401).json({ error: "Invalid token" });
    }

    return res.status(401).json({ error: "Authentication failed" });
  }
}