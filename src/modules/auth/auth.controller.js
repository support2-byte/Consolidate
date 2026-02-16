import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";
import pool from "../../db/pool.js";

const JWT_SECRET = process.env.JWT_SECRET;
const NODE_ENV = process.env.NODE_ENV || "development";
const IS_PRODUCTION = NODE_ENV === "production";

const COOKIE_OPTIONS = {
  httpOnly: true,
  secure: IS_PRODUCTION,
  sameSite: IS_PRODUCTION ? "none" : "lax",
  maxAge: 7 * 24 * 60 * 60 * 1000, // 7 days
  path: "/",
};

export async function register(req, res) {
  const { email, password } = req.body ?? {};

  // Basic validation
  if (!email || !password) {
    return res.status(400).json({ error: "Email and password are required" });
  }

  if (typeof email !== "string" || email.length > 255) {
    return res.status(400).json({ error: "Invalid email format" });
  }

  if (password.length < 8) {
    return res.status(400).json({ error: "Password must be at least 8 characters" });
  }

  try {
    const hash = await bcrypt.hash(password, 12);

    const { rows } = await pool.query(
      `INSERT INTO users (email, password_hash, created_at)
       VALUES ($1, $2, NOW())
       RETURNING id, email`,
      [email.trim().toLowerCase(), hash]
    );

    res.status(201).json({
      success: true,
      user: rows[0],
    });
  } catch (err) {
    if (err.code === "23505") { // unique violation
      return res.status(409).json({ error: "Email already registered" });
    }

    console.error("[REGISTER] Error:", err.message);
    res.status(500).json({ error: "Registration failed. Please try again later." });
  }
}

export async function login(req, res) {
  const { email, password } = req.body ?? {};

  if (!email || !password) {
    return res.status(400).json({ error: "Email and password are required" });
  }

  try {
    const { rows } = await pool.query(
      "SELECT id, email, password_hash FROM users WHERE email = $1",
      [email.trim().toLowerCase()]
    );

    const user = rows[0];
    if (!user) {
      return res.status(401).json({ error: "Invalid email or password" });
    }

    const passwordMatch = await bcrypt.compare(password, user.password_hash);
    if (!passwordMatch) {
      return res.status(401).json({ error: "Invalid email or password" });
    }

    // Create payload – use 'id' instead of 'sub' for clarity
    const payload = {
      id: user.id,
      email: user.email,
      // role: user.role,        // add later if you have roles
      // iat: Math.floor(Date.now() / 1000),
    };

    const token = jwt.sign(payload, JWT_SECRET, {
      expiresIn: "7d",
    });

    res.cookie("token", token, COOKIE_OPTIONS);

    res.json({
      success: true,
      user: {
        id: user.id,
        email: user.email,
      },
    });
  } catch (err) {
    console.error("[LOGIN] Error:", err.message);
    res.status(500).json({ error: "Login failed. Please try again." });
  }
}

// middleware/requireAdmin.js
export function requireAdminRole(req, res, next) {
  if (!req.user || req.user.role !== 'admin') {   // assuming role in JWT payload
    return res.status(403).json({ error: "Admin access required" });
  }
  next();
}

export function me(req, res) {
  const token = req.cookies.token;

  if (!token) {
    return res.status(401).json({ error: "No authentication token found" });
  }

  try {
    const payload = jwt.verify(token, JWT_SECRET);

    // Return consistent user shape
    res.json({
      success: true,
      user: {
        id: payload.id || payload.sub,
        email: payload.email,
      },
    });
  } catch (err) {
    console.warn("[ME] Token verification failed:", err.message);

    if (err.name === "TokenExpiredError") {
      res.clearCookie("token", COOKIE_OPTIONS);
      return res.status(401).json({ error: "Session expired" });
    }

    res.status(401).json({ error: "Invalid authentication token" });
  }
}

// POST  /api/admin/reset-user-password
// Body: { email: "user@example.com", newPassword: "NewPass123!" }
// Requires admin authentication


export async function adminForceResetPassword(req, res) {
  // IMPORTANT: This should be protected by admin-only middleware!
  // Example:
  // if (!req.user || req.user.role !== 'admin') {
  //   return res.status(403).json({ error: "Admin access required" });
  // }

  const { email, newPassword, confirmPassword } = req.body ?? {};

  // ── Input validation ───────────────────────────────────────────────
  if (!email || !newPassword || !confirmPassword) {
    return res.status(400).json({
      error: "Email, new password, and password confirmation are required"
    });
  }

  if (typeof newPassword !== 'string' || newPassword.length < 8) {
    return res.status(400).json({
      error: "New password must be at least 8 characters long"
    });
  }

  if (newPassword !== confirmPassword) {
    return res.status(400).json({
      error: "New password and confirmation do not match"
    });
  }

  try {
    // ── Find target user ──────────────────────────────────────────────
    const { rows } = await pool.query(
      "SELECT id, email FROM users WHERE email = $1",
      [email.trim().toLowerCase()]
    );

    const targetUser = rows[0];
console.log("Target user for password reset:", targetUser);
    // ── Hash the new password ─────────────────────────────────────────
    const saltRounds = 12;
    const passwordHash = await bcrypt.hash(newPassword, saltRounds);

    // ── Perform update ────────────────────────────────────────────────
    await pool.query(
      `
      UPDATE users
      SET 
        password_hash = $1,
        updated_at = NOW()
        -- reset_password_token   = NULL,   -- uncomment if you use reset tokens
        -- reset_password_expires = NULL
      WHERE id = $2
      `,
      [passwordHash, targetUser.id]
    );

    // Optional: Audit log (very recommended for this kind of powerful action)
    // if (req.user?.id) {
    //   await pool.query(
    //     `INSERT INTO admin_actions (admin_id, action, target_user_id, details, created_at)
    //      VALUES ($1, $2, $3, $4, NOW())`,
    //     [req.user.id, 'FORCE_PASSWORD_RESET', targetUser.id, `via admin panel`,]
    //   );
    // }

    // ── Success response ──────────────────────────────────────────────
    res.status(200).json({
      success: true,
      message: `Password successfully updated for ${targetUser.email}`,
      user: {
        id: targetUser.id,
        email: targetUser.email
      }
    });

  } catch (err) {
    console.error("[ADMIN FORCE RESET PASSWORD] Error:", err.message);
    res.status(500).json({
      error: "Server error – password update failed. Please try again later."
    });
  }
}
export function logout(req, res) {
  res.clearCookie("token", COOKIE_OPTIONS);
  res.json({ success: true, message: "Logged out successfully" });
}