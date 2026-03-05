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
  // in login function
const { rows } = await pool.query(
  "SELECT id, email, password_hash, role FROM users WHERE email = $1",
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

  const payload = {
  id: user.id,
  email: user.email,
  role: user.role
};

const token = jwt.sign(payload, JWT_SECRET, { expiresIn: "7d" });

res.cookie("token", token, COOKIE_OPTIONS);

res.json({
  success: true,
  user: {
    id: user.id,
    email: user.email,
    role: user.role
  }
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

export async function me(req, res) {
  const token = req.cookies.token;

  if (!token) {
    return res.status(401).json({ success: false, error: "No authentication token found" });
  }

  try {
    const payload = jwt.verify(token, process.env.JWT_SECRET);
    console.log("[/me] Authenticated payload:", payload);

    // Fetch user with role name (string) from roles table
    const userRes = await pool.query(`
      SELECT 
        u.id,
        u.email,
        u.name AS display_name,          -- or just u.name if that's the field
        COALESCE(r.name, 'unknown') AS role,
        u.active
      FROM users u
      LEFT JOIN roles r ON r.id = u.role
      WHERE u.id = $1
    `, [payload.id]);

    if (userRes.rows.length === 0) {
      return res.status(404).json({ success: false, error: "User not found" });
    }

    const dbUser = userRes.rows[0];

    res.json({
      success: true,
      user: {
        id: dbUser.id,
        email: dbUser.email,
        name: dbUser.display_name || null,
        role: dbUser.role,           // Now returns "viewer", "staff", etc. (string)
        active: dbUser.active,
      }
    });

  } catch (err) {
    console.warn("[/me] Token verification failed:", err.message);

    if (err.name === "TokenExpiredError") {
      res.clearCookie("token", COOKIE_OPTIONS);
      return res.status(401).json({ success: false, error: "Session expired" });
    }

    res.status(401).json({ success: false, error: "Invalid authentication token" });
  }
}

export async function getUsers(req, res) {
  console.log("GET /admin/users - query:", req.query);

  const token = req.cookies.token;
  if (!token) {
    return res.status(401).json({ 
      success: false,
      error: "No authentication token found" 
    });
  }

  try {
    const {
      role,          // can be string like 'staff' or number ID
      active,
      search,
      page = '1',
      limit = '20',
      sort = 'name:asc',
    } = req.query;

    // ─── Pagination ────────────────────────────────
    const pageNum  = parseInt(page, 10) || 1;
    const limitNum = parseInt(limit, 10) || 20;
    const offset   = (pageNum - 1) * limitNum;

    // ─── Sorting ───────────────────────────────────
    let [sortField = 'name', sortDir = 'asc'] = sort.split(':');
    sortField = sortField.trim().toLowerCase();
    sortDir   = sortDir.trim().toLowerCase();

    const fieldMapping = {
      name:       'u.name',
      email:      'u.email',
      role:       'r.name',         // ← changed from r.code
      active:     'u.active',
      created_at: 'u.created_at',
    };

    let dbSortField = fieldMapping[sortField] || 'u.name';
    const orderDir  = ['desc', 'd', '-1'].includes(sortDir) ? 'DESC' : 'ASC';

    // ─── Filters ───────────────────────────────────
    const conditions = [];
    const values = [];
    let paramIndex = 1;

    // Role filter – support both ID (number) and name (string)
    if (role) {
      if (!isNaN(parseInt(role, 10))) {
        conditions.push(`u.role = $${paramIndex++}`);
        values.push(parseInt(role, 10));
      } else {
        conditions.push(`r.name = $${paramIndex++}`);
        values.push(role.trim());
      }
    }

    if (active !== undefined) {
      const activeBool = active === 'true' || active === '1' || active === true;
      conditions.push(`u.active = $${paramIndex++}`);
      values.push(activeBool);
    }

    if (search?.trim()) {
      const searchTerm = `%${search.trim()}%`;
      conditions.push(`
        (u.email ILIKE $${paramIndex} 
         OR u.name ILIKE $${paramIndex})
      `);
      values.push(searchTerm);
      paramIndex++;
    }

    // ─── Base query with JOIN ──────────────────────
let query = `
  SELECT 
    u.id,
    u.name,
    u.email,
    COALESCE(r.name, 'unknown') AS role,
    u.active,
    u.created_at,
    u.updated_at
  FROM users u
  LEFT JOIN roles r ON r.id = u.role
`;

if (conditions.length > 0) {
  query += ` WHERE ${conditions.join(' AND ')}`;
}

query += `
  ORDER BY ${dbSortField} ${orderDir}
  LIMIT $${paramIndex++} 
  OFFSET $${paramIndex}
`;
values.push(limitNum, offset);

// Optional: keep the log, but without comment in SQL
console.log('[getUsers] Executing query:', query.trim());
console.log('[getUsers] Values:', values);
    const { rows } = await pool.query(query, values);

    // ─── Total count ───────────────────────────────
    let countQuery = `
      SELECT COUNT(*) 
      FROM users u
      LEFT JOIN roles r ON r.id = u.role
    `;
    if (conditions.length > 0) {
      countQuery += ` WHERE ${conditions.join(' AND ')}`;
    }

    const countValues = values.slice(0, values.length - 2); // exclude LIMIT & OFFSET
    const { rows: countRows } = await pool.query(countQuery, countValues);
    const total = parseInt(countRows[0].count, 10);

    // ─── Response ──────────────────────────────────
    res.status(200).json({
      success: true,
      users: rows,
      pagination: {
        total,
        page: pageNum,
        limit: limitNum,
        pages: Math.ceil(total / limitNum),
      }
    });

  } catch (err) {
    console.error('[GET /admin/users] Error:', {
      message: err.message,
      code: err.code,
      detail: err.detail,
      stack: err.stack?.substring(0, 300) + '...', // truncate long stack
      query: err.query || 'N/A', // if pg error
    });
    res.status(500).json({
      success: false,
      error: 'Failed to fetch users',
      // message: err.message,   // uncomment only in dev
    });
  }
}
export async function createUser(req, res) {
  console.log("POST /admin/users - body:", req.body);

  const token = req.cookies.token;
  if (!token) {
    return res.status(401).json({ 
      success: false, 
      error: "No authentication token" 
    });
  }

  try {
    const { 
      name, 
      email, 
      password, 
      role = 'staff', 
      active = true 
    } = req.body;

    // Validation
    if (!email || typeof email !== 'string' || !email.includes('@')) {
      return res.status(400).json({ success: false, error: "Valid email is required" });
    }

    if (!password || typeof password !== 'string' || password.length < 8) {
      return res.status(400).json({ 
        success: false, 
        error: "Password is required and must be at least 8 characters" 
      });
    }

    if (!name || typeof name !== 'string' || name.trim() === '') {
      return res.status(400).json({ success: false, error: "Name is required" });
    }

    const hashedPassword = await bcrypt.hash(password, 10);

    // ─── Use your real column name here ─────────────────────────────
    const query = `
      INSERT INTO users (
        name,
        email,
        password_hash,          -- ← changed from password
        role,
        active,
        created_at,
        updated_at
      ) VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
      RETURNING 
        id, 
        name, 
        email, 
        role, 
        active, 
        created_at, 
        updated_at
    `;

    const values = [
      name.trim(),
      email.trim(),
      hashedPassword,
      role,
      active === true || active === 'true' || active === 1
    ];

    const { rows } = await pool.query(query, values);

    res.status(201).json({
      success: true,
      user: rows[0],
    });

  } catch (err) {
    console.error("[POST /admin/users] Error:", err.stack || err.message);

    if (err.code === '23505') {
      return res.status(409).json({
        success: false,
        error: "Email already exists"
      });
    }

    res.status(500).json({
      success: false,
      error: "Failed to create user"
    });
  }
}
export async function updateUser(req, res) {
  const { id } = req.params;
  console.log(`PUT /admin/users/${id} - body:`, req.body);

  const token = req.cookies.token;
  if (!token) {
    return res.status(401).json({ success: false, error: "No authentication token" });
  }

  try {
    const { email, name, role, active, password } = req.body;

    if (Object.keys(req.body).length === 0) {
      return res.status(400).json({ success: false, error: "No fields to update" });
    }

    const updates = [];
    const values = [];
    let paramIndex = 1;

    // Email
    if (email !== undefined) {
      if (typeof email !== 'string' || !email.includes('@') || email.trim() === '') {
        return res.status(400).json({ success: false, error: "Valid email required" });
      }
      const check = await pool.query(
        "SELECT 1 FROM users WHERE email = $1 AND id != $2",
        [email.trim(), id]
      );
      if (check.rowCount > 0) {
        return res.status(409).json({ success: false, error: "Email already in use" });
      }
      updates.push(`email = $${paramIndex}`);
      values.push(email.trim());
      paramIndex++;
    }

    // Name
    if (name !== undefined) {
      if (typeof name !== 'string' || name.trim() === '') {
        return res.status(400).json({ success: false, error: "Name cannot be empty" });
      }
      updates.push(`name = $${paramIndex}`);
      values.push(name.trim());
      paramIndex++;
    }

    // Role – handle both string name and numeric ID
    if (role !== undefined) {
      let roleId;

      if (typeof role === 'number' || !isNaN(parseInt(role, 10))) {
        // Already a number/ID
        roleId = parseInt(role, 10);
      } else if (typeof role === 'string') {
        // String name → lookup ID
        const roleRes = await pool.query(
          "SELECT id FROM roles WHERE name = $1",
          [role.trim()]
        );
        if (roleRes.rows.length === 0) {
          return res.status(400).json({
            success: false,
            error: `Invalid role name "${role}". Allowed: admin, manager, staff, viewer`
          });
        }
        roleId = roleRes.rows[0].id;
      } else {
        return res.status(400).json({ success: false, error: "Invalid role format" });
      }

      // Optional: validate roleId exists (extra safety)
      const idCheck = await pool.query("SELECT 1 FROM roles WHERE id = $1", [roleId]);
      if (idCheck.rowCount === 0) {
        return res.status(400).json({ success: false, error: "Invalid role ID" });
      }

      updates.push(`role = $${paramIndex}`);
      values.push(roleId);
      paramIndex++;
    }

    // Active
    if (active !== undefined) {
      const isActive = active === true || active === 'true' || active === 1 || active === '1';
      updates.push(`active = $${paramIndex}`);
      values.push(!!isActive);
      paramIndex++;
    }

    // Password (hash it!)
    if (password !== undefined) {
      if (typeof password !== 'string' || password.length < 8) {
        return res.status(400).json({ success: false, error: "Password min 8 chars" });
      }
      // TODO: Replace with real hashing
      const hashedPassword = password; // ← REPLACE WITH bcrypt.hash(password, 12)
      updates.push(`password_hash = $${paramIndex}`);
      values.push(hashedPassword);
      paramIndex++;
    }

    if (updates.length === 0) {
      return res.status(400).json({ success: false, error: "No valid fields to update" });
    }

    updates.push(`updated_at = NOW()`);
    values.push(id);

    const query = `
      UPDATE users
      SET ${updates.join(", ")}
      WHERE id = $${paramIndex}
      RETURNING id, name, email, role, active, created_at, updated_at
    `;

    const { rows } = await pool.query(query, values);

    if (rows.length === 0) {
      return res.status(404).json({ success: false, error: "User not found" });
    }

    res.status(200).json({
      success: true,
      user: rows[0],
    });
  } catch (err) {
    console.error(`[PUT /admin/users/${id}] Error:`, err.message, err.stack);

    if (err.code === '22P02') { // invalid input syntax
      return res.status(400).json({ success: false, error: "Invalid data format" });
    }

    if (err.code === '23505') {
      return res.status(409).json({ success: false, error: "Email already in use" });
    }

    res.status(500).json({
      success: false,
      error: "Failed to update user",
    });
  }
}
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