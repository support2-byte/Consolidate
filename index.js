import express from "express";
import pkg from "pg";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";
import cookieParser from "cookie-parser";
import cors from "cors";
import dotenv from "dotenv";

dotenv.config();
const { Pool } = pkg;
const app = express();

// ---------- DATABASE ----------
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  // ssl: { rejectUnauthorized: false },
});

// ---------- MIDDLEWARE ----------
app.use(express.json());
app.use(cookieParser());

// Allowed origins from env (comma separated) or fallback
const allowedOrigins = process.env.CLIENT_ORIGINS
  ? process.env.CLIENT_ORIGINS.split(",")
  : [
      "http://localhost:5173", // dev frontend
      "https://imaginative-pothos-0a1193.netlify.app", // prod frontend
    ];

app.use(
  cors({
    origin: (origin, callback) => {
      if (!origin || allowedOrigins.includes(origin)) {
        callback(null, true);
      } else {
        callback(new Error("Not allowed by CORS"));
      }
    },
    credentials: true,
  })
);

const JWT_SECRET = process.env.JWT_SECRET;

// ---------- AUTH MIDDLEWARE ----------
function auth(req, res, next) {
  const token = req.cookies.token;
  if (!token) return res.status(401).json({ error: "Unauthenticated" });

  try {
    req.user = jwt.verify(token, JWT_SECRET);
    next();
  } catch {
    res.status(401).json({ error: "Invalid token" });
  }
}

// ---------- AUTH ROUTES ----------
app.post("/auth/register", async (req, res) => {
  const { email, password } = req.body ?? {};
  if (!email || !password)
    return res.status(400).json({ error: "Email & password required" });

  const hash = await bcrypt.hash(password, 12);
  try {
    const { rows } = await pool.query(
      "INSERT INTO users (email, password_hash) VALUES ($1,$2) RETURNING id,email",
      [email, hash]
    );
    res.status(201).json({ user: rows[0] });
  } catch (e) {
    if (e.code === "23505")
      return res.status(409).json({ error: "Email already registered" });
    console.error(e);
    res.status(500).json({ error: "Server error" });
  }
});

app.post("/auth/login", async (req, res) => {
  const { email, password } = req.body ?? {};
  const { rows } = await pool.query(
    "SELECT id,email,password_hash FROM users WHERE email=$1",
    [email]
  );
  const user = rows[0];
  if (!user) return res.status(400).json({ error: "Invalid credentials" });

  const ok = await bcrypt.compare(password, user.password_hash);
  if (!ok) return res.status(400).json({ error: "Invalid credentials" });

  const token = jwt.sign({ sub: user.id, email: user.email }, JWT_SECRET, {
    expiresIn: "7d",
  });

  res.cookie("token", token, {
    httpOnly: true,
    sameSite: process.env.NODE_ENV === "production" ? "none" : "lax",
    secure: process.env.NODE_ENV === "production",
  });

  res.json({ user: { id: user.id, email: user.email } });
});

app.get("/auth/me", (req, res) => {
  const token = req.cookies.token;
  if (!token) return res.status(401).json({ error: "No token" });

  try {
    const payload = jwt.verify(token, JWT_SECRET);
    res.json({ user: { id: payload.sub, email: payload.email } });
  } catch {
    res.status(401).json({ error: "Invalid token" });
  }
});

app.post("/auth/logout", (req, res) => {
  res.clearCookie("token", {
    httpOnly: true,
    sameSite: process.env.NODE_ENV === "production" ? "none" : "lax",
    secure: process.env.NODE_ENV === "production",
  });
  res.json({ ok: true });
});

// ---------- CRUD HELPER ----------
const q = (text, params) => pool.query(text, params);

// ---------- CUSTOMERS ----------
app.get("/api/customers", auth, async (_req, res) => {
  const { rows } = await q("SELECT * FROM customers ORDER BY id ASC");
  res.json(rows);
});
app.post("/api/customers", auth, async (req, res) => {
  const { name, email, phone } = req.body ?? {};
  const { rows } = await q(
    "INSERT INTO customers(name,email,phone) VALUES($1,$2,$3) RETURNING *",
    [name, email, phone]
  );
  res.status(201).json(rows[0]);
});
app.put("/api/customers/:id", auth, async (req, res) => {
  const { id } = req.params;
  const { name, email, phone } = req.body ?? {};
  const { rows } = await q(
    "UPDATE customers SET name=$1,email=$2,phone=$3 WHERE id=$4 RETURNING *",
    [name, email, phone, id]
  );
  res.json(rows[0]);
});
app.delete("/api/customers/:id", auth, async (req, res) => {
  const { id } = req.params;
  await q("DELETE FROM customers WHERE id=$1", [id]);
  res.json({ ok: true });
});

// ---------- VENDORS ----------
app.get("/api/vendors", auth, async (_req, res) => {
  const { rows } = await q("SELECT * FROM vendors ORDER BY id ASC");
  res.json(rows);
});
app.post("/api/vendors", auth, async (req, res) => {
  const { name, contact_person, phone } = req.body ?? {};
  const { rows } = await q(
    "INSERT INTO vendors(name,contact_person,phone) VALUES($1,$2,$3) RETURNING *",
    [name, contact_person, phone]
  );
  res.status(201).json(rows[0]);
});
app.put("/api/vendors/:id", auth, async (req, res) => {
  const { id } = req.params;
  const { name, contact_person, phone } = req.body ?? {};
  const { rows } = await q(
    "UPDATE vendors SET name=$1,contact_person=$2,phone=$3 WHERE id=$4 RETURNING *",
    [name, contact_person, phone, id]
  );
  res.json(rows[0]);
});
app.delete("/api/vendors/:id", auth, async (req, res) => {
  const { id } = req.params;
  await q("DELETE FROM vendors WHERE id=$1", [id]);
  res.json({ ok: true });
});

// ---------- CONTAINERS ----------
app.get("/api/containers", auth, async (_req, res) => {
  const { rows } = await q("SELECT * FROM containers ORDER BY id ASC");
  res.json(rows);
});
app.post("/api/containers", auth, async (req, res) => {
  const { container_number, type, status } = req.body ?? {};
  const { rows } = await q(
    "INSERT INTO containers(container_number,type,status) VALUES($1,$2,$3) RETURNING *",
    [container_number, type, status]
  );
  res.status(201).json(rows[0]);
});
app.put("/api/containers/:id", auth, async (req, res) => {
  const { id } = req.params;
  const { container_number, type, status } = req.body ?? {};
  const { rows } = await q(
    "UPDATE containers SET container_number=$1,type=$2,status=$3 WHERE id=$4 RETURNING *",
    [container_number, type, status, id]
  );
  res.json(rows[0]);
});
app.delete("/api/containers/:id", auth, async (req, res) => {
  const { id } = req.params;
  await q("DELETE FROM containers WHERE id=$1", [id]);
  res.json({ ok: true });
});

// ---------- ORDERS ----------
app.get("/api/orders", auth, async (_req, res) => {
  const { rows } = await q(
    `SELECT o.*, c.name AS customer_name
     FROM orders o LEFT JOIN customers c ON c.id=o.customer_id
     ORDER BY o.id ASC`
  );
  res.json(rows);
});
app.post("/api/orders", auth, async (req, res) => {
  const { customer_id, order_date, status, total } = req.body ?? {};
  const { rows } = await q(
    "INSERT INTO orders(customer_id,order_date,status,total) VALUES($1,$2,$3,$4) RETURNING *",
    [customer_id || null, order_date || null, status, total || null]
  );
  res.status(201).json(rows[0]);
});
app.put("/api/orders/:id", auth, async (req, res) => {
  const { id } = req.params;
  const { customer_id, order_date, status, total } = req.body ?? {};
  const { rows } = await q(
    "UPDATE orders SET customer_id=$1,order_date=$2,status=$3,total=$4 WHERE id=$5 RETURNING *",
    [customer_id || null, order_date || null, status, total || null, id]
  );
  res.json(rows[0]);
});
app.delete("/api/orders/:id", auth, async (req, res) => {
  const { id } = req.params;
  await q("DELETE FROM orders WHERE id=$1", [id]);
  res.json({ ok: true });
});

// ---------- CONSIGNMENTS ----------
app.get("/api/consignments", auth, async (_req, res) => {
  const { rows } = await q(
    `SELECT cg.*, o.id AS order_no, ct.container_number
     FROM consignments cg
     LEFT JOIN orders o ON o.id=cg.order_id
     LEFT JOIN containers ct ON ct.id=cg.container_id
     ORDER BY cg.id ASC`
  );
  res.json(rows);
});
app.post("/api/consignments", auth, async (req, res) => {
  const { order_id, container_id, shipment_date, status } = req.body ?? {};
  const { rows } = await q(
    "INSERT INTO consignments(order_id,container_id,shipment_date,status) VALUES($1,$2,$3,$4) RETURNING *",
    [order_id || null, container_id || null, shipment_date || null, status]
  );
  res.status(201).json(rows[0]);
});
app.put("/api/consignments/:id", auth, async (req, res) => {
  const { id } = req.params;
  const { order_id, container_id, shipment_date, status } = req.body ?? {};
  const { rows } = await q(
    "UPDATE consignments SET order_id=$1,container_id=$2,shipment_date=$3,status=$4 WHERE id=$5 RETURNING *",
    [order_id || null, container_id || null, shipment_date || null, status, id]
  );
  res.json(rows[0]);
});
app.delete("/api/consignments/:id", auth, async (req, res) => {
  const { id } = req.params;
  await q("DELETE FROM consignments WHERE id=$1", [id]);
  res.json({ ok: true });
});

// ---------- HEALTH CHECK ----------
app.get("/health", (_req, res) => {
  res.json({ status: "ok", time: new Date().toISOString() });
});

// ---------- SERVER ----------
const PORT = process.env.PORT || 5000;
app.listen(PORT, () =>
  console.log(`✅ API running on http://localhostsasas:${PORT}`)
);
