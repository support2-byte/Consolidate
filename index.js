import express from "express";
import pkg from "pg";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";
import cookieParser from "cookie-parser";
import cors from "cors";
import dotenv from "dotenv";
import axios from "axios";
import multer from "multer";
import path from "path";
import fs from "fs";
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

// Add Replit domains to allowed origins
const replitDomains = process.env.REPLIT_DOMAINS ? process.env.REPLIT_DOMAINS.split(",") : [];
const allAllowedOrigins = [...allowedOrigins, ...replitDomains];

app.use(
  cors({
    origin: (origin, callback) => {
      // Always allow requests without origin (like mobile apps, Postman)
      if (!origin) {
        return callback(null, true);
      }
      
      // Allow all localhost origins for development
      if (origin.includes('localhost') || origin.includes('127.0.0.1')) {
        return callback(null, true);
      }
      
      // Allow Replit domains
      if (origin.includes('replit.dev') || origin.includes('repl.co')) {
        return callback(null, true);
      }
      
      // Allow configured origins
      if (allAllowedOrigins.includes(origin)) {
        return callback(null, true);
      }
      
      // In development, allow all origins
      if (process.env.NODE_ENV !== "production") {
        return callback(null, true);
      }
      
      callback(new Error("Not allowed by CORS"));
    },
    credentials: true,
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'Cookie'],
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

// app.get("/api/users", auth, async (req, res) => {
//   try {
//     const { rows } = await pool.query(
//       "SELECT id, name FROM users ORDER BY name ASC"
//     );
//     res.json(rows);
//   } catch (err) {
//     console.error("Error fetching users", err.message);
//     res.status(500).json({ error: "Failed to fetch users" });
//   }
// });


let zohoAccessToken = null;

async function getZohoAccessToken() {
  // console.log("Getting Zoho access token...",zohoAccessToken);
  if (zohoAccessToken) return zohoAccessToken;

  const res = await axios.post("https://accounts.zoho.com/oauth/v2/token", null, {
    params: {
      refresh_token: process.env.ZOHO_REFRESH_TOKEN,
      client_id: process.env.ZOHO_CLIENT_ID,
      client_secret: process.env.ZOHO_CLIENT_SECRET,
      grant_type: "refresh_token",
    },
  });
// console.log("Zoho token response:", res.data);
  zohoAccessToken = res.data.access_token;

  // Expire cache 1 minute before Zoho expiry
  setTimeout(() => {
    zohoAccessToken = null;
  }, (res.data.expires_in - 60) * 1000);

  return zohoAccessToken;
}


// ------------------- MULTER SETUP ------------------
const upload = multer({ dest: "uploads/" });

// ----------------- CUSTOMERS -----------------
app.get("/api/customers", auth, async (req, res) => {
  try {
    const token = await getZohoAccessToken();

    // 1️⃣ Fetch contacts from Zoho CRM
    const zohoRes = await axios.get("https://www.zohoapis.com/crm/v2/Contacts", {
      headers: { Authorization: `Zoho-oauthtoken ${token}` },
    }); 
 

    const contacts = zohoRes.data.data || [];
    // console.log("Fetched contacts from Zoho:", contacts);
    // 2️⃣ Sync into local DB
    for (const c of contacts) {
      await pool.query(
        `INSERT INTO customers 
  (id, zoho_id, full_name, first_name, last_name, email, phone, mobile, title, department, lead_source, mailing_city, mailing_country, account_name, owner, created_time, modified_time, created_by, modified_by, status)
VALUES 
  ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
         ON CONFLICT (zoho_id) DO UPDATE SET
           full_name = EXCLUDED.full_name,
           first_name = EXCLUDED.first_name,
           last_name = EXCLUDED.last_name,
           email = EXCLUDED.email,
           phone = EXCLUDED.phone,
           mobile = EXCLUDED.mobile,
           title = EXCLUDED.title,
           department = EXCLUDED.department,
           lead_source = EXCLUDED.lead_source,
           mailing_city = EXCLUDED.mailing_city,
           mailing_country = EXCLUDED.mailing_country,
           account_name = EXCLUDED.account_name,
           owner = EXCLUDED.owner,
           modified_time = EXCLUDED.modified_time,
            created_by = EXCLUDED.created_by,
     modified_by = EXCLUDED.modified_by,
     status = EXCLUDED.status`,
        [
          uuidv4(),                           // always generate UUID for id
          c.id,                               // Zoho’s ID
          c.Full_Name || null,
          c.First_Name || null,
          c.Last_Name || null,
          c.Email || null,
          c.Phone || null,
          c.Mobile || null,
          c.Title || null,
          c.Department || null,
          c.Lead_Source || null,
          c.Mailing_City || null,
          c.Mailing_Country || null,
          c.Account_Name?.name || null,
          JSON.stringify(c.Owner) || null,
          c.Created_Time || null,
          c.Modified_Time || null,
        c.Created_By?.name || null,
c.Modified_By?.name || null,
    true
        ]
      );
    }

    // 3️⃣ Fetch from local DB
    const { rows } = await pool.query("SELECT * FROM customers ORDER BY created_time DESC");
    res.json(rows);
  } catch (err) {
    console.error("Zoho sync error", err.response?.data || err.message);
    res.status(500).json({ error: "Failed to fetch customers" });
  }
});

// Get a single customer by ID
// Get a single customer by Zoho ID OR local ID
app.get("/api/customers/:id", auth, async (req, res) => {
  try {
    const { id } = req.params;

    // Check both zoho_id and local id
    const { rows } = await q(
      "SELECT * FROM customers WHERE id ::text = $1",
      [id]
    );

    if (rows.length === 0) {
      return res.status(404).json({ error: "Customer not found" });
    }

    const customer = rows[0];

    // Use local id if available, otherwise fallback to zoho_id
    const customerKey = customer.id;

    // Fetch related contacts
    const contactsRes = await q(
      "SELECT * FROM customer_contacts WHERE customer_id=$1",
      [customerKey]
    );

    // Fetch related documents
    const docsRes = await q(
      "SELECT * FROM customer_documents WHERE customer_id=$1",
      [customerKey]
    );

    customer.contacts = contactsRes.rows;
    customer.documents = docsRes.rows;

    res.json(customer);
  } catch (err) {
    console.error("Fetch single customer failed:", err);
    res.status(500).json({ error: "Failed to fetch customer" });
  }
});
const safeJson = (value) => {
  if (!value) return null;
  try {
    return JSON.stringify(value);
  } catch {
    return null;
  }
}
app.put("/api/customers/:id", auth, async (req, res) => {
  try {
    const { id } = req.params;
    const {
      name,
      email,
      associated_by,
      zoho_notes,
      address,
      system_notes,
      type,
    } = req.body;

    console.log("Update customer called", id, req.body);

    const { rows } = await q(
      `UPDATE customers
       SET account_name = $1,
           email        = $2,
           associated_by= $3,
           zoho_notes   = $4,
           address      = $5,
           system_notes = $6,
           type         = $7,
           modified_by  = $8,   -- ✅ now its own param
           status       = true
       WHERE id = $9
       RETURNING *`,
      [name, email, associated_by, zoho_notes, address, system_notes, type, name, id] // 🔑 use `name` again
    );

    if (rows.length === 0) {
      return res.status(404).json({ error: "Customer not found" });
    }

    res.json(rows[0]);
  } catch (err) {
    console.error("Update customer failed:", err);
    res.status(500).json({ error: "Failed to update customer" });
  }
});

app.delete("/api/customers/:id", auth, async (req, res) => {
  const { id } = req.params;
  await q("DELETE FROM customers WHERE id=$1", [id]);
  res.json({ ok: true });
});
// Ensure uploads folder exists
const uploadDir = path.join(process.cwd(), "uploads");
if (!fs.existsSync(uploadDir)) {
  fs.mkdirSync(uploadDir);
}

// Multer setup
const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, uploadDir),
  filename: (req, file, cb) =>
    cb(null, Date.now() + "-" + file.originalname)
});
import { v4 as uuidv4 } from "uuid";
// -------------------- CONTACTS --------------------
// Create customer (Dashboard only)
app.post("/api/customers", auth, async (req, res) => {
  const {
    name,
    email,
    phone,
    address,
    zoho_notes,
    system_notes,
    type,
    associated_by,
   
    
  } = req.body ?? {};

  try {
    const newId = uuidv4(); // generate a UUID for dashboard-created customer

    const { rows } = await pool.query(
      `INSERT INTO customers 
        (id, account_name, email, phone, address, zoho_notes, system_notes, type, associated_by, status, created_by)
       VALUES 
        ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
       RETURNING *`,
      [
        newId,                     // $1
        name,                      // $2
        email,                     // $3
        phone,                     // $4
        address,                   // $5
        zoho_notes,                // $6
        system_notes,              // $7
        type,                      // $8
        associated_by,             // $9
        true,                      // $10 (status always true on insert)
        name
      ]
    );

    res.status(201).json(rows[0]);
  } catch (err) {
    console.error("Insert customer failed:", err);
    res.status(500).json({ error: "Failed to insert customer" });
  }
});
// Get contacts of a customer
app.get("/api/customers/:id/contacts", auth, async (req, res) => {
  try {
    const { rows } = await q(
      "SELECT * FROM customer_contacts WHERE customer_id=$1",
      [req.params.id]
    );
    res.json(rows);
  } catch (err) {
    console.error("Fetch contacts failed:", err);
    res.status(500).json({ error: "Failed to fetch contacts" });
  }
});

// Add or update one or multiple contacts
app.post("/api/customers/:id/contacts", auth, async (req, res) => {
  try {
    const contacts = Array.isArray(req.body) ? req.body : [req.body];
    const saved = [];

    for (const c of contacts) {
      const { id, name, phone, email, designation, isNew } = c;

      const { rows } = await q(
        `INSERT INTO customer_contacts (id, customer_id, name, phone, email, designation)
         VALUES (COALESCE($1, gen_random_uuid()), $2, $3, $4, $5, $6)
         ON CONFLICT (id) DO UPDATE
         SET name = EXCLUDED.name,
             phone = EXCLUDED.phone,
             email = EXCLUDED.email,
             designation = EXCLUDED.designation
         RETURNING *`,
        [id || null, req.params.id, name, phone, email, designation]
      );

      saved.push(rows[0]);
    }

    res.status(201).json(saved);
  } catch (err) {
    console.error("Insert/Update contact(s) failed:", err);
    res.status(500).json({ error: "Failed to insert/update contact(s)" });
  }
});

// Delete contact
app.delete("/api/customers/:id/contacts/:contactId", auth, async (req, res) => {
  try {
    const { id, contactId } = req.params;
    await q("DELETE FROM customer_contacts WHERE id=$1 AND customer_id=$2", [
      contactId,
      id,
    ]);
    res.json({ ok: true });
  } catch (err) {
    console.error("Delete contact failed:", err);
    res.status(500).json({ error: "Failed to delete contact" });
  }
});
// -------------------- DOCUMENTS --------------------

// Get documents of a customer
app.get("/api/customers/:id/documents", auth, async (req, res) => {
  try {
    const { rows } = await q(
      "SELECT * FROM customer_documents WHERE customer_id=$1",
      [req.params.id]
    );
    res.json(rows);
  } catch (err) {
    console.error("Fetch docs failed:", err);
    res.status(500).json({ error: "Failed to fetch documents" });
  }
});

// Upload document
app.post(
  "/api/customers/:id/documents",
  auth,
  upload.single("file"),
  async (req, res) => {
    try {
      const { filename, path: filepath } = req.file;
      const { rows } = await q(
        `INSERT INTO customer_documents (customer_id, filename, filepath)
         VALUES ($1,$2,$3) RETURNING *`,
        [req.params.id, filename, filepath]
      );
      res.status(201).json(rows[0]);
    } catch (err) {
      console.error("Upload doc failed:", err);
      res.status(500).json({ error: "Failed to upload document" });
    }
  }
);

// Delete document
app.delete("/api/customers/:id/documents/:docId", auth, async (req, res) => {
  try {
    const { rows } = await q(
      "DELETE FROM customer_documents WHERE id=$1 AND customer_id=$2 RETURNING *",
      [req.params.docId, req.params.id]
    );

    if (rows.length > 0) {
      // remove file from disk
      fs.unlink(rows[0].filepath, (err) => {
        if (err) console.warn("File delete error:", err);
      });
    }

    res.json({ ok: true });
  } catch (err) {
    console.error("Delete doc failed:", err);
    res.status(500).json({ error: "Failed to delete document" });
  }
});


//   try {
//     const token = await getZohoAccessToken();

//     // 1️⃣ Fetch contacts from Zoho
//     const zohoRes = await axios.get("https://www.zohoapis.com/crm/v2/Contacts", {
//       headers: { Authorization: `Zoho-oauthtoken ${token}` },
//     });

//     const contacts = zohoRes.data.data || [];
//     console.log("Fetched contacts from Zohosadsadasda:", contacts);

//     // 2️⃣ Sync into local DB
//     for (const c of contacts) {
//   await pool.query(
//     `INSERT INTO customers 
//       (id, full_name, first_name, last_name, email, phone, mobile, title, department, lead_source, mailing_city, mailing_country, account_name, owner, created_time, modified_time)
//      VALUES 
//       ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
//      ON CONFLICT (id) DO UPDATE SET
//        full_name = EXCLUDED.full_name,
//        first_name = EXCLUDED.first_name,
//        last_name = EXCLUDED.last_name,
//        email = EXCLUDED.email,
//        phone = EXCLUDED.phone,
//        mobile = EXCLUDED.mobile,
//        title = EXCLUDED.title,
//        department = EXCLUDED.department,
//        lead_source = EXCLUDED.lead_source,
//        mailing_city = EXCLUDED.mailing_city,
//        mailing_country = EXCLUDED.mailing_country,
//        account_name = EXCLUDED.account_name,
//        owner = EXCLUDED.owner,
//        modified_time = EXCLUDED.modified_time`,
//     [
//       c.id,
//       c.Full_Name || null,
//       c.First_Name || null,
//       c.Last_Name || null,
//       c.Email || null,
//       c.Phone || null,
//       c.Mobile || null,
//       c.Title || null,
//       c.Department || null,
//       c.Lead_Source || null,
//       c.Mailing_City || null,
//       c.Mailing_Country || null,
//       c.Account_Name?.name || null,
//       JSON.stringify(c.Owner) || null,
//       c.Created_Time || null,
//       c.Modified_Time || null,
//     ]
    
//   );




//     }

//     // 3️⃣ Fetch from DB and return
//     const { rows } = await pool.query("SELECT * FROM customers ORDER BY id ASC");
//     res.json(rows);
//   } catch (err) {
//     console.error("Zoho sync error", err.response?.data || err.message);
//     res.status(500).json({ error: "Failed to fetch customers" });
//   }
// });


// // ---------- CUSTOMERS ----------
// // app.get("/api/customers", auth, async (_req, res) => {
// //   const { rows } = await q("SELECT * FROM customers ORDER BY id ASC");
// //   res.json(rows);
// // });
// app.post("/api/customers", auth, async (req, res) => {
//   const { name, email, phone } = req.body ?? {};
//   const { rows } = await q(
//     "INSERT INTO customers(name,email,phone) VALUES($1,$2,$3) RETURNING *",
//     [name, email, phone]
//   );
//   res.status(201).json(rows[0]);
// });


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
const PORT = process.env.PORT || 3000;
app.listen(PORT, "localhost", () =>
  console.log(`✅ API running on http://localhost:${PORT}`)
);
