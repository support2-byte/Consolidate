import pool from "../../db/pool.js";
import axios from "axios";
import { v4 as uuidv4 } from "uuid";

let zohoAccessToken = null;

async function getZohoAccessToken() {
  if (zohoAccessToken) return zohoAccessToken;
  const res = await axios.post("https://accounts.zoho.com/oauth/v2/token", null, {
    params: {
      refresh_token: process.env.ZOHO_REFRESH_TOKEN,
      client_id: process.env.ZOHO_CLIENT_ID,
      client_secret: process.env.ZOHO_CLIENT_SECRET,
      grant_type: "refresh_token",
    },
  });
  zohoAccessToken = res.data.access_token;
  setTimeout(() => { zohoAccessToken = null; }, (res.data.expires_in - 60) * 1000);
  return zohoAccessToken;
}

export async function getCustomers(req, res) {
  try {
    const token = await getZohoAccessToken();
    const zohoRes = await axios.get("https://www.zohoapis.com/crm/v2/Contacts", {
      headers: { Authorization: `Zoho-oauthtoken ${token}` },
    });
    const contacts = zohoRes.data.data || [];
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
          uuidv4(),
          c.id,
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
    const { rows } = await pool.query("SELECT * FROM customers ORDER BY created_time DESC");
    res.json(rows);
  } catch (err) {
    console.error("Zoho sync error", err.response?.data || err.message);
    res.status(500).json({ error: "Failed to fetch customers" });
  }
}

export async function getCustomerById(req, res) {
  try {
    const { id } = req.params;
    const { rows } = await pool.query(
      "SELECT * FROM customers WHERE id ::text = $1",
      [id]
    );
    if (rows.length === 0) {
      return res.status(404).json({ error: "Customer not found" });
    }
    const customer = rows[0];
    const contactsRes = await pool.query(
      "SELECT * FROM customer_contacts WHERE customer_id=$1",
      [customer.id]
    );
    const docsRes = await pool.query(
      "SELECT * FROM customer_documents WHERE customer_id=$1",
      [customer.id]
    );
    customer.contacts = contactsRes.rows;
    customer.documents = docsRes.rows;
    res.json(customer);
  } catch (err) {
    console.error("Fetch single customer failed:", err);
    res.status(500).json({ error: "Failed to fetch customer" });
  }
}

export async function updateCustomer(req, res) {
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
    const { rows } = await pool.query(
      `UPDATE customers
       SET account_name = $1,
           email        = $2,
           associated_by= $3,
           zoho_notes   = $4,
           address      = $5,
           system_notes = $6,
           type         = $7,
           modified_by  = $8,
           status       = true
       WHERE id = $9
       RETURNING *`,
      [name, email, associated_by, zoho_notes, address, system_notes, type, name, id]
    );
    if (rows.length === 0) {
      return res.status(404).json({ error: "Customer not found" });
    }
    res.json(rows[0]);
  } catch (err) {
    console.error("Update customer failed:", err);
    res.status(500).json({ error: "Failed to update customer" });
  }
}

export async function deleteCustomer(req, res) {
  const { id } = req.params;
  await pool.query("DELETE FROM customers WHERE id=$1", [id]);
  res.json({ ok: true });
}

export async function createCustomer(req, res) {
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
    const newId = uuidv4();
    const { rows } = await pool.query(
      `INSERT INTO customers 
        (id, account_name, email, phone, address, zoho_notes, system_notes, type, associated_by, status, created_by)
       VALUES 
        ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
       RETURNING *`,
      [
        newId,
        name,
        email,
        phone,
        address,
        zoho_notes,
        system_notes,
        type,
        associated_by,
        true,
        name
      ]
    );
    res.status(201).json(rows[0]);
  } catch (err) {
    console.error("Insert customer failed:", err);
    res.status(500).json({ error: "Failed to insert customer" });
  }
}