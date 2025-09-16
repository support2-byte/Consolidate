import pool from "../../db/pool.js";

export async function getContacts(req, res) {
  try {
    const { rows } = await pool.query(
      "SELECT * FROM customer_contacts WHERE customer_id=$1",
      [req.params.id]
    );
    res.json(rows);
  } catch (err) {
    console.error("Fetch contacts failed:", err);
    res.status(500).json({ error: "Failed to fetch contacts" });
  }
}

export async function saveContacts(req, res) {
  try {
    const contacts = Array.isArray(req.body) ? req.body : [req.body];
    const saved = [];
    for (const c of contacts) {
      const { id, name, phone, email, designation } = c;
      const { rows } = await pool.query(
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
}

export async function deleteContact(req, res) {
  try {
    const { id, contactId } = req.params;
    await pool.query("DELETE FROM customer_contacts WHERE id=$1 AND customer_id=$2", [
      contactId,
      id,
    ]);
    res.json({ ok: true });
  } catch (err) {
    console.error("Delete contact failed:", err);
    res.status(500).json({ error: "Failed to delete contact" });
  }
}