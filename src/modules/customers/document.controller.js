import pool from "../../db/pool.js";
import fs from "fs";

export async function getDocuments(req, res) {
  try {
    const { rows } = await pool.query(
      "SELECT * FROM customer_documents WHERE customer_id=$1",
      [req.params.id]
    );
    res.json(rows);
  } catch (err) {
    console.error("Fetch docs failed:", err);
    res.status(500).json({ error: "Failed to fetch documents" });
  }
}

export async function uploadDocument(req, res) {
  try {
    const { filename, path: filepath } = req.file;
    const { rows } = await pool.query(
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

export async function deleteDocument(req, res) {
  try {
    const { rows } = await pool.query(
      "DELETE FROM customer_documents WHERE id=$1 AND customer_id=$2 RETURNING *",
      [req.params.docId, req.params.id]
    );
    if (rows.length > 0) {
      fs.unlink(rows[0].filepath, (err) => {
        if (err) console.warn("File delete error:", err);
      });
    }
    res.json({ ok: true });
  } catch (err) {
    console.error("Delete doc failed:", err);
    res.status(500).json({ error: "Failed to delete document" });
  }
}