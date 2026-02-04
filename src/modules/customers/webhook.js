import express from "express";
import pool from "../../db/pool.js";
import axios from "axios";
import fs from "fs"
import FormData from "form-data";
import { v4 as uuidv4 } from "uuid";
import path from "path";
const router = express.Router();

let zohoAccessToken = null;
const ORG_ID = process.env.ZOHO_BOOKS_ORG_ID;  

// ... imports unchanged
router.post('/zoho-customer', async (req, res) => {
  console.log("Received Zoho webhook for customer sync");

  try {
    const secret = req.query.secret;
    if (secret !== process.env.ZOHO_WEBHOOK_SECRET) {
      console.warn("Invalid webhook secret");
      return res.status(200).json({ message: "Ignored" });
    }

    let contact = req.body;

    // ─── THIS IS THE FIX ───
    // Zoho wraps real events in { "contact": { ... } }
    if (contact && contact.contact) {
      contact = contact.contact;
      console.log("Unwrapped nested 'contact' → ready to sync");
    }

    if (!contact?.contact_id) {
      console.log("Ignored: missing contact_id after unwrap");
      return res.status(200).json({ message: "Ignored: missing contact_id" });
    }

    if (contact.contact_type !== 'customer') {
      console.log(`Ignored: not a customer (type: ${contact.contact_type})`);
      return res.status(200).json({ message: "Ignored: not a customer" });
    }

    console.log(`Syncing customer: ${contact.contact_id} - ${contact.contact_name || 'no name'}`);

    // Your existing data preparation + upsert (unchanged)
    const addressStr = contact.billing_address ? JSON.stringify(contact.billing_address) : null;
    const createdTime = contact.created_time ? new Date(contact.created_time).toISOString() : null;
    const modifiedTime = contact.last_modified_time ? new Date(contact.last_modified_time).toISOString() : null;

    await pool.query(
      `INSERT INTO customers 
        (zoho_id, contact_name, email, address, zoho_notes, associated_by, 
         system_notes, contact_type, status, created_by, modified_by, created_time, modified_time)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
       ON CONFLICT (zoho_id) DO UPDATE SET
         contact_name = COALESCE(EXCLUDED.contact_name, customers.contact_name),
         email = COALESCE(EXCLUDED.email, customers.email),
         address = COALESCE(EXCLUDED.address, customers.address),
         zoho_notes = COALESCE(EXCLUDED.zoho_notes, customers.zoho_notes),
         associated_by = COALESCE(EXCLUDED.associated_by, customers.associated_by),
         system_notes = COALESCE(EXCLUDED.system_notes, customers.system_notes),
         contact_type = COALESCE(EXCLUDED.contact_type, customers.contact_type),
         status = COALESCE(EXCLUDED.status, customers.status),
         created_by = COALESCE(EXCLUDED.created_by, customers.created_by),
         modified_by = COALESCE(EXCLUDED.modified_by, customers.modified_by),
         created_time = COALESCE(EXCLUDED.created_time, customers.created_time),
         modified_time = COALESCE(EXCLUDED.modified_time, customers.modified_time)`,
      [
        contact.contact_id,
        contact.contact_name || null,
        contact.email || null,
        addressStr,
        contact.notes || null,
        null,
        null,
        contact.contact_type || null,
        contact.status === "active",
        contact.created_by_name || null,
        contact.custom_fields?.find(cf => cf.label === "Updated By")?.value || null,
        createdTime,
        modifiedTime,
      ]
    );

    console.log(`Successfully auto-synced customer ${contact.contact_id}`);

    res.status(200).json({ status: "success", contact_id: contact.contact_id });

  } catch (err) {
    console.error("Webhook error:", err.message, err.stack?.substring(0, 300));
    // Still acknowledge to Zoho
    res.status(200).json({ status: "error_logged" });
  } finally {
    console.log("Webhook processing finished");
  }
});
router.get('/health', async (req, res) => {
  try {
    await pool.query('SELECT 1');

    const uptime = process.uptime(); // seconds since server started
    const memory = process.memoryUsage();

    res.status(200).json({
      status: 'ok',
      time: new Date().toISOString(),
      db: 'connected',
      uptimeSeconds: Math.round(uptime),
      memory: {
        rss: Math.round(memory.rss / 1024 / 1024) + ' MB', // resident set size
        heapUsed: Math.round(memory.heapUsed / 1024 / 1024) + ' MB'
      }
    });
  } catch (err) {
    console.error('Health check failed:', err.message);
    res.status(503).json({
      status: 'error',
      message: 'DB unavailable',
      error: err.message // optional – be careful in production
    });
  }
});
export default router;