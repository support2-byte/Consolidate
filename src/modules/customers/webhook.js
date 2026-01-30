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


router.post('/zoho-customer', async (req, res) => {
  console.log("Received Zoho webhook for customer sync");

  // Optional: simple rate-limit / duplicate prevention (basic version)
  // For production: use redis/mutex or check recent logs
  // if (isWebhookProcessing) { return res.status(429).json({ error: "Processing in progress" }); }
  // let isWebhookProcessing = true;

  try {
    // Optional: secret validation (recommended)
    const secret = req.query.secret;
    if (secret !== process.env.ZOHO_WEBHOOK_SECRET) {
      console.log("Invalid webhook secret received");
      return res.status(403).json({ error: "Forbidden: Invalid secret" });
    }

    const contact = req.body; // Zoho sends single contact object for create/update

    if (!contact || !contact.contact_id) {
      console.log("Invalid webhook payload - missing contact_id");
      return res.status(200).json({ message: "Ignored: invalid payload" });
    }

    if (contact.contact_type !== 'customer') {
      console.log(`Ignored: contact_type is ${contact.contact_type}`);
      return res.status(200).json({ message: "Ignored: not a customer" });
    }

    console.log(`Processing customer: ${contact.contact_id} - ${contact.contact_name || 'no name'}`);

    // Same data preparation as in getCustomersPanel
    const addressStr = contact.billing_address ? JSON.stringify(contact.billing_address) : null;
    const createdTime = contact.created_time ? new Date(contact.created_time).toISOString() : null;
    const modifiedTime = contact.last_modified_time ? new Date(contact.last_modified_time).toISOString() : null;

    // Exact same upsert query you use
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
        null,  // associated_by
        null,  // system_notes
        contact.contact_type || null,
        contact.status === "active",
        contact.created_by_name || null,
        contact.custom_fields?.find(cf => cf.label === "Updated By")?.value || null,
        createdTime,
        modifiedTime,
      ]
    );

    console.log(`Successfully upserted customer ${contact.contact_id} from webhook`);

    // Optional: return some info (Zoho doesn't care about body, just 200)
    res.status(200).json({ 
      status: "success", 
      contact_id: contact.contact_id 
    });

  } catch (err) {
    console.error("Webhook sync error:", {
      message: err.message,
      stack: err.stack,
      payload: req.body ? JSON.stringify(req.body, null, 2) : "no body"
    });

    const status = err.response?.status || 500;
    const msg = err.response?.data?.message || err.message;

    if (status === 404) {
      return res.status(400).json({
        error: `Possible Zoho config issue - check organization_id or domain`
      });
    }

    res.status(status).json({ error: `Failed to process webhook: ${msg}` });
  } finally {
    // isWebhookProcessing = false;
    console.log("Webhook processing finished");
  }
});

export default router;