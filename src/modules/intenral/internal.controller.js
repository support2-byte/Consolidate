import pool from "../../db/pool.js";
import { getBrowserLabel } from "../../services/getBrowserLabel.js";
import { getLocationFromIp } from "../../services/getLocationFromIp.js";
import logger from "../../services/logger.js";
import {
  notifySingleStatusUpdate,
  sendShipmentEmail,
} from "../../services/sendOrderEmail.js";
import { sendKycFormEmail } from "../../services/sendKycEmail.js";

const BATCH_SIZE = 20;
const INTERNAL_SECRET = process.env.EMAIL_QUEUE_SECRET;
const KYC_FORM_BASE_URL =
  process.env.KYC_FORM_BASE_URL || "https://form.royalgulfshipping.com";

export const processEmailQueue = async (req, res) => {
  const provided = req.headers["x-internal-secret"];
  if (!INTERNAL_SECRET || provided !== INTERNAL_SECRET) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  const client = await pool.connect();
  let batch;
  try {
    await client.query("BEGIN");

    const result = await client.query(
      `SELECT id, order_id, item_ref, recipient_email, recipient_name, email_type
       FROM email_queue
       WHERE status = 'pending'
       ORDER BY created_at ASC
       LIMIT $1
       FOR UPDATE SKIP LOCKED`,
      [BATCH_SIZE],
    );
    batch = result.rows;

    if (batch.length > 0) {
      await client.query(
        `UPDATE email_queue SET status = 'sending' WHERE id = ANY($1)`,
        [batch.map((r) => r.id)],
      );
    }

    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    client.release();
    console.error("[process-email-queue] Failed to claim batch:", err.message);
    return res.status(500).json({ error: "Failed to claim batch" });
  }
  client.release();

  const results = [];
  for (const row of batch) {
    try {
      let result;

      if (row.email_type === "order_created") {
        result = await sendShipmentEmail({
          email: row.recipient_email,
          orderId: row.order_id,
          itemRef: row.item_ref,
          receiverName: row.recipient_name || "Valued Customer",
        });
      } else if (row.email_type === "order_status_update") {
        if (!row.item_ref) {
          throw new Error("Missing item_ref for order_status_update queue row");
        }
        result = await notifySingleStatusUpdate({
          itemRef: row.item_ref,
          orderId: row.order_id,
          email: row.recipient_email,
          receiverName: row.recipient_name || "Valued Customer",
        });
      } else {
        throw new Error(`Unsupported email_type "${row.email_type}"`);
      }

      if (result.skipped) {
        await pool.query(
          `UPDATE email_queue SET status = 'skipped', last_error = $2 WHERE id = $1`,
          [row.id, result.message || "Skipped"],
        );
        results.push({
          id: row.id,
          status: "skipped",
          message: result.message,
        });
        continue;
      }

      if (!result.success) {
        throw new Error(result.error || result.message || "Send failed");
      }

      await pool.query(
        `UPDATE email_queue SET status = 'sent', sent_at = now() WHERE id = $1`,
        [row.id],
      );
      results.push({ id: row.id, status: "sent" });
    } catch (err) {
      await pool.query(
        `UPDATE email_queue
         SET status = 'failed', attempts = attempts + 1, last_error = $2
         WHERE id = $1`,
        [row.id, err.message],
      );
      results.push({ id: row.id, status: "failed", error: err.message });
    }
  }

  return res.status(200).json({
    success: true,
    message: "Email Sent!",
    processed: results.length,
    results,
  });
};

export const verifyRecaptcha = async (req, res) => {
  const { token } = req.body;

  if (!token || typeof token !== "string") {
    return res.status(400).json({ success: false, error: "Missing token" });
  }

  const secretKey = process.env.RECAPTCHA_SECRET_KEY;
  if (!secretKey) {
    logger.error("RECAPTCHA_SECRET_KEY is not set");
    return res
      .status(500)
      .json({ success: false, error: "Server misconfiguration" });
  }

  try {
    const params = new URLSearchParams({
      secret: secretKey,
      response: token,
    });

    const googleRes = await fetch(
      "https://www.google.com/recaptcha/api/siteverify",
      {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: params.toString(),
      },
    );

    const data = await googleRes.json();

    const minScore = 0.5;
    const passed =
      data.success && (data.score === undefined || data.score >= minScore);

    if (!passed) {
      logger.info("reCAPTCHA verification failed", {
        errorCodes: data["error-codes"],
        score: data.score,
      });
    }

    return res.json({ success: passed, score: data.score ?? null });
  } catch (error) {
    logger.error("reCAPTCHA verification error", { error: error.message });
    return res
      .status(500)
      .json({ success: false, error: "Verification failed" });
  }
};

export const getCustomerByZohoId = async (req, res) => {
  const { zohoId } = req.params;
  const { formId, company } = req.query;

  if (!zohoId?.trim()) {
    return res.status(400).json({ message: "Customer reference is required." });
  }

  if (!formId?.trim() || !company?.trim()) {
    return res
      .status(400)
      .json({ message: "formId and company are required." });
  }

  try {
    const { rows } = await pool.query(
      `SELECT zoho_id AS "zohoId",
              contact_name  AS "contactName",
              email,
              phone_number  AS "phoneNumber",
              address
         FROM customers
        WHERE zoho_id = $1 AND status = true`,
      [zohoId.trim()],
    );

    if (!rows.length) {
      logger.warn("KYC customer lookup failed", { zohoId });
      return res.status(404).json({ message: "Customer not found." });
    }

    const { rows: latestFormRows } = await pool.query(
      `SELECT status
         FROM kyc_form
        WHERE customer_ref = $1 AND form_id = $2
        ORDER BY created_at DESC
        LIMIT 1`,
      [zohoId.trim(), `KYC-${formId.trim()}`],
    );
    const latestStatus = latestFormRows[0]?.status || null;
    const alreadySubmitted =
      latestStatus === "pending" || latestStatus === "approved";

    return res.status(200).json({
      customer: rows[0],
      alreadySubmitted,
      formStatus: latestStatus,
    });

    const storedCompany = queueRows[0].company;

    if (storedCompany !== company.trim()) {
      logger.warn("KYC company mismatch", {
        zohoId,
        expected: storedCompany,
        received: company.trim(),
      });
      return res
        .status(403)
        .json({ message: "Unauthorized: company mismatch." });
    }
  } catch (err) {
    logger.error("Failed to validate KYC seed/company", {
      zohoId,
      error: err.message,
    });
    return res.status(500).json({ message: "Failed to validate link." });
  }

  try {
    const { rows } = await pool.query(
      `SELECT zoho_id AS "zohoId",
              contact_name  AS "contactName",
              email,
              phone_number  AS "phoneNumber",
              address
         FROM customers
        WHERE zoho_id = $1 AND status = true`,
      [zohoId.trim()],
    );

    if (!rows.length) {
      logger.warn("KYC customer lookup failed", { zohoId });
      return res.status(404).json({ message: "Customer not found." });
    }

    return res.status(200).json({ customer: rows[0] });
  } catch (err) {
    logger.error("Failed to fetch customer for KYC form", {
      zohoId,
      error: err.message,
    });
    return res
      .status(500)
      .json({ message: "Failed to load customer details." });
  }
};

const COMPANY_SLUG_TO_PATH = {
  RGSL: "rgsl",
  MF: "messiah-freight",
  CAS: "cas",
};

export const processKycEmailQueue = async (req, res) => {
  const provided = req.headers["x-internal-secret"];
  if (!INTERNAL_SECRET || provided !== INTERNAL_SECRET) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  const client = await pool.connect();
  let batch;
  try {
    await client.query("BEGIN");

    const result = await client.query(
      `SELECT id, customer_id, customer_email, customer_name, form_token, company
         FROM kyc_email_queue
        WHERE status = 'pending'
        ORDER BY created_at ASC
        LIMIT $1
        FOR UPDATE SKIP LOCKED`,
      [BATCH_SIZE],
    );
    batch = result.rows;

    if (batch.length > 0) {
      await client.query(
        `UPDATE kyc_email_queue SET status = 'sending' WHERE id = ANY($1)`,
        [batch.map((r) => r.id)],
      );
    }

    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    client.release();
    logger.error("[process-kyc-email-queue] Failed to claim batch", err);
    return res.status(500).json({ error: "Failed to claim batch" });
  }
  client.release();

  const results = [];

  for (const row of batch) {
    try {
      if (!row.form_token) {
        throw new Error("Missing form_token for this row");
      }

      const slugPath = COMPANY_SLUG_TO_PATH[row.company];
      if (!slugPath) {
        throw new Error(`Unknown company slug: ${row.company}`);
      }

      const formUrl = `${KYC_FORM_BASE_URL}/${slugPath}/${row.customer_id}/${row.form_token}`;

      const result = await sendKycFormEmail({
        recipientEmail: row.customer_email,
        recipientName: row.customer_name,
        formUrl,
        company: slugPath,
      });

      if (!result.success) {
        throw new Error(result.error || "Send failed");
      }

      await pool.query(
        `UPDATE kyc_email_queue SET status = 'sent', attempts = attempts + 1, sent_at = now() WHERE id = $1`,
        [row.id],
      );
      results.push({ id: row.id, status: "sent" });
    } catch (err) {
      await pool.query(
        `UPDATE kyc_email_queue
            SET status = 'failed', attempts = attempts + 1, last_error = $2
          WHERE id = $1`,
        [row.id, err.message],
      );
      results.push({ id: row.id, status: "failed", error: err.message });
    }
  }

  return res.status(200).json({
    success: true,
    message: "KYC Emails Processed!",
    processed: results.length,
    results,
  });
};

export const createKycForm = async (req, res) => {
  const {
    customerRef,
    formId,
    company,
    name,
    email,
    phone,
    address,
    emiratesId,
    passportNumber,
    tradeLicense,
  } = req.body;

  if (
    !customerRef?.trim() ||
    !formId?.trim() ||
    !name?.trim() ||
    !email?.trim() ||
    !phone?.trim() ||
    !address?.trim() ||
    !emiratesId?.trim() ||
    !passportNumber?.trim() ||
    !tradeLicense?.trim() ||
    !company?.trim()
  ) {
    logger.warn("KYC form validation failed", {
      reason: "Missing required field",
    });
    return res.status(400).json({ message: "All fields are required." });
  }

  const newFormId = `KYC-${formId}`;

  try {
    const { rows } = await pool.query(
      `INSERT INTO kyc_form
         (form_id, customer_ref, company, name, email, phone, address, emirates_id, passport_number, trade_license, created_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW())
       RETURNING id, form_id AS "formId", customer_ref AS "customerRef", company, name, email, phone, address,
         emirates_id AS "emiratesId", passport_number AS "passportNumber",
         trade_license AS "tradeLicense", status, created_at AS "createdAt"`,
      [
        newFormId,
        customerRef.trim(),
        company.trim(),
        name.trim(),
        email.trim(),
        phone.trim(),
        address.trim(),
        emiratesId.trim(),
        passportNumber.trim(),
        tradeLicense.trim(),
      ],
    );

    const newForm = rows[0];
    logger.info("KYC form created", { formId: newForm.id });

    const passportFile = req.files?.passport?.[0];
    const emiratesFile = req.files?.emiratesId?.[0];
    const signatureFile = req.files?.signature?.[0];

    if (!passportFile || !emiratesFile || !signatureFile) {
      logger.warn("KYC form submitted with missing file(s)", {
        formId: newForm.id,
        hasPassport: !!passportFile,
        hasEmiratesId: !!emiratesFile,
        hasSignature: !!signatureFile,
      });
      return res.status(400).json({
        message: "Passport, Emirates ID, and signature are all required.",
      });
    }

    await pool.query(`UPDATE kyc_form SET signature_url = $1 WHERE id = $2`, [
      signatureFile.path,
      newForm.id,
    ]);
    newForm.signatureUrl = signatureFile.path;

    const attachmentInserts = [passportFile, emiratesFile].map((file) =>
      pool.query(
        `INSERT INTO kyc_form_attachments (form_id, url, created_at) VALUES ($1,$2,NOW()) RETURNING url`,
        [newForm.id, file.path],
      ),
    );
    const attachResults = await Promise.all(attachmentInserts);
    const attachments = attachResults.map((r) => r.rows[0].url);

    logger.info("KYC form attachments uploaded", {
      formId: newForm.id,
      count: attachments.length,
    });

    try {
      const forwardedFor = req.headers["x-forwarded-for"];
      const ipAddress = forwardedFor
        ? forwardedFor.split(",")[0].trim()
        : req.ip || req.socket?.remoteAddress || "unknown";

      const location = getLocationFromIp(ipAddress);
      const ip = ipAddress.slice(0, 100);
      const rawUserAgent = req.headers["user-agent"] || "unknown";
      const browserLabel = getBrowserLabel(rawUserAgent);

      await pool.query(
        `INSERT INTO kyc_form_logs
           (form_id, ip_address, location, browser, user_agent_raw, created_at)
         VALUES ($1,$2,$3,$4,$5,NOW())`,
        [newForm.id, ip, location || "unknown", browserLabel, rawUserAgent],
      );

      logger.info("KYC form log recorded", { formId: newForm.id });
    } catch (logErr) {
      logger.error("Failed to record KYC form log", {
        formId: newForm.id,
        error: logErr.message,
      });
    }

    return res.status(201).json({
      message: "KYC form submitted successfully.",
      form: { ...newForm, attachments },
    });
  } catch (err) {
    logger.error("Failed to create KYC form", { error: err.message });
    return res
      .status(500)
      .json({ message: "Failed to save the form. Please try again." });
  }
};
