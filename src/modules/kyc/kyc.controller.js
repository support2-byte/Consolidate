import crypto from "crypto";
import pool from "../../db/pool.js";
import logger from "../../services/logger.js";
import { sendKycFormEmail } from "../../services/sendKycEmail.js";

const KYC_FORM_BASE_URL = process.env.FORM_BASE_URL || "http://localhost:5174";

const generateFormSeed = () => {
  return crypto.randomInt(0, 1_000_000_000_000).toString().padStart(12, "0");
};

export const queueKycEmails = async (req, res) => {
  const { customers } = req.body;

  if (!Array.isArray(customers) || !customers.length) {
    return res.status(400).json({ message: "No customers provided." });
  }

  const VALID_COMPANIES = ["RGSL", "MF", "CAS"];

  const results = [];

  for (const customer of customers) {
    const { id, name, email, company } = customer;

    if (!id || !email || !company) {
      results.push({
        id,
        status: "skipped",
        reason: "Missing id, email, or company",
      });
      continue;
    }

    if (!VALID_COMPANIES.includes(company)) {
      results.push({
        id,
        status: "skipped",
        reason: `Unknown company: ${company}`,
      });
      continue;
    }

    try {
      const formToken = String(crypto.randomInt(0, 1_000_000_000_000)).padStart(
        12,
        "0",
      );

      await pool.query(
        `INSERT INTO kyc_email_queue
           (customer_id, customer_name, customer_email, form_token, company, status, created_at)
         VALUES ($1, $2, $3, $4, $5, 'pending', NOW())`,
        [id, name, email, formToken, company],
      );

      results.push({ id, status: "queued" });
    } catch (err) {
      logger.error("Failed to queue KYC email", { id, error: err.message });
      results.push({ id, status: "failed", reason: err.message });
    }
  }

  const queued = results.filter((r) => r.status === "queued").length;
  return res.status(200).json({ success: true, queued, results });
};

export const getKycEmailStatus = async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT customer_id,
              COUNT(*) AS send_count,
              MAX(created_at) AS last_sent_at,
              (ARRAY_AGG(status ORDER BY created_at DESC))[1] AS last_status
         FROM public.kyc_email_queue
        GROUP BY customer_id`,
    );

    const statusMap = {};
    result.rows.forEach((row) => {
      statusMap[row.customer_id] = {
        sendCount: Number(row.send_count),
        lastSentAt: row.last_sent_at,
        lastStatus: row.last_status,
      };
    });

    return res.json({ statusMap });
  } catch (error) {
    console.error("getKycEmailStatus error:", error);
    return res.status(500).json({ error: "Failed to fetch KYC email status" });
  }
};

export const getAllKycEmails = async (req, res) => {
  try {
    const { rows } = await pool.query(
      `SELECT id, customer_id, customer_email, customer_name, email_type, company,
              status, attempts, last_error, form_token, created_at, sent_at
         FROM public.kyc_email_queue
        ORDER BY created_at DESC`,
    );

    if (rows.length === 0) {
      logger.warn("No KYC email notifications found!");
      return res
        .status(404)
        .json({ success: false, message: "No KYC email notifications found!" });
    }

    return res.status(200).json({ success: true, notifications: rows });
  } catch (error) {
    logger.error("Failed to fetch KYC email notifications", error);
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
};

export const deleteKycEmail = async (req, res) => {
  const { id } = req.params;

  try {
    const result = await pool.query(
      `DELETE FROM public.kyc_email_queue WHERE id = $1 RETURNING id`,
      [id],
    );

    if (!result.rowCount) {
      return res
        .status(404)
        .json({ success: false, message: "KYC email record not found." });
    }

    return res
      .status(200)
      .json({ success: true, message: "KYC email record deleted." });
  } catch (error) {
    logger.error("Failed to delete KYC email record", error);
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong!" });
  }
};

export const getCustomerKycProfile = async (req, res) => {
  const { customerId } = req.params;

  try {
    const formsResult = await pool.query(
      `SELECT
         f.id, f.form_id, f.customer_ref, f.company, f.name, f.email, f.phone, f.address,
         f.emirates_id, f.passport_number, f.trade_license, f.created_at,
         f.status, f.remarks, f.decision_by, f.signature_url
       FROM kyc_form f
       WHERE f.customer_ref = $1
       ORDER BY f.created_at ASC`,
      [customerId],
    );

    const forms = formsResult.rows;

    if (!forms.length) {
      return res.status(404).json({
        success: false,
        message: "No KYC submissions found for this customer",
      });
    }

    const customerResult = await pool.query(
      `SELECT zoho_id, contact_name, email, phone_number, address, status,
              contact_type, type, created_time, modified_time
       FROM customers
       WHERE zoho_id = $1`,
      [customerId],
    );

    const customer = customerResult.rows[0] || null;

    const formIds = forms.map((f) => f.id);

    const [logsResult, attachmentsResult] = await Promise.all([
      pool.query(
        `SELECT id, form_id, ip_address, location, browser, user_agent_raw, created_at
         FROM kyc_form_logs
         WHERE form_id = ANY($1::int[])
         ORDER BY created_at ASC`,
        [formIds],
      ),
      pool.query(
        `SELECT id, form_id, url, created_at
         FROM kyc_form_attachments
         WHERE form_id = ANY($1::int[])
         ORDER BY created_at ASC`,
        [formIds],
      ),
    ]);

    const logsByForm = {};
    logsResult.rows.forEach((log) => {
      if (!logsByForm[log.form_id]) logsByForm[log.form_id] = [];
      logsByForm[log.form_id].push(log);
    });

    const attachmentsByForm = {};
    attachmentsResult.rows.forEach((att) => {
      if (!attachmentsByForm[att.form_id]) attachmentsByForm[att.form_id] = [];
      attachmentsByForm[att.form_id].push(att);
    });

    const submissions = forms.map((form) => ({
      ...form,
      logs: logsByForm[form.id] || [],
      attachments: attachmentsByForm[form.id] || [],
    }));

    return res.json({ success: true, customer, submissions });
  } catch (err) {
    console.error("getCustomerKycProfile error:", err);
    return res.status(500).json({
      success: false,
      message: "Failed to fetch customer KYC profile",
    });
  }
};

export const getAllKycLogs = async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT
         l.id, l.form_id, l.ip_address, l.location, l.browser,
         l.user_agent_raw, l.created_at,
         f.customer_ref, f.name AS submitted_name, f.email AS submitted_email
       FROM kyc_form_logs l
       JOIN kyc_form f ON f.id = l.form_id
       ORDER BY l.created_at DESC`,
    );

    return res.json({ success: true, logs: result.rows });
  } catch (err) {
    console.error("getAllKycLogs error:", err);
    return res.status(500).json({
      success: false,
      message: "Failed to fetch KYC logs",
    });
  }
};

export const getAllKycSubmissions = async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT * FROM kyc_form ORDER BY created_at DESC`,
    );

    return res.json({ success: true, submissions: result.rows });
  } catch (err) {
    console.error("getAllKycSubmissions error:", err);
    return res.status(500).json({
      success: false,
      message: "Failed to fetch KYC submissions",
    });
  }
};

export const getKycSubmissionStats = async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT status, COUNT(*)::int AS count
       FROM kyc_form
       GROUP BY status`,
    );

    const stats = { total: 0, pending: 0, approved: 0, rejected: 0 };

    result.rows.forEach((row) => {
      stats.total += row.count;
      if (row.status === "approved") stats.approved = row.count;
      else if (row.status === "rejected") stats.rejected = row.count;
      else if (row.status === "pending") stats.pending = row.count;
    });

    return res.json({ success: true, stats });
  } catch (err) {
    console.error("getKycSubmissionStats error:", err);
    return res.status(500).json({
      success: false,
      message: "Failed to fetch KYC submission stats",
    });
  }
};

export const updateKycSubmission = async (req, res) => {
  const { formId } = req.params;
  const { status, remarks } = req.body;

  if (!["approved", "rejected", "pending"].includes(status)) {
    return res.status(400).json({ success: false, message: "Invalid status" });
  }

  try {
    const result = await pool.query(
      `UPDATE kyc_form
       SET status = $1, remarks = $2
       WHERE id = $3
       RETURNING id, status, remarks`,
      [status, remarks || null, formId],
    );

    if (!result.rows.length) {
      return res
        .status(404)
        .json({ success: false, message: "Submission not found" });
    }

    return res.json({ success: true, submission: result.rows[0] });
  } catch (err) {
    console.error("updateKycSubmission error:", err);
    return res.status(500).json({
      success: false,
      message: "Failed to update submission",
    });
  }
};

export const getKycCustomerStatusMap = async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT DISTINCT ON (customer_ref) customer_ref, status, created_at
       FROM kyc_form
       ORDER BY customer_ref, created_at DESC`,
    );

    const statusMap = {};
    result.rows.forEach((row) => {
      statusMap[row.customer_ref] = row.status;
    });

    return res.json({ success: true, statusMap });
  } catch (err) {
    console.error("getKycCustomerStatusMap error:", err);
    return res.status(500).json({
      success: false,
      message: "Failed to fetch KYC customer status map",
    });
  }
};

export const resendKycEmail = async (req, res) => {
  const { id } = req.params;

  try {
    const { rows } = await pool.query(
      `SELECT id, customer_id, customer_email, customer_name, form_token, company
         FROM kyc_email_queue
        WHERE id = $1`,
      [id],
    );

    if (!rows.length) {
      return res
        .status(404)
        .json({ success: false, message: "KYC email record not found." });
    }

    const row = rows[0];

    let newToken = generateFormSeed();
    let attempt = 0;
    while (attempt < 5) {
      try {
        await pool.query(
          `UPDATE kyc_email_queue SET form_token = $1 WHERE id = $2`,
          [newToken, id],
        );
        break;
      } catch (err) {
        if (err.code === "23505" && attempt < 4) {
          newToken = generateFormSeed();
          attempt += 1;
          continue;
        }
        throw err;
      }
    }

    const slugPath = COMPANY_SLUG_TO_PATH[row.company];
    if (!slugPath) {
      throw new Error(`Unknown company slug: ${row.company}`);
    }

    const formUrl = `${KYC_FORM_BASE_URL}/${slugPath}/${row.customer_id}/${newToken}`;

    const result = await sendKycFormEmail({
      recipientEmail: row.customer_email,
      recipientName: row.customer_name,
      formUrl,
      company: row.company,
    });

    if (!result.success) {
      throw new Error(result.error || "Send failed");
    }

    const updated = await pool.query(
      `UPDATE kyc_email_queue
          SET status = 'sent',
              attempts = attempts + 1,
              sent_at = now(),
              last_error = NULL
        WHERE id = $1
        RETURNING id, status, attempts, sent_at, form_token`,
      [id],
    );

    return res.status(200).json({
      success: true,
      message: "KYC email sent",
      row: updated.rows[0],
    });
  } catch (error) {
    logger.error("Failed to resend KYC email", error);

    await pool
      .query(
        `UPDATE public.kyc_email_queue
            SET status = 'failed', attempts = attempts + 1, last_error = $2
          WHERE id = $1`,
        [id, error.message],
      )
      .catch(() => {});

    return res
      .status(500)
      .json({ success: false, message: "Failed to resend KYC email." });
  }
};

export const updateCustomerFromKyc = async (req, res) => {
  const { id } = req.params;

  try {
    const subResult = await pool.query(
      `SELECT id, customer_ref, name, email, phone, address, status
       FROM kyc_form WHERE id = $1`,
      [id],
    );

    if (subResult.rows.length === 0) {
      return res
        .status(404)
        .json({ success: false, message: "Submission not found" });
    }

    const submission = subResult.rows[0];

    if (submission.status !== "approved") {
      return res.status(400).json({
        success: false,
        message: "Only approved submissions can be used to update the customer",
      });
    }

    const updateResult = await pool.query(
      `UPDATE customers
       SET contact_name = $1,
           email = $2,
           phone_number = $3,
           address = $4,
           modified_time = NOW(),
           modified_by = $5
       WHERE zoho_id = $6
       RETURNING *`,
      [
        submission.name,
        submission.email,
        submission.phone,
        submission.address,
        req.user.email,
        submission.customer_ref,
      ],
    );

    if (updateResult.rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: `No customer found with matching ID`,
      });
    }

    return res.json({
      success: true,
      message: "Customer Updated Successfully",
      customer: updateResult.rows[0],
    });
  } catch (err) {
    logger.error("Failed to update customer with Approved KYC", { err });
    return res.status(500).json({
      success: false,
      message: err.message || "Failed to update customer",
    });
  }
};
