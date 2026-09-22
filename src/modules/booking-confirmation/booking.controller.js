import crypto from "crypto";
import pool from "../../db/pool.js";
import logger from "../../services/logger.js";
import { withTransaction } from "../../services/transaction.js";
import { generateAndUploadBookingSnapshot } from "../../utils/bookingDocSnapshot.js";
import { generateBookingConfirmationHtml } from "../../documents/bookingConfirmationDoc.js";
import { fetchOrderAndCompany } from "../../services/fetchOrderAndCompany.js";
import { generateOtp } from "../../services/generateOtp.js";

const PARTY_TABLES = {
  sender: "booking_confirmation_senders",
  receiver: "booking_confirmation_receivers",
};

const generateFormId = () => `CONFIRMATION-${crypto.randomInt(100000, 999999)}`;

const insertBookingConfirmation = async (client, params) => {
  let attempt = 0;
  while (attempt < 5) {
    const formId = generateFormId();
    try {
      const {
        rows: [confirmation],
      } = await client.query(
        `INSERT INTO booking_confirmations
           (company_id, mode, subject, message, total_qty, total_weight, created_by, form_id)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
         RETURNING id, form_id`,
        [...params, formId],
      );
      return confirmation;
    } catch (err) {
      if (err.code === "23505" && attempt < 4) {
        attempt += 1;
        continue;
      }
      throw err;
    }
  }
  throw new Error("Failed to generate a unique form_id after 5 attempts");
};

export const saveConfirmationData = async (req, res) => {
  const { companyId, subject, message, mode, sender, items, participants } =
    req.body;

  if (!companyId) {
    return res.status(400).json({ message: "Company is required" });
  }
  if (!sender?.name || !sender?.email || !sender?.phone || !sender?.address) {
    return res.status(400).json({
      message: "Sender name, email, phone, and address are required",
    });
  }
  if (!subject || !message || !mode) {
    return res
      .status(400)
      .json({ message: "Subject, message, and mode are required" });
  }
  if (!Array.isArray(participants) || !participants.length) {
    return res.status(400).json({ message: "Add at least one recipient" });
  }

  const invalid = participants.find(
    (p) => !p.name || !p.email || !p.phone || !p.address,
  );
  if (invalid) {
    return res.status(400).json({
      message: `Missing name, email, phone, or address for ${invalid.email || "a participant"}`,
    });
  }

  try {
    const result = await withTransaction(async (client) => {
      const totalQty = (items || []).reduce(
        (sum, i) => sum + (Number(i.qty) || 0),
        0,
      );
      const totalWeight = (items || []).reduce(
        (sum, i) => sum + (Number(i.weight) || 0),
        0,
      );

      const toEntries = participants.filter((p) => p.role === "to");
      const ccEntries = participants.filter((p) => p.role === "cc");
      const bccEntries = participants.filter((p) => p.role === "bcc");

      if (!toEntries.length) {
        return res
          .status(400)
          .json({ message: "At least one 'To' recipient is required" });
      }

      const confirmation = await insertBookingConfirmation(client, [
        companyId,
        mode,
        subject,
        message,
        totalQty,
        totalWeight,
        req.user.id,
      ]);

      const {
        rows: [senderRow],
      } = await client.query(
        `INSERT INTO booking_confirmation_senders
          (uuid, booking_form_id, name, email, phone, address, otp)
        VALUES ($1,$2,$3,$4,$5,$6,$7)
        RETURNING id, otp`,
        [
          crypto.randomUUID(),
          confirmation.id,
          sender.name,
          sender.email,
          sender.phone,
          sender.address,
          generateOtp(),
        ],
      );

      const {
        rows: [senderQueuedEmail],
      } = await client.query(
        `INSERT INTO confirmation_email_queue
            (recipient_email, cc_emails, bcc_emails, subject, message, status, order_confirmation_id, created_at)
          VALUES ($1,$2,$3,$4,$5,'pending',$6, now())
          RETURNING id`,
        [
          sender.email,
          ccEntries.length ? ccEntries.map((p) => p.email).join(", ") : null,
          bccEntries.length ? bccEntries.map((p) => p.email).join(", ") : null,
          subject,
          message,
          confirmation.id,
        ],
      );

      await client.query(
        `UPDATE booking_confirmation_senders SET email_queue_id = $1 WHERE id = $2`,
        [senderQueuedEmail.id, senderRow.id],
      );

      for (const item of items || []) {
        await client.query(
          `INSERT INTO booking_confirmation_items
              (booking_form_id, category, subcategory, type, qty, weight,
              place_of_loading, place_of_destination)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
          [
            confirmation.id,
            item.category,
            item.subcategory,
            item.type,
            item.qty,
            item.weight,
            item.portOfLoading,
            item.portOfDestination,
          ],
        );
      }

      const primaryReceiver = toEntries[0];

      const {
        rows: [receiverRow],
      } = await client.query(
        `INSERT INTO booking_confirmation_receivers
          (uuid, booking_form_id, name, email, phone, address, company_name, role, otp)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        RETURNING id, otp`,
        [
          crypto.randomUUID(),
          confirmation.id,
          primaryReceiver.name,
          primaryReceiver.email,
          primaryReceiver.phone,
          primaryReceiver.address,
          primaryReceiver.company || "",
          "to",
          generateOtp(),
        ],
      );

      const {
        rows: [queuedEmail],
      } = await client.query(
        `INSERT INTO confirmation_email_queue
     (recipient_email, cc_emails, bcc_emails, subject, message, status, order_confirmation_id, created_at)
   VALUES ($1,$2,$3,$4,$5,'pending',$6, now())
   RETURNING id`,
        [
          toEntries.map((p) => p.email).join(", "),
          ccEntries.length ? ccEntries.map((p) => p.email).join(", ") : null,
          bccEntries.length ? bccEntries.map((p) => p.email).join(", ") : null,
          subject,
          message,
          confirmation.id,
        ],
      );

      await client.query(
        `UPDATE booking_confirmation_receivers SET email_queue_id = $1 WHERE id = $2`,
        [queuedEmail.id, receiverRow.id],
      );

      return { formId: confirmation.form_id, queued: 2 };
    });

    return res.status(201).json({
      success: true,
      formId: result.formId,
      queued: result.queued,
      total: result.queued,
    });
  } catch (err) {
    logger.error("confirmation-email-queue failed", err);
    return res
      .status(500)
      .json({ message: "Failed to save and queue booking confirmation" });
  }
};

export const getBookingOrder = async (req, res) => {
  const { formId: uuid, type } = req.params;

  const table = PARTY_TABLES[type];
  if (!table) {
    return res.status(400).json({ message: "Invalid participant type." });
  }

  try {
    const { rows } = await pool.query(
      `SELECT
         bc.id AS booking_form_id,
         bc.form_id,
         bc.mode,
         c.company,
         c.logo_url,
         c.primary_color,
         c.secondary_color,
         c.address AS company_address,
         c.phone AS company_phone,
         c.email AS company_email,
         p.id AS party_id,
         p.uuid AS party_uuid,
         p.name AS party_name,
         p.email AS party_email,
         p.phone AS party_phone,
         p.address AS party_address,
         p.otp,
         p.otp_verified_at,
         p.booking_submitted_at
       FROM booking_confirmations bc
       JOIN companies c ON c.id = bc.company_id
       JOIN ${table} p ON p.booking_form_id = bc.id
       WHERE p.uuid = $1`,
      [uuid],
    );

    if (!rows.length) {
      return res.status(404).json({ message: "Booking order not found." });
    }

    const row = rows[0];

    const { rows: itemRows } = await pool.query(
      `SELECT qty, weight, category, subcategory, type,
              place_of_loading AS "portOfLoading",
              place_of_destination AS "portOfDestination"
       FROM booking_confirmation_items
       WHERE booking_form_id = $1
       ORDER BY id`,
      [row.booking_form_id],
    );

    const { rows: senderRows } = await pool.query(
      `SELECT name, address, phone, email
       FROM booking_confirmation_senders
       WHERE booking_form_id = $1`,
      [row.booking_form_id],
    );

    const { rows: receiverRows } = await pool.query(
      `SELECT name, address, phone, email, company_name
       FROM booking_confirmation_receivers
       WHERE booking_form_id = $1`,
      [row.booking_form_id],
    );

    const alreadySubmitted = Boolean(row.booking_submitted_at);

    const senderInfo = {
      senderName: senderRows[0]?.name || "",
      senderAddress: senderRows[0]?.address || "",
      senderContact: senderRows[0]?.phone || "",
      senderEmail: senderRows[0]?.email || "",
    };

    const receiverInfo = {
      receiverName: receiverRows[0]?.name || "",
      receiverAddress: receiverRows[0]?.address || "",
      receiverContact: receiverRows[0]?.phone || "",
      receiverEmail: receiverRows[0]?.email || "",
      receiverCompany: receiverRows[0]?.company_name || "",
    };

    return res.json({
      success: true,
      alreadySubmitted,
      participantRole: type,
      participantName: row.party_name,
      company: {
        company: row.company,
        logoUrl: row.logo_url,
        primaryColor: row.primary_color,
        secondaryColor: row.secondary_color,
        address: row.company_address,
        phone: row.company_phone,
        email: row.company_email,
      },
      order: {
        mode: row.mode,
        ...senderInfo,
        ...receiverInfo,
        items: itemRows,
      },
    });
  } catch (err) {
    logger.error("getBookingOrder failed", err);
    return res.status(500).json({ message: "Failed to load booking order." });
  }
};

export const verifyBookingOtp = async (req, res) => {
  const { formId: uuid, type, otp } = req.body;

  const table = PARTY_TABLES[type];
  if (!uuid || !table || !otp) {
    return res.status(400).json({
      verified: false,
      message: "Missing formId, type, or otp.",
    });
  }

  try {
    const { rows } = await pool.query(
      `SELECT id, otp, booking_submitted_at FROM ${table} WHERE uuid = $1`,
      [uuid],
    );

    if (!rows.length) {
      return res
        .status(404)
        .json({ verified: false, message: "Booking order not found." });
    }

    const party = rows[0];

    if (party.booking_submitted_at) {
      return res.status(400).json({
        verified: false,
        message: "This booking confirmation has already been submitted.",
      });
    }

    if (!party.otp) {
      return res
        .status(400)
        .json({ verified: false, message: "No OTP has been issued yet." });
    }

    if (String(otp).trim() !== String(party.otp).trim()) {
      return res.status(400).json({ verified: false, message: "Invalid OTP." });
    }

    await pool.query(
      `UPDATE ${table} SET otp_verified_at = NOW() WHERE id = $1`,
      [party.id],
    );

    return res.json({ verified: true });
  } catch (err) {
    logger.error("verifyBookingOtp failed", err);
    return res
      .status(500)
      .json({ verified: false, message: "Failed to verify OTP." });
  }
};

export const submitBookingConfirmation = async (req, res) => {
  const {
    formId: uuid,
    type,
    passportNumber,
    emiratesId,
    tradeLicenseNumber,
    expectedDate,
  } = req.body;

  const table = PARTY_TABLES[type];
  if (!uuid || !table) {
    return res.status(400).json({ message: "Missing formId or type." });
  }

  if (!passportNumber || !expectedDate) {
    return res.status(400).json({
      message: "Passport number and expected date are required.",
    });
  }

  try {
    const { rows } = await pool.query(
      `SELECT id, booking_form_id, otp_verified_at, booking_submitted_at
       FROM ${table}
       WHERE uuid = $1`,
      [uuid],
    );

    if (!rows.length) {
      return res.status(404).json({ message: "Booking order not found." });
    }

    const party = rows[0];

    if (party.booking_submitted_at) {
      return res.status(400).json({
        message: "This booking confirmation has already been submitted.",
      });
    }

    if (!party.otp_verified_at) {
      return res
        .status(403)
        .json({ message: "OTP has not been verified for this session." });
    }

    const passportDoc = req.files?.passportDocument?.[0];
    const emiratesDoc = req.files?.emiratesDocument?.[0];
    const tradeLicenseDoc = req.files?.tradeLicenseDocument?.[0];
    const signature = req.files?.signature?.[0];

    if (!passportDoc || !signature) {
      return res.status(400).json({
        message: "Passport document and signature are required.",
      });
    }

    const passportDocUrl = passportDoc.path;
    const emiratesDocUrl = emiratesDoc?.path || null;
    const tradeLicenseDocUrl = tradeLicenseDoc?.path || null;
    const signatureUrl = signature.path;

    const submissionId = await withTransaction(async (client) => {
      const {
        rows: [submission],
      } = await client.query(
        `INSERT INTO booking_confirmation_submissions
           (form_submitter_id, passport_number, emirates_id,
            trade_license_number, expected_date, signature_url, participant_type)
         VALUES ($1,$2,$3,$4,$5,$6,$7)
         RETURNING id`,
        [
          party.id,
          passportNumber,
          emiratesId,
          tradeLicenseNumber,
          expectedDate,
          signatureUrl,
          type,
        ],
      );

      await client.query(
        `INSERT INTO booking_confirmation_attachment
           (submission_id, passport_doc_url, emirates_doc_url, trade_license_doc_url)
         VALUES ($1,$2,$3,$4)`,
        [submission.id, passportDocUrl, emiratesDocUrl, tradeLicenseDocUrl],
      );

      await client.query(
        `UPDATE ${table} SET booking_submitted_at = NOW() WHERE id = $1`,
        [party.id],
      );

      return submission.id;
    });

    try {
      const { order, company, formId } = await fetchOrderAndCompany(
        party.booking_form_id,
      );

      const html = generateBookingConfirmationHtml(order, company, {
        passportNumber,
        emiratesId,
        tradeLicenseNumber,
        expectedDate,
        signatureDataUrl: signatureUrl,
        participantRole: type,
      });

      const formUrl = await generateAndUploadBookingSnapshot(
        html,
        `${formId}_${submissionId}`,
      );

      await pool.query(
        `UPDATE booking_confirmation_submissions SET form_url = $1 WHERE id = $2`,
        [formUrl, submissionId],
      );
    } catch (snapshotErr) {
      logger.error("Booking confirmation snapshot generation failed", {
        submissionId,
        bookingFormId: party.booking_form_id,
        message: snapshotErr?.message,
        stack: snapshotErr?.stack,
        name: snapshotErr?.name,
      });
    }

    return res.json({ success: true, submissionId });
  } catch (err) {
    logger.error("submitBookingConfirmation failed", err);
    return res
      .status(500)
      .json({ message: "Failed to submit booking confirmation." });
  }
};

export const listBookingConfirmations = async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT
        bc.id,
        bc.mode,
        bc.subject,
        bc.total_qty,
        bc.total_weight,
        bc.created_at,
        bc.form_id,
        bc.created_by,
        json_build_object(
          'id', c.id,
          'company', c.company,
          'address', c.address,
          'phone', c.phone,
          'email', c.email,
          'logo_url', c.logo_url,
          'primary_color', c.primary_color,
          'secondary_color', c.secondary_color
        ) AS company,
        COALESCE(senders.rows, '[]') AS senders,
        COALESCE(receivers.rows, '[]') AS receivers,
        COALESCE(items.rows, '[]') AS items
      FROM booking_confirmations bc
      JOIN companies c ON c.id = bc.company_id
      LEFT JOIN LATERAL (
        SELECT json_agg(json_build_object(
          'id', s.id, 'uuid', s.uuid, 'name', s.name, 'email', s.email,
          'phone', s.phone, 'address', s.address,
          'booking_submitted_at', s.booking_submitted_at
        )) AS rows
        FROM booking_confirmation_senders s
        WHERE s.booking_form_id = bc.id
      ) senders ON true
      LEFT JOIN LATERAL (
        SELECT json_agg(json_build_object(
          'id', r.id, 'uuid', r.uuid, 'name', r.name, 'email', r.email,
          'phone', r.phone, 'address', r.address, 'company_name', r.company_name,
          'role', r.role, 'booking_submitted_at', r.booking_submitted_at
        )) AS rows
        FROM booking_confirmation_receivers r
        WHERE r.booking_form_id = bc.id
      ) receivers ON true
      LEFT JOIN LATERAL (
        SELECT json_agg(json_build_object(
          'category', i.category, 'subcategory', i.subcategory, 'type', i.type,
          'qty', i.qty, 'weight', i.weight,
          'portOfLoading', i.place_of_loading, 'portOfDestination', i.place_of_destination
        )) AS rows
        FROM booking_confirmation_items i
        WHERE i.booking_form_id = bc.id
      ) items ON true
      ORDER BY bc.created_at DESC
    `);
    return res.json(rows);
  } catch (err) {
    console.error("listBookingConfirmations error:", err);
    return res
      .status(500)
      .json({ error: "Failed to fetch booking confirmations" });
  }
};

export const getBookingConfirmationById = async (req, res) => {
  const { id } = req.params;
  try {
    const { rows } = await pool.query(
      `SELECT bc.*, row_to_json(c.*) AS company
       FROM booking_confirmations bc
       JOIN companies c ON c.id = bc.company_id
       WHERE bc.id = $1`,
      [id],
    );
    if (!rows.length) return res.status(404).json({ error: "Not found" });
    const booking = rows[0];

    const [sendersRes, receiversRes, itemsRes] = await Promise.all([
      pool.query(
        `SELECT * FROM booking_confirmation_senders WHERE booking_form_id = $1 ORDER BY id`,
        [id],
      ),
      pool.query(
        `SELECT * FROM booking_confirmation_receivers WHERE booking_form_id = $1 ORDER BY id`,
        [id],
      ),
      pool.query(
        `SELECT category, subcategory, type, qty, weight,
                place_of_loading AS "portOfLoading",
                place_of_destination AS "portOfDestination"
         FROM booking_confirmation_items WHERE booking_form_id = $1 ORDER BY id`,
        [id],
      ),
    ]);

    const sender = sendersRes.rows[0] || {};

    const orderData = {
      sender_name: sender.name,
      sender_contact: sender.phone,
      sender_email: sender.email,
      sender_address: sender.address,
      mode: booking.mode,
      items: itemsRes.rows,
      receivers: receiversRes.rows.map((r) => ({
        receiverName: r.name,
        receiverContact: r.phone,
        receiverAddress: r.address,
        receiverEmail: r.email,
        receiverCompany: r.company_name,
      })),
    };

    return res.json({
      booking,
      orderData,
      company: booking.company,
      receivers: receiversRes.rows,
    });
  } catch (err) {
    console.error("getBookingConfirmationById error:", err);
    return res
      .status(500)
      .json({ error: "Failed to fetch booking confirmation" });
  }
};

export const listSubmissions = async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT
        sub.id AS submission_id,
        sub.participant_type,
        sub.form_submitter_id,
        sub.passport_number,
        sub.emirates_id,
        sub.trade_license_number,
        sub.expected_date,
        sub.signature_url,
        sub.submitted_at,
        bc.id AS booking_form_id,
        bc.form_id,
        bc.mode,
        bc.subject,
        c.company AS company_name,
        COALESCE(
          CASE sub.participant_type
            WHEN 'sender' THEN (SELECT s.name FROM booking_confirmation_senders s WHERE s.id = sub.form_submitter_id)
            WHEN 'receiver' THEN (SELECT r.name FROM booking_confirmation_receivers r WHERE r.id = sub.form_submitter_id)
          END, ''
        ) AS submitter_name,
        att.passport_doc_url,
        att.emirates_doc_url,
        att.trade_license_doc_url
      FROM booking_confirmation_submissions sub
      LEFT JOIN booking_confirmation_attachment att ON att.submission_id = sub.id
      -- form_submitter_id is polymorphic; resolve the booking_form_id via a lateral union
      JOIN LATERAL (
        SELECT booking_form_id FROM booking_confirmation_senders WHERE id = sub.form_submitter_id AND sub.participant_type = 'sender'
        UNION ALL
        SELECT booking_form_id FROM booking_confirmation_receivers WHERE id = sub.form_submitter_id AND sub.participant_type = 'receiver'
      ) parent ON true
      JOIN booking_confirmations bc ON bc.id = parent.booking_form_id
      JOIN companies c ON c.id = bc.company_id
      ORDER BY sub.submitted_at DESC
    `);
    return res.json(rows);
  } catch (err) {
    console.error("listSubmissions error:", err);
    return res.status(500).json({ error: "Failed to fetch submissions" });
  }
};

export const getSubmissionDetail = async (req, res) => {
  const { submissionId } = req.params;
  try {
    const subRes = await pool.query(
      `SELECT * FROM booking_confirmation_submissions WHERE id = $1`,
      [submissionId],
    );
    if (!subRes.rows.length)
      return res.status(404).json({ error: "Not found" });
    const sub = subRes.rows[0];

    const attRes = await pool.query(
      `SELECT * FROM booking_confirmation_attachment WHERE submission_id = $1`,
      [submissionId],
    );

    const parentTable =
      sub.participant_type === "sender"
        ? "booking_confirmation_senders"
        : "booking_confirmation_receivers";

    const parentRes = await pool.query(
      `SELECT * FROM ${parentTable} WHERE id = $1`,
      [sub.form_submitter_id],
    );
    if (!parentRes.rows.length)
      return res.status(404).json({ error: "Submitter not found" });
    const parent = parentRes.rows[0];

    // no more bc.form_url here — booking row doesn't have that column
    const bookingRes = await pool.query(
      `SELECT bc.mode, row_to_json(c.*) AS company
       FROM booking_confirmations bc
       JOIN companies c ON c.id = bc.company_id
       WHERE bc.id = $1`,
      [parent.booking_form_id],
    );
    const booking = bookingRes.rows[0];

    return res.json({
      formUrl: sub.form_url || null, // ← comes from the submission row now
      order: {
        mode: booking.mode,
      },
      company: {
        company: booking.company.company,
        address: booking.company.address,
        phone: booking.company.phone,
        email: booking.company.email,
        logoUrl: booking.company.logo_url,
        primaryColor: booking.company.primary_color,
      },
      extras: {
        passportNumber: sub.passport_number,
        emiratesId: sub.emirates_id,
        tradeLicenseNumber: sub.trade_license_number,
        expectedDate: sub.expected_date,
        signatureDataUrl: sub.signature_url,
        participantRole: sub.participant_type,
      },
      attachment: attRes.rows[0] || null,
    });
  } catch (err) {
    console.error("getSubmissionDetail error:", err);
    return res.status(500).json({ error: "Failed to fetch submission detail" });
  }
};
