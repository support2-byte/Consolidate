import pool from "../db/pool.js";
import { sendConfirmationEmail } from "./sendConfirmationEmail.js";
import {
  notifySingleStatusUpdate,
  sendShipmentEmail,
} from "./sendOrderEmail.js";
import { sendInvoiceEmail } from "./sendInvoiceEmail.js";
import { generateOtp } from "./generateOtp.js";
import { INVOICE_LOOKUP } from "./invoiceLookup.js";
import { sendKycFormEmail } from "./sendKycEmail.js";

const FORM_BASE_URL = process.env.FORM_BASE_URL || "http://localhost:5174";
const KYC_PATH_BY_COMPANY = { RGSL: "rgsl", MF: "mf", CAS: "cas" };

const sendNotificationById = async (id) => {
  const { rows } = await pool.query("SELECT * FROM email_queue WHERE id = $1", [
    id,
  ]);
  if (!rows.length) return { success: false, error: "Notification not found" };

  const n = rows[0];
  if (!n.recipient_email)
    return { success: false, error: "No recipient email" };

  const common = {
    email: n.recipient_email,
    orderId: n.order_id,
    trackingId: n.tracking_id,
    recipientId: n.recipient_id,
    recipientType: n.recipient_type,
    receiverName: n.recipient_name || "Valued Customer",
  };

  return n.email_type === "order_created"
    ? sendShipmentEmail(common)
    : notifySingleStatusUpdate(common);
};

const sendKycById = async (id) => {
  const { rows } = await pool.query(
    "SELECT * FROM kyc_email_queue WHERE id = $1",
    [id],
  );
  if (!rows.length) return { success: false, error: "KYC entry not found" };

  const k = rows[0];
  const path = KYC_PATH_BY_COMPANY[k.company];
  if (!k.customer_email) return { success: false, error: "No customer email" };
  if (!path || !k.form_token) {
    return { success: false, error: "Missing company path or form token" };
  }

  const formUrl = `${FORM_BASE_URL}/${path}/${k.customer_id}/${k.form_token}`;

  return sendKycFormEmail({
    email: k.customer_email,
    recipientName: k.customer_name || "Valued Customer",
    company: k.company,
    formUrl,
  });
  return { success: false, error: "Wire up KYC sender in queueSenders.js" };
};

const CONFIRMATION_QUERY = `
  SELECT
    eq.id, eq.recipient_email, eq.cc_emails, eq.bcc_emails, eq.created_at,
    eq.subject, eq.message,
    bc.form_id, bc.mode, bc.total_qty, bc.total_weight,
    c.company, c.logo_url AS company_logo_url,
    p.party_type, p.name AS recipient_name, p.uuid AS party_uuid,
    p.booking_submitted_at,
    bs.name AS sender_name,
    br.name AS receiver_name,
    COALESCE(items.items, '[]'::json) AS items
  FROM confirmation_email_queue eq
  JOIN booking_confirmations bc ON bc.id = eq.order_confirmation_id
  JOIN companies c ON c.id = bc.company_id
  JOIN LATERAL (
    SELECT 'sender' AS party_type, s.name, s.uuid, s.booking_submitted_at
      FROM booking_confirmation_senders s WHERE s.email_queue_id = eq.id
    UNION ALL
    SELECT 'receiver', r.name, r.uuid, r.booking_submitted_at
      FROM booking_confirmation_receivers r WHERE r.email_queue_id = eq.id
  ) p ON true
  LEFT JOIN booking_confirmation_senders bs ON bs.booking_form_id = bc.id
  LEFT JOIN booking_confirmation_receivers br ON br.booking_form_id = bc.id
  LEFT JOIN LATERAL (
    SELECT json_agg(json_build_object(
      'category', bci.category,
      'subcategory', bci.subcategory,
      'type', bci.type,
      'qty', bci.qty,
      'weight', bci.weight,
      'placeOfLoading', bci.place_of_loading,
      'placeOfDestination', bci.place_of_destination
    ) ORDER BY bci.id) AS items
    FROM booking_confirmation_items bci
    WHERE bci.booking_form_id = bc.id
  ) items ON true
  WHERE eq.id = $1`;

export const sendConfirmationById = async (id) => {
  const { rows } = await pool.query(CONFIRMATION_QUERY, [id]);
  if (!rows.length) return { success: false, error: "Queue entry not found" };

  const entry = rows[0];
  if (entry.booking_submitted_at) {
    return {
      success: false,
      skipped: true,
      message: "This party has already submitted the form",
    };
  }

  const otp = generateOtp();
  const viewLink =
    entry.form_id && entry.party_uuid
      ? `${FORM_BASE_URL}/booking-confirmation/${entry.party_uuid}/${entry.party_type}`
      : "#";

  const result = await sendConfirmationEmail({
    recipientEmail: entry.recipient_email,
    recipientName: entry.recipient_name,
    senderName: entry.sender_name,
    receiverName: entry.receiver_name,
    partyRole: entry.party_type,
    subject: entry.subject,
    message: entry.message,
    companyName: entry.company,
    companyLogo: entry.company_logo_url,
    mode: entry.mode,
    totalQty: entry.total_qty,
    totalWeight: entry.total_weight,
    lastUpdated: entry.created_at,
    viewLink,
    ccEmails: entry.cc_emails,
    bccEmails: entry.bcc_emails,
    otp,
    items: entry.items,
  });

  if (!result.success) return result;

  const table =
    entry.party_type === "receiver"
      ? "booking_confirmation_receivers"
      : "booking_confirmation_senders";

  await pool.query(
    `UPDATE ${table} SET otp = $2, otp_verified_at = NULL WHERE email_queue_id = $1`,
    [id, otp],
  );

  return result;
};

const sendInvoiceById = async (id) => {
  const { rows } = await pool.query(
    `SELECT q.*, i.amount, i.public_ref
       FROM invoice_email_queue q
       LEFT JOIN LATERAL (${INVOICE_LOOKUP}) i ON true
      WHERE q.id = $1`,
    [id],
  );
  if (!rows.length) return { success: false, error: "Invoice email not found" };

  const n = rows[0];
  return sendInvoiceEmail({
    email: n.recipient_email,
    recipientId: n.recipient_id,
    itemRef: n.item_ref,
    receiverName: n.recipient_name || "Valued Customer",
    invoiceId: n.invoice_id,
    amount: n.amount,
    invoiceLink: `${FORM_BASE_URL}/invoice-payment/${encodeURIComponent(n.public_ref || n.invoice_id)}`,
  });
};

export const QUEUES = {
  emails: { table: "email_queue", send: sendNotificationById },
  kyc: { table: "kyc_email_queue", send: sendKycById },
  confirmation: {
    table: "confirmation_email_queue",
    send: sendConfirmationById,
  },
  invoice: { table: "invoice_email_queue", send: sendInvoiceById },
};
