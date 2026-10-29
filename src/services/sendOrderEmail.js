import pool from "../db/pool.js";
import { transporter } from "../middleware/nodeMailer.js";
import { escapeHtml } from "./escapeHtml.js";
import logger from "./logger.js";
import { renderTemplate } from "./renderTemplate.js";

const KARACHI_TIMEZONE = "Asia/Karachi";
const FRONTEND_URL = process.env.FORM_BASE_URL || "http://localhost:5174";
const TRACK_BASE =
  process.env.TRACK_BASE_URL || "https://trackorder.royalgulfshipping.com";

function formatDate(date) {
  if (!date) return "—";
  return (
    new Intl.DateTimeFormat("en-GB", {
      timeZone: KARACHI_TIMEZONE,
      day: "2-digit",
      month: "2-digit",
      year: "numeric",
    }).format(new Date(date)) + " (GMT+5)"
  );
}

function formatDateTime(date) {
  if (!date) return "—";
  return (
    new Intl.DateTimeFormat("en-GB", {
      timeZone: KARACHI_TIMEZONE,
      day: "2-digit",
      month: "2-digit",
      year: "numeric",
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
    }).format(new Date(date)) + " (GMT+5)"
  );
}

function buildTrackLink(trackingId) {
  if (!trackingId) return `${TRACK_BASE}/`;
  return `${TRACK_BASE}/?ref=${encodeURIComponent(trackingId)}`;
}

const STATUS_ACTION_LINKS = {
  "Order Created": {
    recipientType: "sender",
    heading: "Want to schedule your drop-off?",
    links: [
      {
        path: "drop-off-request",
        text: "Schedule Drop-off",
        description:
          "Choose a pickup date and zone so we can collect your shipment from your address.",
      },
    ],
  },
  "Shipment Delivered": {
    recipientType: "receiver",
    heading: "Your shipment has arrived. What would you like to do next?",
    links: [
      {
        path: "purchase-storage",
        text: "Arrange Storage",
        description:
          "Need more time before collecting? Request storage space for your goods.",
      },
      {
        path: "request-delivery",
        text: "Request Delivery",
        description:
          "Want it brought to you? Request delivery to your address.",
      },
    ],
  },
};

function buildActionLinksHtml(statusLabel, trackingId, recipientType) {
  const config = STATUS_ACTION_LINKS[statusLabel];
  if (
    !config ||
    !trackingId ||
    config.recipientType !== String(recipientType || "").toLowerCase()
  ) {
    return "";
  }

  const width = Math.floor(100 / config.links.length);
  const cells = config.links
    .map(
      (link) =>
        `<td style="vertical-align:top;width:${width}%;padding:0 10px 0 0"><p style="margin:0 0 6px;font-size:13px;color:#475569">${escapeHtml(link.description)}</p><a class="cta" style="margin:0" href="${FRONTEND_URL}/${link.path}/${encodeURIComponent(trackingId)}" target="_blank" rel="noopener noreferrer">${escapeHtml(link.text)}</a></td>`,
    )
    .join("");

  return `<div style="margin-top:18px"><p style="margin:0;font-weight:700;color:#0f172a">${escapeHtml(config.heading)}</p><table role="presentation" style="width:100%;margin-top:14px;border-collapse:collapse"><tr>${cells}</tr></table></div>`;
}

function buildSubject(templateData) {
  return `Royal Gulf Shipping – ${templateData.statusLabel} (Tracking ID: ${templateData.refId || "—"})`;
}

function buildSubscriptionConfirmationHtml(templateData) {
  const route = `${escapeHtml(templateData.place_of_loading || "—")} &rarr; ${escapeHtml(templateData.place_of_delivery || "—")}`;
  return renderTemplate("subscription_confirmed.html", {
    refId: escapeHtml(templateData.refId || "—"),
    orderId: escapeHtml(templateData.orderId || "—"),
    route,
    eta: escapeHtml(templateData.eta || "—"),
    lastUpdated: escapeHtml(
      templateData.lastUpdated || formatDateTime(new Date()),
    ),
    trackLink: escapeHtml(templateData.trackLink || `${TRACK_BASE}/`),
    currentYear: new Date().getFullYear(),
  });
}

function buildOrderCreatedHtml(templateData) {
  return renderTemplate("order_created.html", {
    statusLabel: escapeHtml(templateData.statusLabel || "Order Created"),
    statusMsg: escapeHtml(
      templateData.statusMsg ||
        "An order has been generated and is now pending further action.",
    ),
    refId: escapeHtml(templateData.refId || "—"),
    orderId: escapeHtml(templateData.orderId || "—"),
    route: escapeHtml(templateData.route || "—"),
    eta: escapeHtml(templateData.etaFormatted || "—"),
    lastUpdated: escapeHtml(
      templateData.lastUpdated || formatDateTime(new Date()),
    ),
    trackLink: escapeHtml(templateData.trackLink || `${TRACK_BASE}/`),
    actionLinks: buildActionLinksHtml(
      templateData.statusLabel || "Order Created",
      templateData.refId,
      templateData.recipientType,
    ),
    currentYear: new Date().getFullYear(),
  });
}

function buildShipmentUpdateHtml(templateData) {
  return renderTemplate("shipment_update.html", {
    statusLabel: escapeHtml(templateData.statusLabel || "Shipment Updated"),
    statusMsg: escapeHtml(
      templateData.statusMsg || "We have an update on your shipment.",
    ),
    refId: escapeHtml(templateData.refId || "—"),
    orderId: escapeHtml(templateData.orderId || "—"),
    route: escapeHtml(templateData.route || "—"),
    eta: escapeHtml(templateData.etaFormatted || "—"),
    lastUpdated: escapeHtml(
      templateData.lastUpdated || formatDateTime(new Date()),
    ),
    trackLink: escapeHtml(templateData.trackLink || `${TRACK_BASE}/`),
    actionLinks: buildActionLinksHtml(
      templateData.statusLabel === "Order Created"
        ? ""
        : templateData.statusLabel || "Shipment Updated",
      templateData.refId,
      templateData.recipientType,
    ),
    currentYear: new Date().getFullYear(),
  });
}

export async function sendOrderEmail(toEmails, templateData) {
  const emails = (Array.isArray(toEmails) ? toEmails : [toEmails])
    .filter((e) => typeof e === "string" && e.includes("@"))
    .map((e) => e.trim());

  if (emails.length === 0) {
    return { success: false, message: "No valid recipients" };
  }

  const html = templateData.isNewOrder
    ? buildOrderCreatedHtml(templateData)
    : buildShipmentUpdateHtml(templateData);

  const mailOptions = {
    from: `"${process.env.EMAIL_FROM_NAME || "Royal Gulf Shipping"}" <${process.env.EMAIL_FROM_ADDRESS || "support@royalgulfshipping.com"}>`,
    to: emails.join(", "),
    subject: buildSubject(templateData),
    html,
  };

  try {
    const info = await transporter.sendMail(mailOptions);
    logger.info("Notification email sent", {
      recipients: emails.length,
      messageId: info.messageId,
    });
    return { success: true, messageId: info.messageId };
  } catch (error) {
    logger.error("Failed to send notification email", {
      error: error.message,
      code: error.code,
    });
    return { success: false, error: error.message };
  }
}

async function isStatusEmailEnabled(orderStatus) {
  if (!orderStatus) return false;
  const { rows } = await pool.query(
    `SELECT send_email FROM statuses
      WHERE status = true AND order_status ILIKE $1 LIMIT 1`,
    [orderStatus],
  );
  return rows[0]?.send_email === true;
}

async function getSubscribedEmails(trackingId, orderId) {
  const { rows } = await pool.query(
    `SELECT DISTINCT email
       FROM notification_subscriptions
      WHERE (reference_id IS NOT NULL AND TRIM(reference_id) ILIKE TRIM($1))
         OR (order_id IS NOT NULL AND order_id = $2)`,
    [trackingId, orderId],
  );
  return rows.map((r) => r.email).filter(Boolean);
}

function mergeRecipients(primaryEmails, subscriberEmails) {
  const seen = new Map();
  for (const e of [...primaryEmails, ...subscriberEmails]) {
    if (!e) continue;
    const key = e.trim().toLowerCase();
    if (!seen.has(key)) seen.set(key, e.trim());
  }
  return [...seen.values()];
}

async function resolveTarget(trackingId) {
  const tid = String(trackingId || "")
    .trim()
    .toUpperCase();
  if (!tid) return null;

  const { rows } = await pool.query(
    `SELECT o.id AS order_id, o.rgl_booking_number, o.status AS order_status,
            oi.id AS item_id, oi.status AS item_status,
            pol.name AS pol_name, pod.name AS pod_name,
            CASE WHEN oi.id IS NULL THEN o.eta ELSE (
              SELECT ot.eta FROM order_tracking ot
               WHERE ot.order_id = o.id AND ot.item_ref = oi.item_ref
                 AND ot.eta IS NOT NULL
               ORDER BY ot.created_time DESC, ot.id DESC LIMIT 1
            ) END AS eta
       FROM orders o
       LEFT JOIN order_items oi
              ON oi.order_id = o.id AND UPPER(oi.tracking_id) = $1
       LEFT JOIN places pol ON pol.id = o.place_of_loading
       LEFT JOIN places pod ON pod.id = o.place_of_delivery
      WHERE UPPER(o.tracking_id) = $1 OR oi.id IS NOT NULL
      LIMIT 1`,
    [tid],
  );
  return rows[0] || null;
}

async function sendTrackingEmail({
  email,
  trackingId,
  recipientType,
  receiverName,
  isNewOrder = false,
}) {
  if (!email || typeof email !== "string" || !email.includes("@")) {
    return { success: false, error: "Invalid email address" };
  }
  if (!trackingId) {
    return { success: false, error: "trackingId is required" };
  }

  const t = await resolveTarget(trackingId);
  if (!t) return { success: false, error: "No order found for tracking ID" };

  const status = t.item_id ? t.item_status : t.order_status;

  if (!isNewOrder && !(await isStatusEmailEnabled(status))) {
    return {
      success: false,
      skipped: true,
      message: `Notifications for status "${status}" are disabled`,
    };
  }

  const isSender = String(recipientType || "").toLowerCase() === "sender";
  const subscribers =
    isSender || isNewOrder
      ? []
      : await getSubscribedEmails(trackingId, t.order_id);
  const emails = mergeRecipients([email.trim()], subscribers);

  return sendOrderEmail(emails, {
    isNewOrder,
    recipientType,
    receiverName: receiverName || "Valued Customer",
    statusLabel: isNewOrder ? "Order Created" : status || "Shipment Updated",
    statusMsg: isNewOrder
      ? "An order has been generated and is now pending further action."
      : "We have an update on your shipment.",
    refId: trackingId,
    orderId: t.rgl_booking_number || "—",
    route: `${t.pol_name || "—"} → ${t.pod_name || "—"}`,
    etaFormatted: t.eta ? formatDate(t.eta) : "",
    trackLink: buildTrackLink(trackingId),
  });
}

export const sendShipmentEmail = (data) =>
  sendTrackingEmail({ ...data, isNewOrder: true });

export async function notifySingleStatusUpdate(data) {
  const r = await sendTrackingEmail({ ...data, isNewOrder: false });
  return {
    success: r.success,
    skipped: r.skipped,
    message: r.message,
    error: r.error,
    results: [r],
  };
}

export async function subscribeToShipment(shipmentData) {
  const { email, referenceId, place_of_loading, place_of_delivery } =
    shipmentData;

  if (!email || typeof email !== "string" || !email.trim().includes("@")) {
    return { success: false, error: "Invalid email address" };
  }
  if (!referenceId) {
    return { success: false, error: "referenceId is required" };
  }

  const t = await resolveTarget(referenceId);
  if (!t) {
    return { success: false, error: "No order found for this tracking ID" };
  }

  try {
    await pool.query(
      `INSERT INTO notification_subscriptions (order_id, reference_id, email)
       VALUES ($1, $2, $3)
       ON CONFLICT (order_id, email) DO UPDATE SET updated_at = now()`,
      [t.order_id, referenceId, email.trim()],
    );
  } catch (err) {
    logger.error("Failed to save subscription", {
      referenceId,
      email,
      error: err.message,
    });
    return { success: false, error: "Could not save subscription" };
  }

  const mailOptions = {
    from: `"${process.env.EMAIL_FROM_NAME || "Royal Gulf Shipping"}" <${process.env.EMAIL_FROM_ADDRESS || "support@royalgulfshipping.com"}>`,
    to: email.trim(),
    subject: `You're subscribed to updates for shipment ${referenceId}`,
    html: buildSubscriptionConfirmationHtml({
      refId: referenceId,
      orderId: t.rgl_booking_number || "—",
      place_of_loading: place_of_loading || t.pol_name || "—",
      place_of_delivery: place_of_delivery || t.pod_name || "—",
      eta: t.eta ? formatDate(t.eta) : "",
      trackLink: buildTrackLink(referenceId),
    }),
  };

  try {
    const info = await transporter.sendMail(mailOptions);
    return { success: true, messageId: info.messageId };
  } catch (error) {
    logger.error("Failed to send subscription confirmation", {
      error: error.message,
    });
    return { success: false, error: error.message };
  }
}
