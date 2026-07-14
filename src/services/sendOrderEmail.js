import pool from "../db/pool.js";
import { transporter } from "../middleware/nodeMailer.js";
import { escapeHtml } from "./escapeHtml.js";
import logger from "./logger.js";

function buildSubject(templateData) {
  return `Royal Gulf Shipping – ${templateData.statusLabel} (Ref: ${templateData.refId || "—"})`;
}

function buildSubscriptionConfirmationHtml(templateData) {
  const currentYear = new Date().getFullYear();
  const receiverName = escapeHtml(
    templateData.receiverName || "Valued Customer",
  );
  const refId = escapeHtml(templateData.refId || "—");
  const pol = escapeHtml(templateData.place_of_loading || "");
  const pod = escapeHtml(templateData.place_of_delivery || "");
  const currentStatus = escapeHtml(templateData.currentStatus || "");
  const trackLink = escapeHtml(
    templateData.trackLink || "https://ordertracking.royalgulfshipping.com/",
  );

  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Subscribed to Shipment Updates</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height:1.5; color:#1e293b; background:#f8fafc; margin:0; }
    .wrap { max-width:600px; margin:0 auto; background:white; border-radius:8px; overflow:hidden; box-shadow:0 4px 12px rgba(0,0,0,0.08); }
    .logo-bar { padding:24px; text-align:center; background:#0f172a; }
    .logo { max-width:220px; height:auto; }
    .brand { text-align:center; padding:16px 24px; background:#0f172a; color:white; }
    .brand h1 { margin:0; font-size:22px; }
    .brand p { margin:4px 0 0; opacity:0.8; font-size:14px; }
    .accent { border:none; height:4px; background:linear-gradient(90deg, #097D76, #F38120); margin:0; }
    .tag-row { padding:24px; }
    .tag { display:inline-block; padding:6px 12px; background:#097D76; color:white; font-weight:600; border-radius:4px; margin-bottom:12px; font-size:13px; letter-spacing:0.3px; }
    .title { margin:0 0 16px; color:#1e293b; font-size:22px; }
    .info-box { padding:16px; background:#f9fafb; border-radius:8px; margin:16px 0; border:1px solid #eef2f7; }
    .info-row { display:flex; justify-content:space-between; padding:6px 0; font-size:14px; }
    .info-row .k { color:#64748b; }
    .info-row .v { font-weight:700; color:#0f172a; }
    .cta { display:inline-block; margin:20px 0; padding:14px 32px; background:#F38120; color:white; text-decoration:none; font-weight:600; border-radius:6px; }
    .foot { text-align:center; padding:24px; background:#f1f5f9; font-size:13px; color:#64748b; }
    .muted { color:#64748b; font-size:13px; }
  </style>
</head>
<body style="padding:16px;background:#f8fafc;">
  <div class="wrap">
    <div class="logo-bar">
      <img class="logo" src="https://royalgulfshipping.com/wp-content/uploads/2025/09/royalgulflogo-1.jpeg" alt="Royal Gulf Shipping Logo">
    </div>
    <div class="brand">
      <h1>Royal Gulf Shipping & Logistics LLC</h1>
      <p>Dubai • London • Karachi • Shenzhen</p>
    </div>
    <hr class="accent">
    <div class="tag-row">
      <div class="tag">Subscribed</div>
      <div class="content">
        <h2 class="title">You're subscribed to updates for shipment ${refId}</h2>
        <p>Dear ${receiverName},</p>
        <p>You'll now receive an email every time the status of this shipment changes.</p>

        <div class="info-box">
          <div class="info-row"><span class="k">Reference ID</span><span class="v">${refId}</span></div>
          <div class="info-row"><span class="k">Route</span><span class="v">${pol} -> ${pod}</span></div>
          ${currentStatus ? `<div class="info-row"><span class="k">Current Status</span><span class="v">${currentStatus}</span></div>` : ""}
        </div>

        <a class="cta" href="${trackLink}" target="_blank" rel="noopener noreferrer">Track This Shipment</a>

        <p class="muted">Didn't request this? You can ignore this email — no further updates will be sent unless this reference ID is tracked again from our site.</p>
      </div>
    </div>
    <div class="foot">
      © ${currentYear} Royal Gulf Shipping & Logistics LLC — All rights reserved.<br>
      Need help? Call <a href="tel:+971555658321" style="color:#097D76;">+971 555 658 321</a> or email
      <a href="mailto:sales@royalgulfshipping.com" style="color:#097D76;">sales@royalgulfshipping.com</a>
    </div>
  </div>
</body>
</html>`;
}

function buildHtml(templateData) {
  const currentYear = new Date().getFullYear();
  const receiverName = escapeHtml(
    templateData.receiverName || "Valued Customer",
  );
  const statusLabel = escapeHtml(templateData.statusLabel || "Status Updated");
  const statusMsg = escapeHtml(
    templateData.statusMsg || "We are working on your shipment.",
  );
  const trackLink = escapeHtml(
    templateData.trackLink || "https://ordertracking.royalgulfshipping.com/",
  );
  const refId = escapeHtml(templateData.refId || "—");
  const route = escapeHtml(templateData.route || "");
  const etaFormatted = escapeHtml(templateData.etaFormatted || "");

  let routeEtaHtml = "";
  if (route || etaFormatted) {
    routeEtaHtml = `
      <div style="display:flex; gap:12px; margin:16px 0; flex-wrap:wrap;">
        ${
          route
            ? `
        <div style="flex:1; min-width:160px; background:#f9fafb; border:1px solid #eef2f7; border-radius:10px; padding:10px 14px;">
          <div style="font-size:11px; color:#64748b; text-transform:uppercase; letter-spacing:0.5px;">Route</div>
          <div style="font-weight:700; font-size:14px; margin-top:4px;">${route}</div>
        </div>`
            : ""
        }
        ${
          etaFormatted
            ? `
        <div style="flex:1; min-width:160px; background:#f9fafb; border:1px solid #eef2f7; border-radius:10px; padding:10px 14px;">
          <div style="font-size:11px; color:#64748b; text-transform:uppercase; letter-spacing:0.5px;">Estimated Arrival</div>
          <div style="font-weight:700; font-size:14px; margin-top:4px;">${etaFormatted}</div>
        </div>`
            : ""
        }
      </div>`;
  }

  let itemsHtml = "";
  if (
    Array.isArray(templateData.updatedItems) &&
    templateData.updatedItems.length > 0
  ) {
    const rows = templateData.updatedItems
      .map(
        (item) =>
          `<div style="margin-bottom:4px;">${escapeHtml(item.itemRef || "—")}</div>`,
      )
      .join("");
    itemsHtml = `
      <h3 style="margin:20px 0 10px; font-size:18px;">Your Updated Shipments</h3>
      <table role="presentation" style="width:100%; border-collapse:collapse;">
        <tr style="background:#f1f5f9;">
          <td style="padding:10px; font-weight:bold;">Item Ref</td>
          <td style="padding:10px;">${rows}</td>
        </tr>
      </table>`;
  }

  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Royal Gulf Shipping – Shipment Update</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height:1.5; color:#1e293b; background:#f8fafc; margin:0; }
    .wrap { max-width:600px; margin:0 auto; background:white; border-radius:8px; overflow:hidden; box-shadow:0 4px 12px rgba(0,0,0,0.08); }
    .logo-bar { padding:24px; text-align:center; background:#0f172a; }
    .logo { max-width:220px; height:auto; }
    .brand { text-align:center; padding:16px 24px; background:#0f172a; color:white; }
    .brand h1 { margin:0; font-size:22px; }
    .brand p { margin:4px 0 0; opacity:0.8; font-size:14px; }
    .accent { border:none; height:4px; background:linear-gradient(90deg, #097D76, #F38120); margin:0; }
    .tag-row { padding:24px; }
    .tag { display:inline-block; padding:6px 12px; background:#097D76; color:white; font-weight:600; border-radius:4px; margin-bottom:12px; font-size:13px; letter-spacing:0.3px; }
    .title { margin:0 0 16px; color:#1e293b; font-size:24px; }
    .status { padding:16px; background:#effaf9; border-radius:8px; margin:16px 0; border:1px solid #097D76; }
    .status .status-heading { font-size:12px; font-weight:700; color:#097D76; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:6px; }
    .status .label { font-weight:700; color:#0f172a; font-size:18px; margin-bottom:6px; }
    .status .msg { color:#475569; font-size:14px; }
    .cta { display:inline-block; margin:20px 0; padding:14px 32px; background:#F38120; color:white; text-decoration:none; font-weight:600; border-radius:6px; }
    .foot { text-align:center; padding:24px; background:#f1f5f9; font-size:13px; color:#64748b; }
    .muted { color:#64748b; font-size:13px; }
  </style>
</head>
<body style="padding:16px;background:#f8fafc;">
  <div class="wrap">
    <div class="logo-bar">
      <img class="logo" src="https://royalgulfshipping.com/wp-content/uploads/2025/09/royalgulflogo-1.jpeg" alt="Royal Gulf Shipping Logo">
    </div>
    <div class="brand">
      <h1>Royal Gulf Shipping & Logistics LLC</h1>
      <p>Dubai • London • Karachi • Shenzhen</p>
    </div>
    <hr class="accent">
    <div class="tag-row">
      <div class="tag">Shipment ${escapeHtml(refId)}</div>
      <div class="content">
        <h2 class="title">${statusLabel}</h2>
        <p>Dear ${receiverName},</p>
        <div class="status">
          <div class="status-heading">Current Status</div>
          <div class="label">${statusLabel}</div>
          <div class="msg">${statusMsg}</div>
        </div>
        ${routeEtaHtml}
        ${itemsHtml}
        <a class="cta" style="text-decoraion:none" href="${trackLink}" target="_blank" rel="noopener noreferrer">View Live Tracking</a>
        <p class="muted" style="margin:16px 0;">
          If the button doesn't work, visit:<br>
          <a href="https://ordertracking.royalgulfshipping.com/">https://ordertracking.royalgulfshipping.com/</a><br>
          and enter reference ID: <strong>${refId}</strong>
        </p>
        <p class="muted">You're receiving this because you subscribed to shipment notifications.</p>
      </div>
    </div>
    <div class="foot">
      © ${currentYear} Royal Gulf Shipping & Logistics LLC — All rights reserved.<br>
      Need help? Call <a href="tel:+971555658321" style="color:#097D76;">+971 555 658 321</a> or email
      <a href="mailto:sales@royalgulfshipping.com" style="color:#097D76;">sales@royalgulfshipping.com</a>
    </div>
  </div>
</body>
</html>`;
}

async function sendOrderEmail(toEmails, templateData) {
  const emails = (Array.isArray(toEmails) ? toEmails : [toEmails])
    .filter((e) => typeof e === "string" && e.includes("@"))
    .map((e) => e.trim());

  if (emails.length === 0) {
    return { success: false, message: "No valid recipients" };
  }

  const mailOptions = {
    from: `"${process.env.EMAIL_FROM_NAME || "Royal Gulf Shipping"}" <${process.env.EMAIL_FROM_ADDRESS || "support@royalgulfshipping.com"}>`,
    to: emails.join(", "),
    subject: buildSubject(templateData),
    html: buildHtml(templateData),
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

export async function sendShipmentEmail(shipmentData) {
  const { email, orderId, referenceId } = shipmentData;

  if (!email || typeof email !== "string" || !email.trim().includes("@")) {
    return { success: false, error: "Invalid email address" };
  }
  if (!orderId) {
    return { success: false, error: "orderId is required" };
  }

  const receiverName =
    String(shipmentData.receiverName || "Valued Customer")
      .trim()
      .replace(/\s+/g, " ") || "Valued Customer";

  const templateData = {
    receiverName,
    statusLabel: String(shipmentData.statusLabel || "Shipment Updated"),
    statusMsg: String(
      shipmentData.statusMsg || "We have an update on your shipment.",
    ),
    refId: String(shipmentData.refId || referenceId || "—"),
    trackLink: String(
      shipmentData.trackLink || "https://consolidatetracking.onrender.com/",
    ),
    updatedItems: Array.isArray(shipmentData.updatedItems)
      ? shipmentData.updatedItems
      : [],
  };

  try {
    await pool.query(
      `INSERT INTO notification_subscriptions (order_id, reference_id, email)
       VALUES ($1, $2, $3)
       ON CONFLICT (order_id, email)
       DO UPDATE SET reference_id = EXCLUDED.reference_id, updated_at = now()`,
      [orderId, referenceId || templateData.refId, email.trim()],
    );
  } catch (err) {
    logger.error("Failed to save notification subscription", {
      orderId,
      email,
      error: err.message,
    });
  }

  return sendOrderEmail(email, templateData);
}

export async function notifyOrderStatusUpdate(orderId, statusData) {
  const { rows } = await pool.query(
    `SELECT email FROM notification_subscriptions WHERE order_id = $1`,
    [orderId],
  );

  if (rows.length === 0) {
    return {
      success: false,
      skipped: true,
      message: "No subscribers for this order",
    };
  }

  const templateData = {
    receiverName: statusData.receiverName || "Valued Customer",
    statusLabel: String(statusData.statusLabel || "Shipment Updated"),
    statusMsg: String(
      statusData.statusMsg || "We have an update on your shipment.",
    ),
    refId: String(statusData.refId || "—"),
    route: String(statusData.route || ""),
    etaFormatted: String(statusData.etaFormatted || ""),
    trackLink: String(
      statusData.trackLink || "https://consolidatetracking.onrender.com/",
    ),
    updatedItems: Array.isArray(statusData.updatedItems)
      ? statusData.updatedItems
      : [],
  };

  return sendOrderEmail(
    rows.map((r) => r.email),
    templateData,
  );
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

  const { rows } = await pool.query(
    `SELECT id,
            (SELECT status FROM receivers r WHERE r.order_id = orders.id LIMIT 1) AS current_status
     FROM orders WHERE booking_ref = $1 LIMIT 1`,
    [referenceId],
  );

  if (rows.length === 0) {
    return { success: false, error: "No order found for this reference ID" };
  }
  const order = rows[0];

  try {
    await pool.query(
      `INSERT INTO notification_subscriptions (order_id, reference_id, email)
       VALUES ($1, $2, $3)
       ON CONFLICT (order_id, email) DO UPDATE SET updated_at = now()`,
      [order.id, referenceId, email.trim()],
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
      place_of_loading: place_of_loading || "—",
      place_of_delivery: place_of_delivery || "—",
      currentStatus: order.current_status || "",
      trackLink: `https://consolidatetracking.onrender.com/?ref=${encodeURIComponent(referenceId)}`,
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
