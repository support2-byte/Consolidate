import pool from "../db/pool.js";
import { transporter } from "../middleware/nodeMailer.js";
import { escapeHtml } from "./escapeHtml.js";
import logger from "./logger.js";
import { renderTemplate } from "./renderTemplate.js";

function buildSubject(templateData) {
  return `Royal Gulf Shipping – ${templateData.statusLabel} (Ref: ${templateData.refId || "—"})`;
}

function buildSubscriptionConfirmationHtml(templateData) {
  const refId = escapeHtml(templateData.refId || "—");
  const orderId = escapeHtml(templateData.orderId || "—");
  const pol = escapeHtml(templateData.place_of_loading || "—");
  const pod = escapeHtml(templateData.place_of_delivery || "—");
  const route = `${pol} &rarr; ${pod}`;
  const eta = escapeHtml(templateData.etaFormatted || "—");
  const lastUpdated = escapeHtml(
    templateData.lastUpdated || new Date().toLocaleString(),
  );
  const trackLink = escapeHtml(
    templateData.trackLink || "https://ordertracking.royalgulfshipping.com/",
  );

  return renderTemplate("subscription_confirmed.html", {
    refId,
    orderId,
    route,
    eta,
    lastUpdated,
    trackLink,
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
    eta: escapeHtml(templateData.etaFormatted || "—"),
    lastUpdated: escapeHtml(
      templateData.lastUpdated || new Date().toLocaleString(),
    ),
    trackLink: escapeHtml(
      templateData.trackLink || "https://ordertracking.royalgulfshipping.com/",
    ),
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
      templateData.lastUpdated || new Date().toLocaleString(),
    ),
    trackLink: escapeHtml(
      templateData.trackLink || "https://ordertracking.royalgulfshipping.com/",
    ),
  });
}

async function sendOrderEmail(toEmails, templateData) {
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
    isNewOrder: true,
    receiverName,
    statusLabel: String(shipmentData.statusLabel || "Order Created"),
    statusMsg: String(
      shipmentData.statusMsg ||
        "An order has been generated and is now pending further action.",
    ),
    refId: String(shipmentData.refId || referenceId || "—"),
    orderId: String(orderId),
    etaFormatted: String(shipmentData.etaFormatted || ""),
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
    isNewOrder: false,
    receiverName: statusData.receiverName || "Valued Customer",
    statusLabel: String(statusData.statusLabel || "Shipment Updated"),
    statusMsg: String(
      statusData.statusMsg || "We have an update on your shipment.",
    ),
    refId: String(statusData.refId || "—"),
    orderId: String(orderId),
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
      orderId: order.id,
      place_of_loading: place_of_loading || "—",
      place_of_delivery: place_of_delivery || "—",
      etaFormatted: order.eta_formatted || "",
      trackLink: `https://consolidatetracking-1.onrender.com/?ref=${encodeURIComponent(referenceId)}`,
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
