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
  const eta = escapeHtml(templateData.eta || "—");
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

export async function sendShipmentEmail(shipmentData) {
  const { email, orderId, itemRef: passedItemRef } = shipmentData;

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

  let formNo = "—";
  let etaFormatted = String(shipmentData.etaFormatted || "");
  let route = String(shipmentData.route || "");
  const itemRef =
    passedItemRef || shipmentData.refId || shipmentData.referenceId || "—";

  try {
    const { rows } = await pool.query(
      `SELECT o.rgl_booking_number, o.booking_ref, ot.eta
        FROM orders o
       LEFT JOIN order_tracking ot ON ot.order_id = o.id
       WHERE o.id = $1
       ORDER BY ot.created_time DESC
       LIMIT 1`,
      [orderId],
    );

    const order = rows[0];
    formNo = order?.rgl_booking_number || "—";

    if (!etaFormatted && order?.eta) {
      etaFormatted = new Date(order.eta).toLocaleDateString("en-GB", {
        day: "2-digit",
        month: "short",
        year: "numeric",
      });
    }
  } catch (err) {
    logger.error("Failed to fetch order details for order created email", {
      orderId,
      error: err.message,
    });
  }

  const templateData = {
    isNewOrder: true,
    receiverName,
    statusLabel: String(shipmentData.statusLabel || "Order Created"),
    statusMsg: String(
      shipmentData.statusMsg ||
        "An order has been generated and is now pending further action.",
    ),
    refId: itemRef,
    orderId: formNo,
    route,
    etaFormatted,
    trackLink: String(
      shipmentData.trackLink ||
        `https://consolidatetracking-1.onrender.com/?ref=${encodeURIComponent(itemRef)}`,
    ),
    updatedItems: Array.isArray(shipmentData.updatedItems)
      ? shipmentData.updatedItems
      : [],
  };

  return sendOrderEmail(email, templateData);
}

async function getSubscribedEmails(itemRef, orderId) {
  const { rows } = await pool.query(
    `SELECT DISTINCT email
       FROM notification_subscriptions
      WHERE (reference_id IS NOT NULL AND TRIM(reference_id) ILIKE TRIM($1))
         OR (order_id IS NOT NULL AND order_id = $2)`,
    [itemRef, orderId],
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

async function buildAndSendTracking(
  itemRef,
  orderId,
  primaryEmails,
  statusData,
) {
  const { rows } = await pool.query(
    `SELECT ot.order_id, ot.eta, ot.status AS current_status, o.rgl_booking_number,
            pol.name AS pol_name, pod.name AS pod_name
       FROM order_tracking ot
       JOIN orders o ON o.id = ot.order_id
       LEFT JOIN places pol ON pol.id = o.place_of_loading
       LEFT JOIN places pod ON pod.id = o.place_of_delivery
      WHERE TRIM(ot.item_ref) ILIKE TRIM($1)
      ORDER BY ot.created_time DESC
      LIMIT 1`,
    [itemRef],
  );

  if (rows.length === 0) {
    logger.error("No order_tracking record found for item_ref", { itemRef });
    return { itemRef, success: false, error: "No tracking record found" };
  }

  const tracking = rows[0];
  const subscriberEmails = await getSubscribedEmails(itemRef, orderId);
  const emails = mergeRecipients(primaryEmails, subscriberEmails);

  if (emails.length === 0) {
    logger.error("No recipients (primary or subscribed) for item_ref", {
      itemRef,
    });
    return { itemRef, success: false, error: "No recipients found" };
  }

  const route =
    statusData.route ||
    `${tracking.pol_name || "—"} → ${tracking.pod_name || "—"}`;

  const templateData = {
    isNewOrder: false,
    receiverName: statusData.receiverName || "Valued Customer",
    statusLabel: String(
      statusData.statusLabel || tracking.current_status || "Shipment Updated",
    ),
    statusMsg: String(
      statusData.statusMsg || "We have an update on your shipment.",
    ),
    refId: itemRef,
    orderId: tracking.rgl_booking_number || "—",
    route,
    etaFormatted: tracking.eta
      ? new Date(tracking.eta).toLocaleDateString("en-GB", {
          day: "2-digit",
          month: "short",
          year: "numeric",
        })
      : "",
    trackLink: `https://consolidatetracking-1.onrender.com/?ref=${encodeURIComponent(itemRef)}`,
    updatedItems: Array.isArray(statusData.updatedItems)
      ? statusData.updatedItems
      : [],
  };

  const result = await sendOrderEmail(emails, templateData);
  return { itemRef, emails, ...result };
}

export async function notifyOrderStatusUpdate(orderId, statusData) {
  const groupedByRef = subscriptions.reduce((acc, sub) => {
    const ref = sub.reference_id || "—";
    if (!acc[ref]) acc[ref] = [];
    acc[ref].push(sub.email);
    return acc;
  }, {});

  const results = [];
  for (const [itemRef, emails] of Object.entries(groupedByRef)) {
    results.push(await buildAndSendTracking(itemRef, emails, statusData));
  }

  return { success: results.every((r) => r.success), results };
}

export async function notifySingleStatusUpdate(statusData) {
  if (!statusData.itemRef) {
    return { success: false, error: "Missing itemRef in statusData" };
  }
  const primaryEmails = statusData.email ? [statusData.email] : [];
  const result = await buildAndSendTracking(
    statusData.itemRef,
    statusData.orderId,
    primaryEmails,
    statusData,
  );
  return { success: result.success, results: [result] };
}

export async function notifySubscriber(orderId, statusData) {
  const { rows: subscriptions } = await pool.query(
    `SELECT email, reference_id FROM notification_subscriptions WHERE order_id = $1`,
    [orderId],
  );

  if (subscriptions.length === 0) {
    return {
      success: false,
      skipped: true,
      message: "No subscribers for this order",
    };
  }

  const groupedByRef = subscriptions.reduce((acc, sub) => {
    const ref = sub.reference_id || "—";
    if (!acc[ref]) acc[ref] = [];
    acc[ref].push(sub.email);
    return acc;
  }, {});

  const results = [];

  for (const [itemRef, emails] of Object.entries(groupedByRef)) {
    const { rows } = await pool.query(
      `SELECT ot.order_id, ot.eta, ot.status AS current_status, o.rgl_booking_number,
              pol.name AS pol_name, pod.name AS pod_name
        FROM order_tracking ot
        JOIN orders o ON o.id = ot.order_id
        LEFT JOIN places pol ON pol.id = o.place_of_loading
        LEFT JOIN places pod ON pod.id = o.place_of_delivery
        WHERE TRIM(ot.item_ref) ILIKE TRIM($1)
        ORDER BY ot.created_time DESC
        LIMIT 1`,
      [itemRef],
    );

    if (rows.length === 0) {
      logger.error("No order_tracking record found for subscribed item_ref", {
        orderId,
        itemRef,
      });
      continue;
    }
    const tracking = rows[0];

    const route =
      statusData.route ||
      `${tracking.pol_name || "—"} → ${tracking.pod_name || "—"}`;

    const templateData = {
      isNewOrder: false,
      receiverName: statusData.receiverName || "Valued Customer",
      statusLabel: String(
        statusData.statusLabel || tracking.current_status || "Shipment Updated",
      ),
      statusMsg: String(
        statusData.statusMsg || "We have an update on your shipment.",
      ),
      refId: itemRef,
      orderId: tracking.rgl_booking_number || "—",
      route,
      etaFormatted: tracking.eta
        ? new Date(tracking.eta).toLocaleDateString("en-GB", {
            day: "2-digit",
            month: "short",
            year: "numeric",
          })
        : "",
      trackLink: `https://consolidatetracking-1.onrender.com/?ref=${encodeURIComponent(itemRef)}`,
      updatedItems: Array.isArray(statusData.updatedItems)
        ? statusData.updatedItems
        : [],
    };

    const result = await sendOrderEmail(emails, templateData);
    results.push({ itemRef, emails, ...result });
  }

  return { success: results.every((r) => r.success), results };
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
    `SELECT ot.order_id, ot.eta, ot.status AS current_status, o.rgl_booking_number
      FROM order_tracking ot
      JOIN orders o ON o.id = ot.order_id
      WHERE TRIM(ot.item_ref) ILIKE TRIM($1)
      ORDER BY ot.created_time DESC
      LIMIT 1`,
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
      [order.order_id, referenceId, email.trim()],
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
      orderId: order.rgl_booking_number || "—",
      place_of_loading: place_of_loading || "—",
      place_of_delivery: place_of_delivery || "—",
      currentStatus: order.current_status || "",
      eta: order.eta || "",
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
