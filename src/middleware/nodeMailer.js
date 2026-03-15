import dotenv from 'dotenv';
import nodemailer from 'nodemailer';
import { getNotificationSettings } from '../modules/orders/order.controller.js';  // Adjust path as needed
// import  pool  from '../db/pool';  // Assuming you have a db.js that exports a configured pg Pool 
// Load environment variables
dotenv.config();
console.log('Email configuration:', {
  service: process.env.EMAIL_SERVICE,
  user: process.env.GMAIL_USER,
  fromName: process.env.GMAIL_PASS,
  fromAddress: process.env.GMAIL_FROM_ADDRESS
});
// Create the transporter (reusable)
const transporter = nodemailer.createTransport({
  service: process.env.EMAIL_SERVICE || 'gmail',
  auth: {
    user: process.env.GMAIL_USER,
    pass: process.env.GMAIL_PASS
  },
  secure: true,  // Use SSL (port 465 implicit for Gmail)
  tls: {
    rejectUnauthorized: false  // For dev; remove in prod
  }
});

// // e.g. src/utils/notificationUtils.js
// async function getNotificationSettings(typeCode) {
//   try {
//     const client = await pool.connect();
//     const result = await client.query(`
//       SELECT 
//         nt.type_code,
//         nt.name,
//         ns.enabled,
//         ns.subject,
//         ns.heading,
//         ns.additional_content,
//         ns.email_type,
//         ns.recipients,
//         ns.trigger_statuses
//       FROM notification_types nt
//       LEFT JOIN notification_settings ns ON ns.type_id = nt.id
//       WHERE nt.type_code = $1
//     `, [typeCode]);

//     if (result.rows.length === 0) {
//       return null; // or default settings
//     }

//     const settings = result.rows[0];
//     return {
//       ...settings,
//       trigger_statuses: settings.trigger_statuses
//         ? settings.trigger_statuses.split(',').map(s => s.trim().toLowerCase())
//         : [],
//     };
//   } catch (err) {
//     console.error('Failed to fetch notification settings:', err);
//     return null;
//   }
// }

// Verify on startup (optional, for debugging)
transporter.verify((error, success) => {
  if (error) {
    console.error('Email transporter verification failed:', error);
  } else {
    console.log('Email transporter ready!');
  }
});


// Safe escape function (defined first – no initialization errors)
// const escapeHtml = (unsafe) => {
//   const safeString = String(unsafe ?? '');
//   return safeString
//     .replace(/&/g, "&amp;")
//     .replace(/</g, "&lt;")
//     .replace(/>/g, "&gt;")
//     .replace(/"/g, "&quot;")
//     .replace(/'/g, "&#039;");
// };

// Route city mapping
const ROUTE_CITY_MAP = {
  '1': 'Shenzhen',
  '2': 'Karachi',
  '3': 'London',
  '5': 'Dubai',
  // Add more as needed
};

// ────────────────────────────────────────────────────────────────────────────────
//  Helper: Basic HTML escape (you can also use he or a real template engine)
function escapeHtml(unsafe) {
  if (typeof unsafe !== 'string') return '';
  return unsafe
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

// ────────────────────────────────────────────────────────────────────────────────
async function sendOrderEmail(toEmails, notificationType, templateData) {
  console.log(`Preparing to send ${notificationType} email to:`, toEmails);

  // Normalize recipients
  if (!Array.isArray(toEmails)) {
    toEmails = [toEmails].filter(Boolean);
  }

  toEmails = toEmails
    .filter(email => typeof email === 'string' && email.trim() && email.includes('@'))
    .map(email => email.trim());

  if (toEmails.length === 0) {
    console.warn('No valid email recipients found.');
    return { success: false, message: 'No valid recipients' };
  }

  // 1. Fetch notification settings
  let settings;
  try {
    settings = await getNotificationSettings(notificationType);
  } catch (err) {
    console.error(`Failed to load notification settings for ${notificationType}:`, err);
    // Decide policy: fail silently / fail hard / send default email
    // Here: fail silently but log
    settings = {};
  }

  // Disabled / missing config → skip (uncomment when ready)
  if (!settings || settings.enabled === false) {
    console.log(`Notification "${notificationType}" is disabled or not configured. Skipping.`);
    return { success: false, message: `Notification type "${notificationType}" is disabled` };
  }

  // 2. Optional: status filter for status-update notifications
  if (notificationType.includes('status-update') && templateData?.currentStatus) {
    const currentStatus = String(templateData.currentStatus).toLowerCase().trim();
    const allowed = Array.isArray(settings.trigger_statuses) ? settings.trigger_statuses : [];

    if (allowed.length > 0 && !allowed.map(s => s.toLowerCase().trim()).includes(currentStatus)) {
      console.log(
        `Status "${currentStatus}" not in allowed list [${allowed.join(', ')}]. Skipping email.`
      );
      return {
        success: false,
        message: `Current status "${currentStatus}" not configured for notifications`,
      };
    }
  }

  // 3. Prepare route display (city names)
  let routeDisplay = templateData.route || '—';
  if (routeDisplay.includes('→')) {
    const [fromCode, toCode] = routeDisplay.split('→').map(s => s.trim());
    const fromCity = ROUTE_CITY_MAP?.[fromCode] || fromCode || '—';
    const toCity   = ROUTE_CITY_MAP?.[toCode]   || toCode   || '—';
    routeDisplay = `${fromCity} → ${toCity}`;
  }

  const currentYear      = new Date().getFullYear();
  const formattedLastUpdated = templateData.lastUpdated
    ? new Date(templateData.lastUpdated).toLocaleString('en-GB', {
        dateStyle: 'medium',
        timeStyle: 'short',
      })
    : '—';

  // 4. Subject with fallback
  const defaultSubject = `Royal Gulf Shipping – Shipment Update (Ref: ${templateData.refId || '—'})`;

  const subject = settings.subject
    ? settings.subject
        .replace(/{order_number}/gi,   templateData.orderNumber   || '')
        .replace(/{consignment_id}/gi, templateData.consignmentId || '')
        .replace(/{status}/gi,         templateData.statusLabel   || '')
        .replace(/{ref_id}/gi,         templateData.refId         || '')
    : defaultSubject;

  // 5. Build HTML safely
  const receiverName   = escapeHtml(templateData.receiverName   || 'Valued Customer');
  const statusLabel    = escapeHtml(templateData.statusLabel    || 'Status Updated');
  const statusMsg      = escapeHtml(templateData.statusMsg      || 'We are working on your shipment.');
  const trackLink      = escapeHtml(templateData.trackLink      || 'https://royalgulfshipping.com/track-your-shipment/');
  const refId          = escapeHtml(templateData.refId          || '—');

  let itemsHtml = '<p style="color:#64748b;">No specific item details available.</p>';

  if (
    Array.isArray(templateData.updatedItems) &&
    templateData.updatedItems.length > 0
  ) {
    const rows = templateData.updatedItems
      .map(item => escapeHtml(item.itemRef || '—'))
      .map(ref => `<div style="margin-bottom:4px;">${ref}</div>`)
      .join('');

    itemsHtml = `
      <h3 style="margin:20px 0 10px; font-size:18px;">Your Updated Shipments</h3>
      <table class="info" role="presentation" style="width:100%; border-collapse:collapse;">
        <tr style="background:#f1f5f9;">
          <td style="padding:10px; font-weight:bold;">Item Ref</td>
          <td style="padding:10px;">${rows}</td>
        </tr>
        <!-- Add more rows if you have more fields (description, qty, etc.) -->
      </table>
    `;
  }

  const additionalContent = settings.additional_content
    ? `<div style="margin:20px 0; padding:16px; background:#f8f9fa; border-left:4px solid #faae56;">
         ${settings.additional_content.replace(/\n/g, '<br>')}
       </div>`
    : '';

  const html = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Royal Gulf Shipping – Shipment Update</title>
  <style>
    /* ── Paste your full original CSS here ── */
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height:1.5; color:#1e293b; background:#f8fafc; margin:0; }
    .wrap { max-width:600px; margin:0 auto; background:white; border-radius:8px; overflow:hidden; box-shadow:0 4px 12px rgba(0,0,0,0.08); }
    .logo-bar { padding:24px; text-align:center; background:#0f172a; }
    .logo { max-width:220px; height:auto; }
    .brand { text-align:center; padding:16px 24px; background:#1e293b; color:white; }
    .brand h1 { margin:0; font-size:22px; }
    .brand p { margin:4px 0 0; opacity:0.8; font-size:14px; }
    .accent { border:none; height:4px; background:linear-gradient(90deg, #68bb75, #faae56); margin:0; }
    .tag-row { padding:24px; }
    .tag { display:inline-block; padding:6px 12px; background:#68bb75; color:white; font-weight:600; border-radius:4px; margin-bottom:12px; }
    .title { margin:0 0 16px; color:#1e293b; font-size:24px; }
    .status { padding:16px; background:#f0fdf4; border-radius:8px; margin:16px 0; border:1px solid #bbf7d0; }
    .status .label { font-weight:700; color:#15803d; font-size:18px; margin-bottom:6px; }
    .cta { display:inline-block; margin:20px 0; padding:14px 32px; background:#68bb75; color:white; text-decoration:none; font-weight:600; border-radius:6px; }
    .cta:hover { background:#5ea66a; }
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
      <div class="tag">🔔 Shipment Update</div>
      <div class="content">
        <h2 class="title">${escapeHtml(settings.heading || 'Your shipment status has changed')}</h2>
        <p>Dear ${receiverName},</p>
        <p>We’re pleased to inform you that your shipment has a new update.</p>

        <div class="status">
          <div class="label">🚢 ${statusLabel}</div>
          <div class="msg">${statusMsg}</div>
        </div>

        <!-- Minimal progress indicator -->
        <div style="display:flex; justify-content:center; gap:10px; margin:16px 0 24px;">
          <div style="width:12px; height:12px; border-radius:50%; background:#68bb75;"></div>
          <div style="width:12px; height:12px; border-radius:50%; background:#faae56; box-shadow:0 0 0 4px rgba(250,174,86,0.3);"></div>
          <div style="width:12px; height:12px; border-radius:50%; background:#e5e7eb;"></div>
        </div>

        ${itemsHtml}

        ${additionalContent}

        <a class="cta" href="${trackLink}" target="_blank" rel="noopener noreferrer">
          View Live Tracking
        </a>

        <p class="muted" style="margin:16px 0;">
          If the button doesn’t work, visit:<br>
          <a href="https://royalgulfshipping.com/track-your-shipment/">https://royalgulfshipping.com/track-your-shipment/</a><br>
          and enter reference ID: <strong>${refId}</strong>
        </p>

        <p class="muted">
          You’re receiving this because you subscribed to shipment notifications.
        </p>
      </div>
    </div>

    <div class="foot">
      © ${currentYear} Royal Gulf Shipping & Logistics LLC — All rights reserved.<br>
      Need help? Call <a href="tel:+971555658321" style="color:#68bb75;">+971 555 658 321</a> or email
      <a href="mailto:sales@royalgulfshipping.com" style="color:#68bb75;">sales@royalgulfshipping.com</a>
    </div>
  </div>
</body>
</html>`;

  // ── Send ───────────────────────────────────────────────────────────────
  const mailOptions = {
    from: `"${process.env.EMAIL_FROM_NAME || 'Royal Gulf Shipping'}" <${process.env.EMAIL_FROM_ADDRESS || 'support@royalgulfshipping.com'}>`,
    to: toEmails.join(', '),
    subject,
    html,
    // Optional: add text fallback version later
    // text: `Dear ${receiverName}, ...`,
  };

  try {
    const info = await transporter.sendMail(mailOptions);
    console.log(`✅ ${notificationType} email sent to ${toEmails.length} recipient(s) → ${info.messageId}`);
    return { success: true, messageId: info.messageId };
  } catch (error) {
    console.error(`❌ Failed to send ${notificationType} email`, {
      to: toEmails,
      subject,
      error: error.message,
      code: error.code,
      response: error.response,
    });
    return { success: false, error: error.message };
  }
}
export { transporter, sendOrderEmail };
export default sendOrderEmail;
