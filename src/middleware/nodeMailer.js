import dotenv from 'dotenv';
import nodemailer from 'nodemailer';

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

// Verify on startup (optional, for debugging)
transporter.verify((error, success) => {
  if (error) {
    console.error('Email transporter verification failed:', error);
  } else {
    console.log('Email transporter ready!');
  }
});

// Main email-sending function (handles multiple recipients, uses your HTML templates)
async function sendOrderEmail(toEmails, subject, templateData) {
    console.log('Preparing to send email to:', toEmails);
  if (!Array.isArray(toEmails)) toEmails = [toEmails];  // Ensure array
  toEmails = toEmails.filter(email => email && email.includes('@'));  // Validate/filter

  if (toEmails.length === 0) {
    console.warn('No valid emails to send to.');
    return { success: false, message: 'No recipients' };
  }

  // Your shipment update template (from code)
  const shipmentUpdateTemplate = `<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <title>Royal Gulf Shipping – Shipment Update</title>
    <style>
      body{margin:0;background:#f8fafc;font-family:Arial,Helvetica,sans-serif;color:#334155;}
      a{text-decoration:none}
      .wrap{max-width:640px;margin:0 auto;background:#ffffff;border-radius:12px;box-shadow:0 8px 26px rgba(2,8,23,.06);overflow:hidden}
      .logo-bar{background:#ffffff;text-align:center;padding:18px 0}
      .logo{height:52px}
      .brand{padding:0 22px 14px;text-align:center}
      .brand h1{margin:0;font-size:18px;font-weight:800;color:#0f172a;letter-spacing:.2px}
      .brand p{margin:4px 0 0;font-size:12px;color:#64748b}
      .accent{height:4px;background:#faae56;opacity:.9;border:0}
      .tag-row{position:relative}
      .tag{position:absolute;right:18px;top:18px;background:#fff7ed;border:1px solid #facc15;
           color:#9a6700;font-weight:700;font-size:12px;padding:6px 10px;border-radius:999px;float: right;
      margin: 22px 20px 0px 0px;}
      .content{padding:24px 22px}
      h2.title{margin:0 0 10px;font-size:20px;font-weight:800;color:#0f172a}
      p{margin:0 0 10px;line-height:1.55}
      .status{background:#ecfdf5;border-left:5px solid #f97316;border-radius:10px;padding:14px 16px;margin:16px 0}
      .status .label{font-weight:800;color:#0f172a;font-size:16px}
      .status .msg{margin-top:6px;color:#475569;font-size:14px;white-space:pre-line}
      table.info{width:100%;border-collapse:collapse;margin-top:14px}
      .info td{padding:7px 0;font-size:13px;border-bottom:1px dashed #e5e7eb}
      .info td:first-child{width:160px;color:#64748b;text-transform:uppercase;font-size:11px;letter-spacing:.4px}
      .cta{display:inline-block;margin:22px 0 6px;background:linear-gradient(135deg,#faae56,#68bb75);
           color:#ffffff;font-weight:800;padding:12px 22px;border-radius:999px;text-align:center;text-decoration:none}
      .foot{background:#f1f5f9;padding:18px;text-align:center;font-size:12px;color:#64748b;line-height:1.6}
      .muted{font-size:12px;color:#94a3b8;margin-top:14px}
      @media (max-width:480px){
        .content{padding:18px}
        .tag{position:static;display:inline-block;margin:12px auto 0}
        .cta{display:block;width:100%;text-align:center;box-sizing:border-box}
      }
    </style>
  </head>
  <body style="padding:16px">
    <div class="wrap">
      <!-- Header -->
      <div class="logo-bar">
      <img class="logo" src="https://royalgulfshipping.com/wp-content/uploads/2025/09/royalgulflogo-1.jpeg" alt="Royal Gulf Shipping Logo">
      </div>
      <div class="brand">
        <h1>Royal Gulf Shipping &amp; Logistics LLC</h1>
        <p>Dubai • London • Karachi • Shenzhen</p>
      </div>
      <hr class="accent">

      <!-- Tag + Content -->
      <div class="tag-row">
        <div class="tag">&#128276; Shipment Update</div> <!-- 🔔 -->
        <div class="content">
          <h2 class="title">Your shipment status has changed</h2>
          <p>Dear Customer,</p>
          <p>We’re pleased to inform you that your shipment has a new update.</p>

          <!-- Status block -->
          <div class="status">
            <div class="label">&#128674; ${templateData.statusLabel}</div> <!-- 🚢 -->
            <div class="msg">${templateData.statusMsg}</div>
          </div>

          <!-- Progress mini-bar (static for now) -->
          <div style="display:flex;justify-content:center;gap:8px;margin:8px 0 14px">
            <div style="width:10px;height:10px;border-radius:50%;background:#68bb75"></div>
            <div style="width:10px;height:10px;border-radius:50%;background:#faae56;box-shadow:0 0 0 4px rgba(250,174,86,.3)"></div>
            <div style="width:10px;height:10px;border-radius:50%;background:#e5e7eb"></div>
          </div>

          <!-- Shipment details -->
          <table class="info" role="presentation">
            <tr><td>Ref ID</td><td><strong>${templateData.refId}</strong></td></tr>
            <tr><td>Order ID</td><td>${templateData.orderId}</td></tr>
            <tr><td>Route</td><td>${templateData.route}</td></tr>
            <tr><td>ETA</td><td>${templateData.etaFormatted}</td></tr>
            <tr><td>Last Updated</td><td>${templateData.lastUpdated}</td></tr>
          </table>

          <!-- CTA Button -->
          <a class="cta" href="${templateData.trackLink}" target="_blank" rel="noopener noreferrer">View Live Tracking</a>
          <p class="muted" style="font-size:10px;">If the button above is not working, please visit the following link: https://royalgulfshipping.com/track-your-shipment/ and paste your reference ID to track your shipment.</p>
          <p class="muted">You’re receiving this because you subscribed to shipment notifications on our website.</p>
        </div>
      </div>

      <!-- Footer -->
      <div class="foot">
        © 2025 Royal Gulf Shipping &amp; Logistics LLC — All rights reserved.<br>
        Need help? Call +971 555 658 321 or email
        <a href="mailto:sales@royalgulfshipping.com" style="color:#68bb75">sales@royalgulfshipping.com</a>
      </div>
    </div>
  </body>
</html>`;

  // Your subscription confirmation template (from code)
  const subscriptionTemplate = `<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <title>Royal Gulf Shipping – Subscription Confirmed</title>
    <style>
      body{margin:0;background:#f8fafc;font-family:Arial,Helvetica,sans-serif;color:#334155;}
      a{text-decoration:none}
      .wrap{max-width:640px;margin:0 auto;background:#ffffff;border-radius:12px;box-shadow:0 8px 26px rgba(2,8,23,.06);overflow:hidden}
      .logo-bar{background:#ffffff;text-align:center;padding:18px 0}
      .logo{height:52px}
      .brand{padding:0 22px 14px;text-align:center}
      .brand h1{margin:0;font-size:18px;font-weight:800;color:#0f172a;letter-spacing:.2px}
      .brand p{margin:4px 0 0;font-size:12px;color:#64748b}
      .accent{height:4px;background:#faae56;opacity:.9;border:0}
      .content{padding:24px 22px}
      h2.title{margin:0 0 10px;font-size:20px;font-weight:800;color:#0f172a}
      p{margin:0 0 10px;line-height:1.55}
      .status{background:#ecfdf5;border-left:5px solid #10b981;border-radius:10px;padding:14px 16px;margin:16px 0}
      .status .label{font-weight:800;color:#0f172a;font-size:16px}
      .status .msg{margin-top:6px;color:#475569;font-size:14px;white-space:pre-line}
      table.info{width:100%;border-collapse:collapse;margin-top:14px}
      .info td{padding:7px 0;font-size:13px;border-bottom:1px dashed #e5e7eb}
      .info td:first-child{width:160px;color:#64748b;text-transform:uppercase;font-size:11px;letter-spacing:.4px}
      .cta{display:inline-block;margin:22px 0 6px;background:linear-gradient(135deg,#faae56,#68bb75);
           color:#ffffff;font-weight:800;padding:12px 22px;border-radius:999px;text-align:center;text-decoration:none}
      .foot{background:#f1f5f9;padding:18px;text-align:center;font-size:12px;color:#64748b;line-height:1.6}
      .muted{font-size:12px;color:#94a3b8;margin-top:14px}
      @media (max-width:480px){
        .content{padding:18px}
        .cta{display:block;width:100%;text-align:center;box-sizing:border-box}
      }
    </style>
  </head>
  <body style="padding:16px">
    <div class="wrap">
      <!-- Header -->
      <div class="logo-bar">
        <img class="logo" src="https://royalgulfshipping.com/wp-content/uploads/2025/09/royalgulflogo-1.jpeg" alt="Royal Gulf Shipping Logo">
      </div>
      <div class="brand">
        <h1>Royal Gulf Shipping &amp; Logistics LLC</h1>
        <p>Dubai • London • Karachi • Shenzhen</p>
      </div>
      <hr class="accent">

      <!-- Content -->
      <div class="content">
        <h2 class="title">Subscription Confirmed</h2>
        <p>Dear Customer,</p>
        <p>You’ve successfully subscribed to updates for your shipment.</p>
        <p>You’ll receive notifications whenever there’s a status change.</p>

        <!-- Status block -->
        <div class="status">
          <div class="label">&#128276; Current Status: ${templateData.phaseLabel}</div>
          <div class="msg">${templateData.phaseMsg}</div>
        </div>

        <!-- Shipment details -->
        <table class="info" role="presentation">
          <tr><td>Ref ID</td><td><strong>${templateData.referenceId}</strong></td></tr>
          <tr><td>Route</td><td>${templateData.route}</td></tr>
          <tr><td>ETA</td><td>${templateData.etaFormatted}</td></tr>
        </table>

        <!-- CTA Button -->
        <a class="cta" href="${templateData.trackLink}" target="_blank" rel="noopener noreferrer">View Live Tracking</a>
        <p class="muted">You can unsubscribe anytime by contacting support.</p>
      </div>

      <!-- Footer -->
      <div class="foot">
        © 2025 Royal Gulf Shipping &amp; Logistics LLC — All rights reserved.<br>
        Need help? Call +971 555 658 321 or email
        <a href="mailto:sales@royalgulfshipping.com" style="color:#68bb75">sales@royalgulfshipping.com</a>
      </div>
    </div>
  </body>
</html>`;

  // Select template based on data type (shipment or subscription)
  let html;
  if (templateData.type === 'subscription') {
    html = subscriptionTemplate.replace('${templateData.phaseLabel}', templateData.phaseLabel || '')
                               .replace('${templateData.phaseMsg}', templateData.phaseMsg || '')
                               .replace('${templateData.referenceId}', templateData.referenceId || '')
                               .replace('${templateData.route}', templateData.route || '')
                               .replace('${templateData.etaFormatted}', templateData.etaFormatted || '')
                               .replace('${templateData.trackLink}', templateData.trackLink || '');
  } else {
    // Default to shipment update
    html = shipmentUpdateTemplate.replace('${templateData.statusLabel}', templateData.statusLabel || '')
                                 .replace('${templateData.statusMsg}', templateData.statusMsg || '')
                                 .replace('${templateData.refId}', templateData.refId || '')
                                 .replace('${templateData.orderId}', templateData.orderId || '')
                                 .replace('${templateData.route}', templateData.route || '')
                                 .replace('${templateData.etaFormatted}', templateData.etaFormatted || '')
                                 .replace('${templateData.lastUpdated}', templateData.lastUpdated || '')
                                 .replace('${templateData.trackLink}', templateData.trackLink || '');
  }

  const mailOptions = {
    from: `"${process.env.EMAIL_FROM_NAME}" <${process.env.EMAIL_FROM_ADDRESS}>`,
    to: toEmails.join(', '),  // Comma-separated for multiple
    subject: subject,
    html: html
  };

  try {
    const info = await transporter.sendMail(mailOptions);
    console.log(`Email sent successfully to ${toEmails.length} recipients: ${info.messageId}`);
    return { success: true, messageId: info.messageId };
  } catch (error) {
    console.error('Email send failed:', error.message);
    return { success: false, error: error.message };
  }
}

export { transporter, sendOrderEmail };
export default sendOrderEmail;