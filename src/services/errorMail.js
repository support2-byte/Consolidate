import { v2 as cloudinary } from "cloudinary";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { transporter } from "../middleware/nodeMailer.js";
import logger from "./logger.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
  api_key: process.env.CLOUDINARY_API_KEY,
  api_secret: process.env.CLOUDINARY_API_SECRET,
});

const safe = (str = "") =>
  String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");

function buildErrorHtml({
  apiName,
  errorMessage,
  errorStack,
  requestBody,
  timestamp,
  filename,
  fileUrl,
}) {
  const date = new Date(timestamp).toLocaleString();
  const hasFile = Boolean(filename && fileUrl);

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Error Notification · RGSL</title>
<style>
  body { font-family: 'Segoe UI', Arial, sans-serif; background: #f4f7fc; margin: 0; padding: 40px 20px; color: #1f2937; }
  .card { max-width: 700px; margin: 0 auto; background: #ffffff; border-radius: 24px; overflow: hidden; box-shadow: 0 12px 40px rgba(9, 125, 118, 0.15), 0 4px 12px rgba(0,0,0,0.05); border: 1px solid #eef2f6; }
  .header { background: linear-gradient(135deg, #097D76 0%, #06655f 100%); color: white; padding: 32px; text-align: center; border-bottom: 4px solid #F38120; }
  .logo-area { display: block; text-align: center; margin-bottom: 20px; }
  .logo-plate { display: inline-block; background: #ffffff; border-radius: 12px; padding: 12px 20px; }
  .logo-img { height: 48px; display: block; }
  .header-title { font-size: 24px; font-weight: 700; margin: 0; }
  .header-subtitle { font-size: 13px; margin: 8px 0 0; color: rgba(255,255,255,0.85); }
  .content { padding: 36px 32px; line-height: 1.6; font-size: 15px; color: #334155; }
  .body-text { margin-bottom: 24px; font-weight: 600; color: #475569; }
  .detail-table { width: 100%; border-collapse: collapse; border: 1px solid #e2e8f0; border-radius: 12px; overflow: hidden; }
  .detail-table td { padding: 12px 16px; border-bottom: 1px solid #e2e8f0; }
  .detail-label { width: 140px; background: #f8fafc; font-weight: 600; color: #097D76; vertical-align: top; }
  .error-value { color: #b91c1c; font-weight: 600; }
  .pre-box { margin: 0; white-space: pre-wrap; word-break: break-word; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; padding: 10px; font-size: 12px; line-height: 1.5; color: #334155; max-height: 280px; overflow: auto; }
  .info-box { background: #f8fafc; border: 1px solid #e2e8f0; border-left: 4px solid #F38120; border-radius: 12px; padding: 20px; margin-top: 28px; }
  .info-title { font-size: 15px; font-weight: 600; color: #097D76; margin-bottom: 12px; }
  .btn-wrapper { text-align: center; margin: 28px 0 0; }
  .btn { display: inline-block; background: #F38120; color: #ffffff !important; text-decoration: none; padding: 12px 30px; border-radius: 40px; font-weight: 600; font-size: 14px; }
  .footer { background: #0f172a; color: white; padding: 32px; text-align: center; border-top: 1px solid #e2e8f0; }
  .footer-logo { height: 36px; margin-bottom: 16px; opacity: 0.8; }
  .footer-text { font-size: 13px; color: #94a3b8; line-height: 1.5; margin-bottom: 16px; }
  .footer-divider { height: 1px; background: #334155; margin: 20px 0; }
  .footer-copyright { font-size: 12px; color: #64748b; }
  @media (max-width: 640px) {
    body { padding: 16px; }
    .card { border-radius: 16px; }
    .header { padding: 24px 20px; }
    .content { padding: 24px 20px; }
    .footer { padding: 24px 20px; }
    .header-title { font-size: 20px; }
  }
</style>
</head>
<body>
<div class="card">
  <div class="header">
    <div class="logo-area">
      <div class="logo-plate">
        <img src="https://royalgulfshipping.com/wp-content/uploads/2023/08/RGSL-LOGO.png" alt="Royal Gulf Shipping & Logistics" class="logo-img">
      </div>
    </div>
    <h2 class="header-title">Error Log Notification</h2>
    <p class="header-subtitle">${safe(date)}</p>
  </div>
  <div class="content">
    <p class="body-text">An error occurred and has been logged${hasFile ? " (log file attached below)" : ""}.</p>

    <table class="detail-table" cellpadding="0" cellspacing="0">
      <tr>
        <td class="detail-label">API</td>
        <td>${safe(apiName)}</td>
      </tr>
      <tr>
        <td class="detail-label">Error Message</td>
        <td class="error-value">${safe(errorMessage)}</td>
      </tr>
      <tr>
        <td class="detail-label">Stack Trace</td>
        <td><pre class="pre-box">${safe(errorStack)}</pre></td>
      </tr>
      <tr>
        <td class="detail-label" style="border-bottom:none;">Request Body</td>
        <td style="border-bottom:none;"><pre class="pre-box" style="max-height:200px;">${safe(requestBody)}</pre></td>
      </tr>
    </table>

    ${
      hasFile
        ? `
    <div class="info-box">
      <div class="info-title">Log File</div>
      <p style="margin:0 0 12px;color:#475569;font-size:14px;">${safe(filename)}</p>
      <div class="btn-wrapper" style="margin-top:0;">
        <a class="btn" href="${fileUrl}">Download Log File</a>
      </div>
    </div>`
        : `
    <div class="info-box">
      <div class="info-title" style="margin-bottom:0;">No log file was generated for this error.</div>
    </div>`
    }
  </div>
  <div class="footer">
    <img src="https://royalgulfshipping.com/wp-content/uploads/2023/08/RGSL-LOGO-white.png" alt="RGSL Logo" class="footer-logo">
    <div class="footer-text">
      Royal Gulf Shipping &amp; Logistics LLC
      <br>
      Automated system notification.
    </div>
    <div class="footer-divider"></div>
    <div class="footer-copyright">
      © ${new Date().getFullYear()} Royal Gulf Shipping LLC. All rights reserved.
      <br>
      <span style="color: #64748b; font-size: 11px; margin-top: 4px; display: block;">This is an automated notification. Please do not reply directly to this email.</span>
    </div>
  </div>
</div>
</body>
</html>`;
}

function buildErrorText({
  apiName,
  errorMessage,
  errorStack,
  requestBody,
  timestamp,
  filename,
  fileUrl,
}) {
  const date = new Date(timestamp).toLocaleString();
  const hasFile = Boolean(filename && fileUrl);

  return [
    "ERROR NOTIFICATION",
    "──────────────────────────────",
    `Time: ${date}`,
    `API: ${apiName}`,
    "",
    `MESSAGE: ${errorMessage}`,
    "",
    "STACK TRACE:",
    errorStack,
    "",
    "REQUEST BODY:",
    requestBody,
    "",
    hasFile
      ? `LOG FILE: ${filename}\nDOWNLOAD: ${fileUrl}`
      : "LOG FILE: none generated for this error.",
    "",
    "──────────────────────────────",
    "This email was generated automatically.",
  ].join("\n");
}

const getRecipients = () =>
  process.env.NOTIFICATION_RECIPIENTS
    ? process.env.NOTIFICATION_RECIPIENTS.split(",").map((e) => e.trim())
    : [process.env.GMAIL_USER, "saadsaifullah.rgsl@gmail.com"];

const sendToMultipleRecipients = async (subject, html, text) => {
  const recipients = getRecipients().filter(Boolean);

  if (recipients.length === 0) {
    return { success: false, error: "No recipients configured" };
  }

  try {
    const info = await transporter.sendMail({
      to: recipients,
      subject,
      text,
      html,
      headers: {
        "X-Priority": "1",
        "X-MSMail-Priority": "High",
      },
    });
    logger.info("Error notification email sent", {
      recipients,
      messageId: info.messageId,
    });
    return {
      total: recipients.length,
      successful: recipients.length,
      failed: 0,
      failedRecipients: [],
      messageIds: [info.messageId],
    };
  } catch (error) {
    logger.error("Failed to send error notification email", {
      recipients,
      error: error.message,
    });
    return {
      total: recipients.length,
      successful: 0,
      failed: recipients.length,
      failedRecipients: recipients,
      messageIds: [],
    };
  }
};

const uploadLogToCloudinary = async (filePath, filename) => {
  const result = await cloudinary.uploader.upload(filePath, {
    folder: "consolidate-app/error-logs",
    resource_type: "raw",
    public_id: filename.replace(/\.log$/, ""),
  });
  return result.secure_url;
};

export const logErrorAndNotify = async (apiName, error, req, options = {}) => {
  const { writeFile = false } = options;

  const timestamp = new Date().toISOString();
  const errorMessage = error.message || String(error);
  const errorStack = error.stack || errorMessage;
  const requestBody = JSON.stringify(req?.body ?? {}, null, 2);

  if (!writeFile) {
    const html = buildErrorHtml({
      apiName,
      errorMessage,
      errorStack,
      requestBody,
      timestamp,
      filename: null,
      fileUrl: null,
    });
    const text = buildErrorText({
      apiName,
      errorMessage,
      errorStack,
      requestBody,
      timestamp,
      filename: null,
      fileUrl: null,
    });

    return sendToMultipleRecipients(
      `RGSL - Consolidate Error Mail`,
      html,
      text,
    );
  }

  let filePath;
  try {
    const safeTimestamp = timestamp.replace(/:/g, "-");
    const filename = `${apiName}_error_${safeTimestamp}.log`;
    const logsDir = path.join(__dirname, "..", "tmp_logs");

    if (!fs.existsSync(logsDir)) {
      fs.mkdirSync(logsDir, { recursive: true });
    }

    filePath = path.join(logsDir, filename);

    fs.writeFileSync(
      filePath,
      `[${timestamp}]\nAPI: ${apiName}\nError: ${errorStack}\nRequest Body: ${requestBody}\n`,
    );

    const fileUrl = await uploadLogToCloudinary(filePath, filename);

    const html = buildErrorHtml({
      apiName,
      errorMessage,
      errorStack,
      requestBody,
      timestamp,
      filename,
      fileUrl,
    });
    const text = buildErrorText({
      apiName,
      errorMessage,
      errorStack,
      requestBody,
      timestamp,
      filename,
      fileUrl,
    });

    return await sendToMultipleRecipients(
      `RGSL - Consolidate Error Mail`,
      html,
      text,
    );
  } catch (err) {
    logger.error(
      "Failed to upload log to Cloudinary, falling back to no-file email",
      {
        apiName,
        error: err.message,
      },
    );
    return logErrorAndNotify(apiName, error, req, { writeFile: false });
  } finally {
    if (filePath && fs.existsSync(filePath)) {
      try {
        fs.unlinkSync(filePath);
      } catch (unlinkError) {
        logger.error("Failed to delete local log file", {
          filePath,
          error: unlinkError.message,
        });
      }
    }
  }
};
