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

  return `
  <!DOCTYPE html>
  <html>
  <body style="margin:0;padding:0;font-family:Segoe UI, Arial, sans-serif;color:#334155;">
    <table width="100%" cellpadding="0" cellspacing="0">
      <tr>
        <td align="center" style="padding:32px 16px;">
          <table width="100%" cellpadding="0" cellspacing="0" style="
            max-width:700px;
            background:#ffffff;
            border:1px solid #e2e8f0;
            border-radius:10px;
            overflow:hidden;
          ">

            <!-- Header -->
            <tr>
              <td style="background:#0f766e;padding:20px 24px;">
                <table width="100%">
                  <tr>
                    <td>
                      <h2 style="margin:0;color:#ffffff;font-size:20px;font-weight:600;">
                        RGSL - Error Log Notification
                      </h2>
                    </td>
                    <td align="right">
                      <span style="color:rgba(255,255,255,0.85);font-size:12px;">
                        ${safe(date)}
                      </span>
                    </td>
                  </tr>
                </table>
              </td>
            </tr>

            <!-- Body -->
            <tr>
              <td style="padding:28px 24px;">

                <p style="margin:0 0 24px;color:#475569;line-height:1.6;font-weight:600;">
                  An error occurred and has been logged${hasFile ? " (log file attached below)" : ""}.
                </p>

                <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;border:1px solid #e2e8f0;">
                  <tr>
                    <td style="width:140px;padding:12px 16px;background:#f8fafc;font-weight:600;border-bottom:1px solid #e2e8f0;">
                      API
                    </td>
                    <td style="padding:12px 16px;border-bottom:1px solid #e2e8f0;">
                      ${safe(apiName)}
                    </td>
                  </tr>

                  <tr>
                    <td style="padding:12px 16px;background:#f8fafc;font-weight:600;vertical-align:top;border-bottom:1px solid #e2e8f0;">
                      Error Message
                    </td>
                    <td style="padding:12px 16px;border-bottom:1px solid #e2e8f0;color:#b91c1c;font-weight:600;">
                      ${safe(errorMessage)}
                    </td>
                  </tr>

                  <tr>
                    <td style="padding:12px 16px;background:#f8fafc;font-weight:600;vertical-align:top;border-bottom:1px solid #e2e8f0;">
                      Stack Trace
                    </td>
                    <td style="padding:12px 16px;border-bottom:1px solid #e2e8f0;">
                      <pre style="
                        margin:0;
                        white-space:pre-wrap;
                        word-break:break-word;
                        background:#f8fafc;
                        border:1px solid #e2e8f0;
                        border-radius:6px;
                        padding:10px;
                        font-size:12px;
                        line-height:1.5;
                        color:#334155;
                        max-height:280px;
                        overflow:auto;
                      ">${safe(errorStack)}</pre>
                    </td>
                  </tr>

                  <tr>
                    <td style="padding:12px 16px;background:#f8fafc;font-weight:600;vertical-align:top;">
                      Request Body
                    </td>
                    <td style="padding:12px 16px;">
                      <pre style="
                        margin:0;
                        white-space:pre-wrap;
                        word-break:break-word;
                        background:#f8fafc;
                        border:1px solid #e2e8f0;
                        border-radius:6px;
                        padding:10px;
                        font-size:12px;
                        line-height:1.5;
                        color:#334155;
                        max-height:200px;
                        overflow:auto;
                      ">${safe(requestBody)}</pre>
                    </td>
                  </tr>
                </table>

                ${
                  hasFile
                    ? `
                <table width="100%" cellpadding="0" cellspacing="0" style="margin-top:24px;">
                  <tr>
                    <td>
                      <p style="margin:0 0 8px;color:#475569;font-size:13px;">
                        <strong>Log file:</strong> ${safe(filename)}
                      </p>
                      <a href="${fileUrl}"
                         style="display:inline-block;margin:6px 0;padding:10px 18px;
                                background-color:#f58220;color:#ffffff;text-decoration:none;
                                border-radius:6px;font-weight:600;font-size:13px;">
                        Download Log File
                      </a>
                    </td>
                  </tr>
                </table>`
                    : `
                <table width="100%" cellpadding="0" cellspacing="0" style="margin-top:24px;">
                  <tr>
                    <td style="font-size:13px;color:#64748b;">
                      No log file was generated for this error.
                    </td>
                  </tr>
                </table>`
                }

              </td>
            </tr>

            <!-- Footer -->
            <tr>
              <td style="background:#f58220;padding:14px 24px;">
                <p style="margin:0;text-align:center;color:#ffffff;font-size:12px;">
                  This is an automated notification from the application.
                </p>
              </td>
            </tr>

          </table>
        </td>
      </tr>
    </table>
  </body>
  </html>
  `;
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

  const emailPromises = recipients.map(async (recipient) => {
    try {
      const info = await transporter.sendMail({
        to: recipient,
        subject,
        text,
        html,
        headers: {
          "X-Priority": "1",
          "X-MSMail-Priority": "High",
        },
      });
      logger.info("Error notification email sent", {
        recipient,
        messageId: info.messageId,
      });
      return { success: true, recipient, messageId: info.messageId };
    } catch (error) {
      logger.error("Failed to send error notification email", {
        recipient,
        error: error.message,
      });
      return { success: false, recipient, error: error.message };
    }
  });

  const results = await Promise.all(emailPromises);
  const successful = results.filter((r) => r.success);
  const failed = results.filter((r) => !r.success);

  return {
    total: recipients.length,
    successful: successful.length,
    failed: failed.length,
    failedRecipients: failed.map((f) => f.recipient),
    messageIds: successful.map((s) => s.messageId),
  };
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
