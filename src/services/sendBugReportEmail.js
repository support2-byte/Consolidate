import { transporter } from "../middleware/nodeMailer.js";
import logger from "./logger.js";

function buildHtml({ title, description, submittedAt }) {
  const date = new Date(submittedAt).toLocaleString();

  const safe = (str) =>
    String(str).replace(/&/g, "&").replace(/</g, "<").replace(/>/g, ">");

  return `

  <!DOCTYPE html>

  <html>
  <body style="
    margin:0;
    padding:0;
    font-family:Segoe UI, Arial, sans-serif;
    color:#334155;
  ">

<table width="100%" cellpadding="0" cellspacing="0">
  <tr>
    <td align="center" style="padding:32px 16px;">

      <table
        width="100%"
        cellpadding="0"
        cellspacing="0"
        style="
          max-width:700px;
          background:#ffffff;
          border:1px solid #e2e8f0;
          border-radius:10px;
          overflow:hidden;
        "
      >

        <!-- Header -->
        <tr>
          <td style="
            background:#0f766e;
            padding:20px 24px;
          ">
            <table width="100%">
              <tr>
                <td>
                  <h2 style="
                    margin:0;
                    color:#ffffff;
                    font-size:20px;
                    font-weight:600;
                  ">
                    RGSL - IT Department
                  </h2>
                </td>

                <td align="right">
                  <span style="
                    color:rgba(255,255,255,0.85);
                    font-size:12px;
                  ">
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

            <p style="
              margin:0 0 24px;
              color:#475569;
              line-height:1.6;
              font-weight:600;
            ">
              A new bug report has been submitted and requires review.
            </p>

            <table
              width="100%"
              cellpadding="0"
              cellspacing="0"
              style="
                border-collapse:collapse;
                border:1px solid #e2e8f0;
              "
            >

              <tr>
                <td style="
                  width:140px;
                  padding:12px 16px;
                  background:#f8fafc;
                  font-weight:600;
                  border-bottom:1px solid #e2e8f0;
                ">
                  Title
                </td>

                <td style="
                  padding:12px 16px;
                  border-bottom:1px solid #e2e8f0;
                ">
                  ${safe(title)}
                </td>
              </tr>

              <tr>
                <td style="
                  padding:12px 16px;
                  background:#f8fafc;
                  font-weight:600;
                  vertical-align:top;
                ">
                  Description
                </td>

                <td style="
                  padding:12px 16px;
                ">
                  ${safe(description)}
                </td>
              </tr>

            </table>

            <table
              width="100%"
              cellpadding="0"
              cellspacing="0"
              style="margin-top:24px;"
            >
              <tr>
                <td style="
                  font-size:13px;
                  color:#64748b;
                ">
                  Submitted via Bug Report Form
                </td>
              </tr>
            </table>

          </td>
        </tr>

        <!-- Footer -->
        <tr>
          <td style="
            background:#f58220;
            padding:14px 24px;
          ">
            <p style="
              margin:0;
              text-align:center;
              color:#ffffff;
              font-size:12px;
            ">
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

function buildText({ title, description, submittedAt }) {
  const date = new Date(submittedAt).toLocaleString();
  return [
    "BUG REPORT",
    "──────────────────────────────",
    `Submitted: ${date}`,
    "",
    `TITLE: ${title}`,
    "",
    "DESCRIPTION:",
    description,
    "",
    "──────────────────────────────",
    "This email was generated automatically.",
  ].join("\n");
}

export async function sendBugReportEmail({ title, description, submittedAt }) {
  const recipients = [process.env.GMAIL_USER, "saadsaifullah.rgsl@gmail.com"];

  const mailOptions = {
    to: recipients.join(", "),
    subject: `Bug Report: ${title}`,
    text: buildText({ title, description, submittedAt }),
    html: buildHtml({ title, description, submittedAt }),
  };

  const info = await transporter.sendMail(mailOptions);

  logger.info("Bug report email sent", {
    messageId: info.messageId,
    title,
    recipients,
  });

  return info;
}
