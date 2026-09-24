import { transporter } from "../middleware/nodeMailer.js";
import { buildKycEmailHtml } from "./kycEmailTemplate.js";
import { buildKycEmailHtmlCas } from "./kycEmailTemplateCas.js";
import { buildKycEmailHtmlMf } from "./kycEmailTemplateMf.js";
import logger from "./logger.js";

export const sendKycFormEmail = async ({
  recipientEmail,
  recipientName,
  formUrl,
  company,
}) => {
  try {
    let emailTemplate;

    if (company === "MF") {
      emailTemplate = buildKycEmailHtmlMf({ recipientName, formUrl });
    } else if (company === "CAS") {
      emailTemplate = buildKycEmailHtmlCas({ recipientName, formUrl });
    } else {
      emailTemplate = buildKycEmailHtml({ recipientName, formUrl });
    }

    await transporter.sendMail({
      from: "Royal Gulf Shipping & Logistics",
      to: recipientEmail,
      subject: "Action Required: Complete Your KYC Verification",
      html: emailTemplate,
    });

    return { success: true };
  } catch (error) {
    logger.error("Failed to send KYC email", {
      recipientEmail,
      error: error.message,
    });
    return { success: false, error: error.message };
  }
};
