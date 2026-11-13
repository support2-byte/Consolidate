import { transporter } from "../middleware/nodeMailer.js";
import { buildKycEmailHtml } from "./kycEmailTemplate.js";
import { buildKycEmailHtmlCas } from "./kycEmailTemplateCas.js";
import { buildKycEmailHtmlMf } from "./kycEmailTemplateMf.js";
import logger from "./logger.js";

const KYC_BUILDERS = {
  MF: buildKycEmailHtmlMf,
  CAS: buildKycEmailHtmlCas,
};

export const sendKycFormEmail = async ({
  recipientEmail,
  recipientName,
  formUrl,
  company,
}) => {
  try {
    const build = KYC_BUILDERS[company] || buildKycEmailHtml; // default: RGSL
    const emailTemplate = build({ recipientName, formUrl });

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
