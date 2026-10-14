import { transporter } from "../middleware/nodeMailer.js";
import { buildConfirmationEmailHtml } from "./confirmationEmailTemplate.js";
import logger from "./logger.js";

export const sendConfirmationEmail = async ({
  recipientEmail,
  recipientName,
  senderName,
  companyName,
  companyLogo,
  mode,
  totalQty,
  totalWeight,
  lastUpdated,
  viewLink,
  ccEmails,
  bccEmails,
  otp,
  items,
}) => {
  try {
    const emailTemplate = buildConfirmationEmailHtml({
      recipientName,
      senderName,
      companyName,
      companyLogo,
      mode,
      totalQty,
      totalWeight,
      lastUpdated,
      viewLink,
      otp,
      items,
    });

    await transporter.sendMail({
      from: "Royal Gulf Shipping & Logistics",
      to: recipientEmail,
      cc: ccEmails || undefined,
      bcc: bccEmails || undefined,
      subject: "Your Booking Confirmation",
      html: emailTemplate,
    });

    return { success: true };
  } catch (error) {
    logger.error("Failed to send confirmation email", {
      recipientEmail,
      error: error.message,
    });
    return { success: false, error: error.message };
  }
};
