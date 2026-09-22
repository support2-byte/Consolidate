import { transporter } from "../middleware/nodeMailer.js";
import { buildInvoiceEmailHtml } from "./buildInvoiceEmailHtml.js";
import logger from "./logger.js";

export const sendInvoiceEmail = async ({
  email,
  recipientId,
  itemRef,
  receiverName,
  invoiceId,
  amount,
  invoiceLink,
  dueDate,
  otp,
}) => {
  try {
    const html = buildInvoiceEmailHtml({
      recipientName: receiverName,
      itemRef,
      invoiceId,
      amount,
      invoiceLink,
      dueDate,
      otp,
    });

    await transporter.sendMail({
      from: `Royal Gulf Shipping & Logistics`,
      to: email,
      subject: `Invoice ${invoiceId} — Royal Gulf Shipping & Logistics`,
      html,
    });

    return { success: true };
  } catch (error) {
    logger.error("Failed to send invoice email", {
      email,
      error: error.message,
    });
    return { success: false, error: error.message };
  }
};
