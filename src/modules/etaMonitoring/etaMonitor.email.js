import { transporter } from "../../middleware/nodeMailer.js";
import logger from "../../services/logger.js";
import {
  buildEtaStaleEmailHtml,
  groupStaleItems,
} from "./etaMonitor.email.template.js";

const getRecipients = () =>
  (process.env.ETA_NOTIFICATION_EMAILS || "")
    .split(",")
    .map((e) => e.trim())
    .filter(Boolean);

export const sendStaleItemsEmail = async (allItems) => {
  const recipients = getRecipients();
  if (!recipients.length) {
    logger.warn("ETA stale email skipped: ETA_NOTIFICATION_EMAILS is empty");
    return { sent: false, count: allItems.length };
  }

  const orderCount = groupStaleItems(allItems).length;
  const html = buildEtaStaleEmailHtml({
    items: allItems,
    adminUrl: process.env.ETA_ADMIN_PAGE_URL || "",
  });

  await transporter.sendMail({
    from: process.env.GMAIL_FROM_ADDRESS || process.env.GMAIL_USER,
    to: recipients,
    subject: `ETA Monitoring: ${allItems.length} stale item${allItems.length > 1 ? "s" : ""} across ${orderCount} order${orderCount > 1 ? "s" : ""}`,
    html,
  });

  logger.info("ETA stale email sent", {
    recipients: recipients.length,
    items: allItems.length,
  });
  return { sent: true, count: allItems.length };
};
