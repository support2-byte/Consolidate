import { escapeHtml } from "./escapeHtml.js";
import { renderEmail } from "./renderEmail.js";

export const buildInvoiceEmailHtml = ({
  recipientName,
  invoiceId,
  itemRef,
  amount,
  invoiceLink,
}) =>
  renderEmail("invoice_email", {
    recipientName: escapeHtml(recipientName?.trim() || "Valued Customer"),
    invoiceId: escapeHtml(invoiceId || "—"),
    itemRef: escapeHtml(itemRef || "—"),
    amount: amount ? `AED ${escapeHtml(amount)}` : "—",
    invoiceLink: escapeHtml(invoiceLink || "#"),
    lastUpdated: new Intl.DateTimeFormat("en-GB", {
      timeZone: "Asia/Karachi",
      day: "2-digit",
      month: "2-digit",
      year: "numeric",
    }).format(new Date()),
  });
