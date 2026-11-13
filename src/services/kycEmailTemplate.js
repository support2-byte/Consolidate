import { escapeHtml } from "./escapeHtml.js";
import { renderEmail } from "./renderEmail.js";

export const buildKycEmailHtml = ({ recipientName, formUrl }) =>
  renderEmail("kyc_rgsl", {
    recipientName: escapeHtml(recipientName?.trim() || "Valued Customer"),
    formUrl: escapeHtml(formUrl || "#"),
  });
