import { escapeHtml } from "./escapeHtml.js";
import { renderEmail } from "./renderEmail.js";

export const buildKycEmailHtmlMf = ({ recipientName, formUrl }) =>
  renderEmail("kyc_mf", {
    recipientName: escapeHtml(recipientName?.trim() || "Valued Customer"),
    formUrl: escapeHtml(formUrl || "#"),
  });
