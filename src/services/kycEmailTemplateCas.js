import { escapeHtml } from "./escapeHtml.js";
import { renderEmail } from "./renderEmail.js";

export const buildKycEmailHtmlCas = ({ recipientName, formUrl }) =>
  renderEmail("kyc_cas", {
    recipientName: escapeHtml(recipientName?.trim() || "Valued Customer"),
    formUrl: escapeHtml(formUrl || "#"),
  });
