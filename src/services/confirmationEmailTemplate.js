import { renderEmail } from "./renderEmail.js";

const esc = (v) =>
  String(v ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");

const DEFAULT_LOGO =
  "https://royalgulfshipping.com/wp-content/uploads/2025/09/royalgulflogo-1.jpeg";

export const buildConfirmationEmailHtml = ({
  recipientName,
  senderName,
  receiverName,
  partyRole,
  message,
  companyName,
  companyLogo,
  mode,
  totalQty,
  totalWeight,
  viewLink,
  otp,
  items,
}) => {
  const safeCompany = esc(
    companyName?.trim() || "Royal Gulf Shipping & Logistics",
  );

  const isSenderParty = partyRole === "sender";
  const otherParty = esc(isSenderParty ? receiverName : senderName);
  const introText = `${safeCompany} has prepared a booking confirmation in which you are named as the ${isSenderParty ? "sender" : "receiver"}${otherParty ? `, ${isSenderParty ? "shipping to" : "receiving from"} ${otherParty}` : ""}. Please review the details below and complete your form using the button.`;

  const messageSection = message?.trim()
    ? `<div class="info-box" style="margin-top:0;"><p style="margin:0;white-space:pre-line;">${esc(message.trim())}</p></div>`
    : "";

  const itemsRows =
    Array.isArray(items) && items.length
      ? items
          .map(
            (it) => `<tr>
          <td>${esc(it.category) || "—"}</td>
          <td>${esc(it.subcategory) || "—"}</td>
          <td>${esc(it.type) || "—"}</td>
          <td>${esc(it.qty ?? "—")}</td>
          <td>${esc(it.weight ?? "—")} KG</td>
          <td>${esc(it.placeOfLoading) || "—"}</td>
          <td>${esc(it.placeOfDestination) || "—"}</td>
        </tr>`,
          )
          .join("")
      : "";

  const itemsSection = itemsRows
    ? `<div class="info-box">
      <div class="info-title">Items</div>
      <table class="items-table" role="presentation">
        <thead><tr>
          <th>Category</th><th>Subcategory</th><th>Type</th><th>Qty</th>
          <th>Weight</th><th>Loading</th><th>Destination</th>
        </tr></thead>
        <tbody>${itemsRows}</tbody>
      </table>
    </div>`
    : "";

  const otpSection = otp
    ? `<div class="info-box">
      <div class="info-title">Verification Code</div>
      <p style="margin:0 0 4px;font-size:28px;font-weight:bold;letter-spacing:6px;color:#1E9E4A;text-align:center;">${esc(otp)}</p>
      <p style="margin:0;font-size:12px;color:#64748b;text-align:center;">Use this code to verify your booking confirmation. Do not share it with anyone.</p>
    </div>`
    : "";

  return renderEmail("confirmation_email", {
    companyLogo: esc(companyLogo || DEFAULT_LOGO),
    companyName: safeCompany,
    recipientName: esc(recipientName?.trim() || "Customer"),
    introText,
    messageSection,
    viewLink: esc(viewLink || "#"),
    mode: esc(mode || "—"),
    totalQty: esc(totalQty ?? "—"),
    totalWeight: esc(totalWeight ?? "—"),
    itemsSection: itemsSection + otpSection,
  });
};
