const MAX_ORDERS = 10;
const MAX_ITEMS_PER_ORDER = 5;

const TEAL = "#097D76";
const ORANGE = "#F38120";

const esc = (v) =>
  String(v ?? "-")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");

const fmtDate = (v) => {
  if (!v) return "-";
  const d = new Date(v);
  const p = (n) => String(n).padStart(2, "0");
  return `${p(d.getDate())}/${p(d.getMonth() + 1)}/${d.getFullYear()}`;
};

const idleColor = (days) => (Number(days) >= 7 ? "#c62828" : "#c2410c");

const plural = (n, word) => `${n} ${word}${n === 1 ? "" : "s"}`;

export const groupStaleItems = (items) => {
  const groups = new Map();
  items.forEach((it) => {
    const key = `${it.rgl_booking_number || ""}|${it.booking_ref || ""}`;
    if (!groups.has(key)) {
      groups.set(key, {
        formNo: it.rgl_booking_number,
        bookingRef: it.booking_ref,
        receiver: it.receiver_name,
        maxIdle: Number(it.idle_days) || 0,
        items: [],
      });
    }
    groups.get(key).items.push(it);
  });
  return [...groups.values()];
};

const th = (label, width) =>
  `<td width="${width}" style="padding:7px 12px;font-size:11px;font-weight:700;color:#8a4b0f;letter-spacing:0.5px;text-transform:uppercase;">${label}</td>`;

const renderOrder = (g) => {
  const shown = g.items.slice(0, MAX_ITEMS_PER_ORDER);
  const moreItems = g.items.length - shown.length;

  const itemRows = shown
    .map(
      (it, i) => `
      <tr>
        <td width="40%" style="background:#fff8f1;border-left:4px solid ${ORANGE};border-bottom:1px dashed #f0d9c2;padding:9px 12px;font-size:13px;color:#0f172a;word-break:break-all;">
          <span style="color:#94a3b8;font-size:11px;">${i + 1}.</span> ${esc(it.item_ref)}
        </td>
        <td width="28%" style="background:#fff8f1;border-bottom:1px dashed #f0d9c2;padding:9px 12px;font-size:13px;color:#334155;">${esc(it.status)}</td>
        <td width="14%" style="background:#fff8f1;border-bottom:1px dashed #f0d9c2;padding:9px 12px;font-size:13px;font-weight:700;color:${idleColor(it.idle_days)};white-space:nowrap;">${esc(it.idle_days)}d</td>
        <td width="18%" style="background:#fff8f1;border-bottom:1px dashed #f0d9c2;padding:9px 12px;font-size:13px;color:#334155;white-space:nowrap;">${fmtDate(it.eta)}</td>
      </tr>`,
    )
    .join("");

  const moreRow = moreItems
    ? `<tr>
        <td colspan="4" style="background:#fff8f1;border-left:4px solid ${ORANGE};padding:8px 12px;font-size:12px;color:#8a4b0f;font-style:italic;">
          + ${plural(moreItems, "more item")} in this order
        </td>
      </tr>`
    : "";

  return `
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;border:1px solid #cfe5e3;margin-bottom:16px;">
    <tr>
      <td colspan="2" style="background:#d9eeec;border-left:4px solid ${TEAL};padding:12px;">
        <div style="font-size:15px;font-weight:700;color:${TEAL};">${esc(g.formNo)}
          <span style="font-weight:400;color:#475569;font-size:13px;">&nbsp;·&nbsp; ${esc(g.bookingRef)}</span>
        </div>
        <div style="font-size:13px;color:#0f172a;margin-top:3px;">${esc(g.receiver)}</div>
      </td>
      <td colspan="2" align="right" style="background:#d9eeec;padding:12px;white-space:nowrap;">
        <span style="display:inline-block;background:${ORANGE};color:#ffffff;font-size:12px;font-weight:700;padding:3px 10px;border-radius:12px;">${plural(g.items.length, "item")}</span>
        <div style="font-size:12px;color:#475569;margin-top:6px;">Longest idle: <strong style="color:${idleColor(g.maxIdle)};">${g.maxIdle}d</strong></div>
      </td>
    </tr>
    <tr style="background:#ffe9d2;">
      <td width="40%" style="background:#ffe9d2;border-left:4px solid ${ORANGE};padding:7px 12px;font-size:11px;font-weight:700;color:#8a4b0f;letter-spacing:0.5px;text-transform:uppercase;">Item Ref</td>
      ${th("Status", "28%")}
      ${th("Idle", "14%")}
      ${th("ETA", "18%")}
    </tr>
    ${itemRows}
    ${moreRow}
  </table>`;
};

const stat = (value, label, color) => `
  <td align="center" style="padding:14px 8px;background:#f8fafc;border:1px solid #e2e8f0;">
    <div style="font-size:26px;font-weight:700;color:${color};line-height:1.1;">${value}</div>
    <div style="font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:0.5px;margin-top:4px;">${label}</div>
  </td>`;

export const buildEtaStaleEmailHtml = ({ items, adminUrl }) => {
  const groups = groupStaleItems(items);
  const shownGroups = groups.slice(0, MAX_ORDERS);
  const hiddenGroups = groups.slice(MAX_ORDERS);
  const hiddenOrderCount = hiddenGroups.length;
  const hiddenItemCount = hiddenGroups.reduce((s, g) => s + g.items.length, 0);
  const longestIdle = groups.length ? groups[0].maxIdle : 0;
  const today = new Date().toLocaleDateString("en-GB", {
    day: "2-digit",
    month: "short",
    year: "numeric",
  });

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ETA Monitoring · RGSL</title>
<style>
  body { font-family: 'Segoe UI', Arial, sans-serif; background: #f4f7fc; margin: 0; padding: 40px 20px; color: #1f2937; }
  .card { max-width: 770px; margin: 0 auto; background: #ffffff; border-radius: 24px; overflow: hidden; box-shadow: 0 12px 40px rgba(9,125,118,0.15), 0 4px 12px rgba(0,0,0,0.05); border: 1px solid #eef2f6; }
  .header { background: linear-gradient(135deg, #097D76 0%, #06655f 100%); color: white; padding: 32px; text-align: center; border-bottom: 4px solid #F38120; }
  .logo-plate { display: inline-block; background: #ffffff; border-radius: 12px; padding: 12px 20px; margin-bottom: 20px; }
  .logo-img { height: 48px; display: block; }
  .header-title { font-size: 24px; font-weight: 700; margin: 0; }
  .header-subtitle { font-size: 15px; margin: 8px 0 0; color: rgba(255,255,255,0.9); line-height: 1.4; }
  .content { padding: 32px; line-height: 1.6; font-size: 15px; color: #334155; }
  .footer { background: #0f172a; color: white; padding: 24px 32px; text-align: center; }
  .footer-text { font-size: 13px; color: #94a3b8; line-height: 1.5; }
  .footer-copyright { font-size: 12px; color: #64748b; margin-top: 12px; }
  @media (max-width: 640px) {
    body { padding: 16px; }
    .card { border-radius: 16px; }
    .header, .content, .footer { padding: 20px; }
    .header-title { font-size: 20px; }
  }
</style>
</head>
<body>
<div class="card">
  <div class="header">
    <div class="logo-plate">
      <img src="https://royalgulfshipping.com/wp-content/uploads/2025/09/royalgulflogo-1.jpeg" alt="Royal Gulf Shipping & Logistics" class="logo-img">
    </div>
    <h2 class="header-title">ETA Monitoring Report</h2>
    <p class="header-subtitle">Daily check · ${today}</p>
  </div>

  <div class="content">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="8" style="margin:-8px -8px 12px;width:calc(100% + 16px);">
      <tr>
        ${stat(items.length, "Stale items", "#c2410c")}
        ${stat(groups.length, "Orders affected", TEAL)}
        ${stat(`${longestIdle}d`, "Longest idle", "#c62828")}
      </tr>
    </table>

    <p style="margin:0 0 20px;">
      These items have stayed in one status past their check point and need admin action.
      Orders with the longest-idle items are listed first. Please add grace days or mark them as Delayed.
    </p>

    ${shownGroups.map(renderOrder).join("")}

    ${
      hiddenOrderCount > 0
        ? `<p style="margin:4px 0 0;font-size:13px;color:#92400e;background:#fff7ed;border:1px solid #ffedd5;border-radius:12px;padding:12px 16px;">
      Showing the top ${shownGroups.length} orders by idle time.
      <strong>${plural(hiddenOrderCount, "more order")}</strong> (${plural(hiddenItemCount, "item")}) are pending in Admin Notifications.
    </p>`
        : ""
    }

    ${
      adminUrl
        ? `<div style="text-align:center;margin:28px 0 0;">
      <a href="${esc(adminUrl)}" style="display:inline-block;background:${TEAL};color:#ffffff;text-decoration:none;padding:12px 32px;border-radius:40px;font-weight:600;font-size:15px;">Open Admin Notifications</a>
    </div>`
        : ""
    }
  </div>

  <div class="footer">
    <div class="footer-text">Royal Gulf Shipping &amp; Logistics LLC<br>Providing world-class shipping, clearing, and forwarding solutions.</div>
    <div class="footer-copyright">
      © ${new Date().getFullYear()} Royal Gulf Shipping LLC. All rights reserved.<br>
      This is an automated ETA monitoring message. Please do not reply directly to this email.
    </div>
  </div>
</div>
</body>
</html>`;
};
