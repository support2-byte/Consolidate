export const TEMPLATE_BY_EMAIL_TYPE = {
  order_created: "order_created",
  order_status_update: "shipment_update",
  invoice_generated: "invoice_email",
};

export const KYC_TEMPLATE_BY_COMPANY = {
  RGSL: "kyc_rgsl",
  MF: "kyc_mf",
  CAS: "kyc_cas",
};

const RGSL_OFFICES = `
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="font-size:11px;color:#64748b;line-height:1.6;">
  <tr>
    <td width="50%" valign="top" style="padding:0 8px 12px 0;">
      <strong style="color:#334155;">Dubai Office</strong><br>
      Royal Gulf Shipping &amp; Logistics LLC<br>Ras Al Khor Industrial Area 2<br>Dubai, United Arab Emirates
    </td>
    <td width="50%" valign="top" style="padding:0 0 12px 8px;">
      <strong style="color:#334155;">London Office</strong><br>
      Royal Gulf Shipping &amp; Logistics UK Ltd<br>Office # 2, Edgware Road, London<br>United Kingdom
    </td>
  </tr>
  <tr>
    <td valign="top" style="padding:0 8px 0 0;">
      <strong style="color:#334155;">Karachi Office</strong><br>
      Royal Gulf Shipping Pakistan<br>Clifton Block 5, Karachi, Pakistan
    </td>
    <td valign="top" style="padding:0 0 0 8px;">
      <strong style="color:#334155;">Shenzhen Office</strong><br>
      Royal Gulf Shipping China<br>Nanshan District, Shenzhen, China
    </td>
  </tr>
</table>`;

const BRANDS = {
  rgsl: {
    logo: "https://royalgulfshipping.com/wp-content/uploads/2025/09/royalgulflogo-1.jpeg",
    logoBg: "#ffffff",
    name: "Royal Gulf Shipping &amp; Logistics LLC",
    tagline: "Dubai &bull; London &bull; Karachi &bull; Shenzhen",
    accent: "#F26B1D", // orange
    green: "#1E9E4A",
    services: [
      ["International Freight Forwarding", "Sea, Air &amp; Land"],
      [
        "Cold Storage &amp; Reefer Solutions",
        "Temperature-controlled warehousing",
      ],
      ["Container Sales &amp; Rentals", "Dry, HC &amp; Reefer units"],
      [
        "Warehousing &amp; Fulfilment",
        "Short &amp; long-term storage with WMS",
      ],
      ["LCL Consolidation", "Weekly services from China, UK &amp; Europe"],
      ["Local Transportation", "UAE-wide pickup &amp; delivery"],
    ],
    help: {
      phone: "+971 56 681 7335",
      whatsapp: "https://wa.me/971566817335",
      email: "sales@royalgulfshipping.com",
      phoneLabel: "UAE Operations",
    },
    offices: RGSL_OFFICES,
    copyright: "Royal Gulf Shipping &amp; Logistics LLC",
  },
  mf: {
    logo: "https://messiahfreight.co.uk/wp-content/uploads/2023/10/mfd-white.png",
    logoBg: "#34419F",
    name: "Messiah Freight Delivery Ltd",
    tagline: "Reliable freight and delivery solutions across the UK and beyond",
    accent: "#F46A17",
    green: "#34419F",
    services: [
      ["UK &amp; International Freight", "Road, Sea &amp; Air"],
      ["Customs Clearance", "Import &amp; export documentation"],
      ["Last-mile Delivery", "Reliable UK-wide delivery"],
    ],
    help: null,
    offices: "",
    copyright: "Messiah Freight Delivery Ltd",
  },
  cas: {
    logo: "https://www.cargoas.com/wp-content/uploads/2026/02/Cargo-Avaiation-Logo.png.webp",
    logoBg: "#ffffff",
    name: "Cargo Aviation System",
    tagline: "World-class air freight and logistics solutions",
    accent: "#D91423",
    green: "#04274A",
    services: [
      ["Air Freight", "Scheduled &amp; charter cargo"],
      ["Airport-to-Airport &amp; Door-to-Door", "Flexible routing options"],
      ["Customs &amp; Documentation", "Aviation and freight compliance"],
    ],
    help: null,
    offices: "",
    copyright: "Cargo Aviation System",
  },
  dynamic: {
    logo: "{{companyLogo}}",
    logoBg: "#ffffff",
    name: "{{companyName}}",
    tagline: "",
    accent: "#F26B1D",
    green: "#1E9E4A",
    services: [],
    help: null,
    offices: "",
    copyright: "{{companyName}}",
  },
};

const rowsHtml = (rows) =>
  rows
    .map(([label, value], i) => {
      const border =
        i === rows.length - 1 ? "" : "border-bottom:1px solid #e5e7eb;";
      if (value === undefined) {
        return `<tr><td colspan="2" style="padding:11px 16px;font-size:13px;color:#334155;${border}">${label}</td></tr>`;
      }
      return `<tr>
        <td width="38%" style="padding:12px 16px;font-size:13px;color:#64748b;${border}">${label}</td>
        <td style="padding:12px 16px;font-size:13px;color:#0f172a;${border}">${value}</td>
      </tr>`;
    })
    .join("");

const layout = (
  brand,
  {
    title,
    pageTitle = "Royal Gulf Shipping",
    subLine = "",
    intro,
    statusBlock = "",
    summaryTitle,
    rows,
    beforeCta = "",
    cta,
    afterCta = "",
    extra = "",
    footerNote = "This is an automated notification. If you have already received these details, please ignore this email.",
  },
) => {
  const b = BRANDS[brand];

  const services = b.services.length
    ? `<tr><td style="padding:8px 24px 4px;">
        <h3 style="margin:18px 0 10px;font-size:17px;color:${b.green};">What We Do &ndash; Our Services</h3>
        <ul style="margin:0;padding-left:20px;font-size:13px;color:#475569;line-height:1.9;">
          ${b.services
            .map(
              ([n, d]) =>
                `<li style="color:${b.accent};"><strong style="color:${b.accent};">${n}</strong> <span style="color:#475569;">&ndash; ${d}</span></li>`,
            )
            .join("")}
        </ul>
      </td></tr>`
    : "";

  const help = b.help
    ? `<p style="margin:0 0 6px;font-size:13px;color:#475569;">&#128222; ${b.help.phoneLabel}: <a href="tel:${b.help.phone.replace(/\s/g, "")}" style="color:#1d4ed8;font-weight:bold;">${b.help.phone}</a></p>
       <p style="margin:0 0 6px;font-size:13px;color:#475569;">&#128172; WhatsApp Support: <a href="${b.help.whatsapp}" style="color:${b.green};font-weight:bold;">Click here to chat</a></p>
       <p style="margin:0;font-size:13px;color:#475569;">&#128231; Email: <a href="mailto:${b.help.email}" style="color:#1d4ed8;font-weight:bold;">${b.help.email}</a></p>`
    : "";

  const helpBlock = `<tr><td style="background:#f8fafc;border-top:1px solid #e5e7eb;padding:20px 24px;">
      <h3 style="margin:0 0 8px;font-size:17px;color:${b.accent};">Need Help?</h3>
      <p style="margin:0 0 10px;font-size:13px;color:#475569;line-height:1.5;">Our support team is available to assist you with any questions.</p>
      ${help}
    </td></tr>`;

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>${pageTitle}</title>
<style>
  body { margin:0; padding:0; background:#f1f5f9; font-family: Arial, Helvetica, sans-serif; color:#334155; }
  a { text-decoration:none; }
  p { margin:0 0 12px; line-height:1.55; }
  .body-text { font-size:13px; color:#334155; }
  .info-box { background:#f8fafc; border:1px solid #e5e7eb; border-left:4px solid ${b.accent}; border-radius:8px; padding:14px 16px; margin:18px 0; }
  .info-title { font-size:14px; font-weight:bold; color:${b.green}; margin-bottom:8px; }
  .items-table { width:100%; border-collapse:collapse; }
  .items-table th, .items-table td { padding:8px 8px; font-size:12px; text-align:left; border-bottom:1px solid #e5e7eb; }
  .items-table th { background:#f1f5f9; color:#475569; font-size:10.5px; text-transform:uppercase; }
  .items-table tr:last-child td { border-bottom:none; }
  .btn { display:inline-block; background:${b.accent}; color:#ffffff !important; font-weight:bold; font-size:14px; padding:13px 26px; border-radius:4px; }
  @media (max-width:640px) { .wrap { width:100% !important; } }
</style>
</head>
<body>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#f1f5f9;padding:16px 8px;">
<tr><td align="center">
<table role="presentation" class="wrap" width="600" cellpadding="0" cellspacing="0" style="max-width:600px;width:100%;background:#ffffff;">

  <!-- Header -->
  <tr><td style="padding:18px 24px 14px;border-bottom:2px solid ${b.accent};">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr>
      <td align="left" valign="middle">
        <span style="display:inline-block;background:${b.logoBg};border-radius:6px;padding:${b.logoBg === "#ffffff" ? "0" : "8px 12px"};">
          <img src="${b.logo}" alt="${b.name}" height="52" style="display:block;height:52px;">
        </span>
      </td>
      <td align="right" valign="middle" style="font-size:12px;color:#64748b;line-height:1.5;">
        <strong style="color:#0f172a;font-size:12px;">${b.name}</strong><br>${b.tagline}
      </td>
    </tr></table>
  </td></tr>

  <!-- Body -->
  <tr><td style="padding:24px 24px 8px;">
    <h2 style="margin:0 0 8px;font-size:22px;color:${b.accent};">${title}</h2>
    ${subLine ? `<p style="font-size:12px;color:#64748b;margin:0 0 14px;">${subLine}</p>` : ""}
    <p style="font-size:13px;font-weight:bold;color:#0f172a;">Dear <span style="color:${b.green};">{{recipientName}}</span>,</p>
    <p class="body-text">${intro}</p>
    ${statusBlock}
    ${
      rows && rows.length
        ? `<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border:1px solid #e5e7eb;margin:18px 0;border-collapse:collapse;">
        <tr><td colspan="2" style="background:${b.green};color:#ffffff;font-weight:bold;font-size:14px;padding:11px 16px;">${summaryTitle}</td></tr>
        ${rowsHtml(rows)}
      </table>`
        : ""
    }
    ${beforeCta ? `<p class="body-text">${beforeCta}</p>` : ""}
    ${extra}
    ${
      cta
        ? `<p style="margin:18px 0;"><a class="btn" href="${cta.href}" target="_blank" rel="noopener noreferrer" style="display:inline-block;background:${b.accent};color:#ffffff;font-weight:bold;font-size:14px;padding:13px 26px;border-radius:4px;">${cta.label}</a></p>`
        : ""
    }
    ${afterCta}
  </td></tr>

  ${services}
  ${helpBlock}

  <!-- Footer -->
  <tr><td style="padding:18px 24px 22px;border-top:1px solid #e5e7eb;">
    ${b.offices}
    <p style="margin:16px 0 0;font-size:10.5px;color:#94a3b8;">${footerNote}</p>
    <p style="margin:6px 0 0;font-size:10.5px;color:#94a3b8;">&copy; {{year}} ${b.copyright}. All rights reserved.</p>
  </td></tr>

</table>
</td></tr>
</table>
</body>
</html>`;
};

const statusBlock = (
  brand,
  label = "{{statusLabel}}",
  msg = "{{statusMsg}}",
) => `<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin:12px 0;background:#f0fdf4;border-left:5px solid ${BRANDS[brand].accent};">
  <tr><td style="padding:14px 16px;">
    <div style="font-weight:bold;font-size:15px;color:#0f172a;">&#128674; ${label}</div>
    <div style="margin-top:6px;font-size:13px;color:#475569;white-space:pre-line;">${msg}</div>
  </td></tr>
</table>`;

const trackFallback = `<p style="font-size:11px;color:#94a3b8;">If the button above is not working, visit
  <a href="https://trackorder.royalgulfshipping.com/" style="color:#1d4ed8;">https://trackorder.royalgulfshipping.com/</a>
  and enter your tracking ID.</p>
  {{actionLinks}}`;

const kycDocs = [
  ["Clear scanned copy of your <strong>Passport</strong> (Photo page)"],
  ["Scanned copy of your <strong>Emirates ID</strong> (Front and Back)"],
  [
    "Valid <strong>Trade License</strong> (If applicable for corporate accounts)",
  ],
];

const kycTemplate = (brand, regulation, pageTitle) =>
  layout(brand, {
    pageTitle,
    title: "KYC Verification Request",
    subLine:
      "Action required: please complete your KYC submission to ensure uninterrupted account access.",
    intro: `We are currently updating our compliance records in accordance with international ${regulation} regulations. To maintain your active account status and ensure seamless operations, we require your updated Know Your Customer (KYC) information. This takes approximately 5-10 minutes.`,
    summaryTitle: "Required Documents for Upload",
    rows: kycDocs,
    cta: { href: "{{formUrl}}", label: "Complete KYC Verification" },
    afterCta: `<p style="font-size:11px;color:#92400e;background:#fff7ed;border:1px solid #ffedd5;padding:10px 12px;border-radius:6px;">&#128274; This link is unique to you and will expire after use. Please do not share it with third parties.</p>`,
    footerNote:
      "This is an automated compliance message. Please do not reply directly to this email.",
  });

export const EMAIL_TEMPLATES = {
  order_created: layout("rgsl", {
    pageTitle: "Royal Gulf Shipping – Order Created",
    title: "Your Order Has Been Created",
    intro:
      "Thank you for your business with <strong>Royal Gulf Shipping &amp; Logistics LLC</strong>. An order has been generated and is now pending further action.",
    statusBlock: statusBlock("rgsl"),
    summaryTitle: "Tracking Summary",
    rows: [
      ["Tracking ID #", "<strong>{{refId}}</strong>"],
      ["Order ID", "{{orderId}}"],
      ["ETA", "{{eta}}"],
      ["Last Updated", "{{lastUpdated}}"],
    ],
    beforeCta:
      "Your tracking link will automatically pre-fill your Tracking ID. Simply click <strong>Track</strong> to view live shipment status.",
    cta: { href: "{{trackLink}}", label: "Track your shipment online" },
    afterCta: trackFallback,
  }),

  shipment_update: layout("rgsl", {
    pageTitle: "Royal Gulf Shipping – Shipment Update",
    title: "Your Shipment Status Has Changed",
    intro:
      "Thank you for your business with <strong>Royal Gulf Shipping &amp; Logistics LLC</strong>. Your shipment has a new update.",
    statusBlock: statusBlock("rgsl"),
    summaryTitle: "Tracking Summary",
    rows: [
      ["Ref ID", "<strong>{{refId}}</strong>"],
      ["Order ID", "{{orderId}}"],
      ["Route", "{{route}}"],
      ["ETA", "{{eta}}"],
      ["Last Updated", "{{lastUpdated}}"],
    ],
    beforeCta:
      "Your tracking link will automatically pre-fill your Reference ID. Simply click <strong>Track</strong> to view live shipment status.",
    cta: { href: "{{trackLink}}", label: "Track your shipment online" },
    afterCta: trackFallback,
  }),

  subscription_confirmed: layout("rgsl", {
    pageTitle: "Royal Gulf Shipping – Subscription Confirmed",
    title: "Your Subscription Is Confirmed",
    intro:
      "Thank you for your interest in <strong>Royal Gulf Shipping &amp; Logistics LLC</strong>. You will now receive shipment notifications directly to your inbox.",
    statusBlock: statusBlock(
      "rgsl",
      "Subscription Confirmed",
      "You have successfully subscribed to receive shipment updates from Royal Gulf Shipping.",
    ),
    summaryTitle: "Tracking Summary",
    rows: [
      ["Tracking ID #", "<strong>{{refId}}</strong>"],
      ["Order ID", "{{orderId}}"],
      ["Route", "{{route}}"],
      ["ETA", "{{eta}}"],
      ["Last Updated", "{{lastUpdated}}"],
    ],
    beforeCta:
      "Your tracking link will automatically pre-fill your Tracking ID. Simply click <strong>Track</strong> to view live shipment status.",
    cta: { href: "{{trackLink}}", label: "Track your shipment online" },
    afterCta:
      '<p style="font-size:11px;color:#94a3b8;">You are receiving this because you subscribed to shipment notifications on our website.</p>',
  }),

  kyc_rgsl: kycTemplate("rgsl", "maritime and financial", "KYC Request · RGSL"),
  kyc_cas: kycTemplate(
    "cas",
    "aviation and freight",
    "KYC Request · Cargo Aviation System",
  ),
  kyc_mf: kycTemplate(
    "mf",
    "freight and customs",
    "KYC Request · Messiah Freight",
  ),

  confirmation_email: layout("dynamic", {
    pageTitle: "Booking Confirmation",
    title: "Booking Confirmation",
    subLine:
      "Your booking details have been recorded and are ready for your review.",
    intro: "{{introText}}",
    extra: "{{messageSection}}",
    summaryTitle: "Booking Summary",
    rows: [
      ["Mode", "<strong>{{mode}}</strong>"],
      ["Total Qty", "{{totalQty}}"],
      ["Total Weight", "{{totalWeight}} KG"],
    ],
    beforeCta: "",
    cta: { href: "{{viewLink}}", label: "View Booking Confirmation" },
    afterCta: "{{itemsSection}}",
    footerNote:
      "This email was sent on behalf of {{companyName}}. If the button above doesn't work, contact us directly.",
  }),

  invoice_email: layout("rgsl", {
    pageTitle: "Invoice · RGSL",
    title: "Your Invoice {{invoiceId}}",
    subLine: "Invoice: {{invoiceId}}",
    intro:
      "Thank you for your business with <strong>Royal Gulf Shipping &amp; Logistics LLC</strong>. A new invoice has been generated for your account.",
    summaryTitle: "Invoice Summary",
    rows: [
      ["Invoice No.", "<strong>{{invoiceId}}</strong>"],
      ["Item Ref", "{{itemRef}}"],
      ["Issue Date", "{{lastUpdated}}"],
      [
        "Amount Due",
        '<strong style="color:#1E9E4A;font-size:15px;">{{amount}}</strong>',
      ],
    ],
    cta: { href: "{{invoiceLink}}", label: "View Invoice" },
    afterCta:
      '<p style="font-size:11px;color:#92400e;">If you have already made this payment, please disregard this notice.</p>',
    footerNote:
      "This is an automated billing message. Please do not reply directly to this email.",
  }),
};
