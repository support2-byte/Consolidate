export const buildInvoiceEmailHtml = ({
  recipientName,
  invoiceId,
  itemRef,
  amount,
  invoiceLink,
  otp,
}) => {
  const safeName = recipientName?.trim() || "Valued Customer";

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Invoice · RGSL</title>
<style>
  body { font-family: 'Segoe UI', Arial, sans-serif; background: #f4f7fc; margin: 0; padding: 40px 20px; color: #1f2937; }
  .card { max-width: 620px; margin: 0 auto; background: #ffffff; border-radius: 24px; overflow: hidden; box-shadow: 0 12px 40px rgba(9, 125, 118, 0.15), 0 4px 12px rgba(0,0,0,0.05); border: 1px solid #eef2f6; }
  .header { background: linear-gradient(135deg, #097D76 0%, #06655f 100%); color: white; padding: 32px; text-align: center; border-bottom: 4px solid #F38120; }
  .logo-area { display: block; text-align: center; margin-bottom: 20px; }
  .logo-plate { display: inline-block; background: #ffffff; border-radius: 12px; padding: 12px 20px; }
  .logo-img { height: 48px; display: block; }
  .header-title { font-size: 24px; font-weight: 700; margin: 0; }
  .header-subtitle { font-size: 15px; margin: 8px 0 0; color: rgba(255,255,255,0.9); line-height: 1.4; }
  .content { padding: 36px 32px; line-height: 1.6; font-size: 15px; color: #334155; }
  .greeting { font-size: 16px; font-weight: 600; color: #0f172a; margin-bottom: 12px; }
  .body-text { margin-bottom: 24px; }
  .btn-wrapper { text-align: center; margin: 32px 0; }
  .btn { display: inline-block; background: #097D76; color: white; text-decoration: none; padding: 14px 36px; border-radius: 40px; font-weight: 600; font-size: 16px; box-shadow: 0 6px 20px rgba(9, 125, 118, 0.3); }
  .btn-icon { margin-right: 8px; }
  .amount-box { background: #ecfdf5; border-left: 5px solid #097D76; border-radius: 10px; padding: 16px 20px; margin: 20px 0; text-align: center; }
  .amount-label { font-size: 12px; color: #64748b; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px; }
  .amount-value { font-size: 26px; font-weight: 800; color: #097D76; }
  .info-box { background: #f8fafc; border: 1px solid #e2e8f0; border-left: 4px solid #F38120; border-radius: 12px; padding: 20px; margin-top: 28px; }
  .info-title { font-size: 15px; font-weight: 600; color: #097D76; display: flex; align-items: center; gap: 8px; margin-bottom: 12px; }
  .info-table { width: 100%; border-collapse: collapse; }
  .info-table td { padding: 7px 0; font-size: 14px; color: #475569; border-bottom: 1px dashed #e2e8f0; }
  .info-table td:first-child { width: 140px; color: #64748b; text-transform: uppercase; font-size: 11px; letter-spacing: 0.4px; }
  .info-table tr:last-child td { border-bottom: none; }
  .security-notice { display: flex; align-items: center; gap: 10px; background: #fff7ed; border: 1px solid #ffedd5; color: #92400e; font-size: 13px; padding: 12px 16px; border-radius: 12px; margin-top: 24px; }
  .security-icon { color: #F38120; font-size: 16px; }
  .footer { background: #0f172a; color: white; padding: 32px; text-align: center; border-top: 1px solid #e2e8f0; }
  .footer-logo { height: 36px; margin-bottom: 16px; opacity: 0.8; }
  .footer-text { font-size: 13px; color: #94a3b8; line-height: 1.5; margin-bottom: 16px; }
  .footer-divider { height: 1px; background: #334155; margin: 20px 0; }
  .footer-copyright { font-size: 12px; color: #64748b; }
  @media (max-width: 640px) {
    body { padding: 16px; }
    .card { border-radius: 16px; }
    .header { padding: 24px 20px; }
    .content { padding: 24px 20px; }
    .footer { padding: 24px 20px; }
    .header-title { font-size: 20px; }
  }
</style>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
</head>
<body>
<div class="card">
  <div class="header">
    <div class="logo-area">
        <div class="logo-plate">
            <img src="https://royalgulfshipping.com/wp-content/uploads/2023/08/RGSL-LOGO.png" alt="Royal Gulf Shipping & Logistics" class="logo-img">
        </div>
    </div>
    <h2 class="header-title">Invoice ${invoiceId}</h2>
    <p class="header-subtitle">A new invoice has been generated for your account.</p>
  </div>
  <div class="content">
    <p class="greeting">Dear ${safeName},</p>
    <p class="body-text">Please find below the details of your invoice ${invoiceId}. You can view or download the full invoice using the button below.</p>
    <div class="btn-wrapper">
        <a class="btn" href="${invoiceLink || "#"}" style="display: inline-block; background: #097D76; color: #ffffff !important; text-decoration: none; padding: 14px 36px; border-radius: 40px; font-weight: 600; font-size: 16px;">
            <i class="fas fa-file-invoice-dollar btn-icon"></i>
            View Invoice
        </a>
    </div>
    ${
      amount
        ? `<div class="amount-box">
      <div class="amount-label">Amount Due</div>
      <div class="amount-value">${amount}</div>
    </div>`
        : ""
    }
        ${
          otp
            ? `<div class="amount-box">
      <div class="amount-label">Verification Code</div>
      <div class="amount-value" style="letter-spacing: 6px;">${otp}</div>
      <div style="font-size: 12px; color: #64748b; margin-top: 6px;">Enter this code on the payment page to unlock payment.</div>
    </div>`
            : ""
        }
    <div class="info-box">
      <div class="info-title">
        <i class="fas fa-clipboard-list"></i>
        Invoice Summary
      </div>
      <table class="info-table" role="presentation">
        <tr><td>Invoice No.</td><td><strong>${invoiceId}</strong></td></tr>
        <tr><td>Item Ref</td><td>${itemRef}</td></tr>
      </table>
    </div>
    <div class="security-notice">
      <i class="fas fa-info-circle security-icon"></i>
      <span>If you have already made this payment, please disregard this notice.</span>
    </div>
  </div>
  <div class="footer">
    <img src="https://royalgulfshipping.com/wp-content/uploads/2023/08/RGSL-LOGO-white.png" alt="RGSL Logo" class="footer-logo">
    <div class="footer-text">
      Royal Gulf Shipping &amp; Logistics LLC<br>
      Providing world-class shipping, clearing, and forwarding solutions.
    </div>
    <div class="footer-divider"></div>
    <div class="footer-copyright">
      © ${new Date().getFullYear()} Royal Gulf Shipping LLC. All rights reserved.
      <br>
      <span style="color: #64748b; font-size: 11px; margin-top: 4px; display: block;">This is an automated billing message. Please do not reply directly to this email.</span>
    </div>
  </div>
</div>
</body>
</html>`;
};
