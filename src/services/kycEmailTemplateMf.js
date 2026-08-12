export const buildKycEmailHtmlMf = ({ recipientName, formUrl }) => {
  const safeName = recipientName?.trim() || "Valued Customer";

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>KYC Request · Messiah Freight</title>
<style>
  body { font-family: 'Segoe UI', Arial, sans-serif; background: #f4f5fb; margin: 0; padding: 40px 20px; color: #1f2937; }
  .card { max-width: 620px; margin: 0 auto; background: #ffffff; border-radius: 24px; overflow: hidden; box-shadow: 0 12px 40px rgba(52, 65, 159, 0.15), 0 4px 12px rgba(0,0,0,0.05); border: 1px solid #e0e3f5; }
  .header { background: linear-gradient(135deg, #34419F 0%, #252e7a 100%); color: white; padding: 32px; text-align: center; border-bottom: 4px solid #F46A17; }
  .logo-area { display: block; text-align: center; margin-bottom: 20px; }
  .logo-plate { display: inline-block; background: rgba(255,255,255,0.12); border-radius: 12px; padding: 12px 20px; }
  .logo-img { height: 48px; display: block; }
  .header-title { font-size: 24px; font-weight: 700; margin: 0; }
  .header-subtitle { font-size: 15px; margin: 8px 0 0; color: rgba(255,255,255,0.9); line-height: 1.4; }
  .content { padding: 36px 32px; line-height: 1.6; font-size: 15px; color: #334155; }
  .greeting { font-size: 16px; font-weight: 600; color: #0f172a; margin-bottom: 12px; }
  .body-text { margin-bottom: 24px; }
  .btn-wrapper { text-align: center; margin: 32px 0; }
  .btn { display: inline-block; background: #34419F; color: #ffffff !important; text-decoration: none; padding: 14px 36px; border-radius: 40px; font-weight: 600; font-size: 16px; box-shadow: 0 6px 20px rgba(52, 65, 159, 0.35); }
  .btn-icon { margin-right: 8px; }
  .info-box { background: #f8fafc; border: 1px solid #e2e8f0; border-left: 4px solid #F46A17; border-radius: 12px; padding: 20px; margin-top: 28px; }
  .info-title { font-size: 15px; font-weight: 600; color: #34419F; display: flex; align-items: center; gap: 8px; margin-bottom: 12px; }
  .info-list { margin: 0; padding-left: 20px; color: #475569; }
  .info-list li { margin-bottom: 8px; font-size: 14px; }
  .info-list li::marker { color: #F46A17; }
  .security-notice { display: flex; align-items: center; gap: 10px; background: #fff7ed; border: 1px solid #ffedd5; color: #92400e; font-size: 13px; padding: 12px 16px; border-radius: 12px; margin-top: 24px; }
  .security-icon { color: #F46A17; font-size: 16px; }
  .footer { background: #1a2060; color: white; padding: 32px; text-align: center; border-top: 1px solid #2d3a8c; }
  .footer-logo { height: 40px; margin-bottom: 16px; opacity: 0.9; }
  .footer-text { font-size: 13px; color: #94a3b8; line-height: 1.5; margin-bottom: 16px; }
  .footer-divider { height: 1px; background: #2d3a8c; margin: 20px 0; }
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
        <img src="https://messiahfreight.co.uk/wp-content/uploads/2023/10/mfd-white.png" alt="Messiah Freight" class="logo-img">
      </div>
    </div>
    <h2 class="header-title">KYC Verification Request</h2>
    <p class="header-subtitle">Action required: Please complete your KYC submission to ensure uninterrupted account access.</p>
  </div>
  <div class="content">
    <p class="greeting">Dear ${safeName},</p>
    <p class="body-text">We are currently updating our compliance records in accordance with international freight and customs regulations. To maintain your active account status and ensure seamless operations, we require your updated Know Your Customer (KYC) information.</p>
    <p class="body-text">Please click the secure button below to access the submission portal and provide the necessary details. This process will take approximately 5-10 minutes.</p>
    <div class="btn-wrapper">
      <a class="btn" href="${formUrl}" style="display: inline-block; background: #34419F; color: #ffffff !important; text-decoration: none; padding: 14px 36px; border-radius: 40px; font-weight: 600; font-size: 16px;">
        <i class="fas fa-shield-alt btn-icon"></i>
        Complete KYC Verification
      </a>
    </div>
    <div class="info-box">
      <div class="info-title">
        <i class="fas fa-clipboard-list"></i>
        Required Documents for Upload
      </div>
      <ul class="info-list">
        <li>Clear scanned copy of your <strong>Passport</strong> (Photo page)</li>
        <li>Scanned copy of your <strong>Emirates ID</strong> (Front and Back)</li>
        <li>Valid <strong>Trade License</strong> (If applicable for corporate accounts)</li>
      </ul>
    </div>
    <div class="security-notice">
      <i class="fas fa-lock security-icon"></i>
      <span>This link is unique to you and will expire after use. Please do not share this link with third parties.</span>
    </div>
  </div>
  <div class="footer">
    <img src="https://messiahfreight.co.uk/wp-content/uploads/2023/10/mfd-white.png" alt="Messiah Freight" class="footer-logo">
    <div class="footer-text">
      Messiah Freight Delivery Ltd<br>
      Reliable freight and delivery solutions across the UK and beyond.
    </div>
    <table role="presentation" align="center" cellpadding="0" cellspacing="0" style="margin: 0 auto 20px; width: 100%; max-width: 480px;">
      <tr>
        <td style="padding: 6px 12px; font-size: 13px; color: #e2e8f0; text-align: center;">
          <i class="fas fa-globe" style="color: #F46A17;"></i>
          <a href="https://messiahfreight.co.uk" style="color: #F46A17; text-decoration: none;">messiahfreight.co.uk</a>
        </td>
      </tr>
    </table>
    <div class="footer-divider"></div>
    <div class="footer-copyright">
      © ${new Date().getFullYear()} Messiah Freight Delivery Ltd. All rights reserved.
      <br>
      <span style="color: #64748b; font-size: 11px; margin-top: 4px; display: block;">This is an automated compliance message. Please do not reply directly to this email.</span>
    </div>
  </div>
</div>
</body>
</html>`;
};
