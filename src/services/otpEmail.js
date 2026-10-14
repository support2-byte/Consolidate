export const otpEmailTemplate = ({ otp, fullName }) => {
  const safeName = fullName || "there";
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Your Login Code · RGSL</title>
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
  .body-text { margin-bottom: 20px; }
  .credential-box { background: #f8fafc; border: 1px dashed #097D76; border-left: 4px solid #097D76; border-radius: 12px; padding: 20px; margin: 24px 0; text-align: center; }
  .credential-label { font-size: 13px; text-transform: uppercase; tracking: 1px; color: #64748b; font-weight: 600; margin-bottom: 6px; }
  .credential-value { font-family: 'Courier New', Courier, monospace; font-size: 32px; font-weight: 700; color: #097D76; letter-spacing: 8px; word-break: break-all; }
  .expiry-note { font-size: 13px; color: #64748b; margin-top: 10px; }
  .security-notice { display: flex; align-items: center; gap: 10px; background: #fff7ed; border: 1px solid #ffedd5; color: #92400e; font-size: 13px; padding: 12px 16px; border-radius: 12px; margin-top: 24px; }
  .security-icon { color: #F38120; font-size: 16px; }
  .footer { background: #0f172a; color: white; padding: 24px 32px; text-align: center; border-top: 1px solid #e2e8f0; }
  .footer-copyright { font-size: 12px; color: #64748b; }
  @media (max-width: 640px) {
    body { padding: 16px; }
    .card { border-radius: 16px; }
    .header { padding: 24px 20px; }
    .content { padding: 24px 20px; }
    .footer { padding: 20px; }
    .credential-value { font-size: 26px; letter-spacing: 5px; }
  }
</style>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
</head>
<body>
<div class="card">
  <div class="header">
    <div class="logo-area">
      <div class="logo-plate">
        <img src="https://royalgulfshipping.com/wp-content/uploads/2023/08/RGSL-LOGO.png" alt="RGSL Portal" class="logo-img">
      </div>
    </div>
    <h2 class="header-title">Your Login Code</h2>
    <p class="header-subtitle">Use the code below to securely sign in to your account.</p>
  </div>
  <div class="content">
    <p class="greeting">Hello ${safeName},</p>
    <p class="body-text">Here is your one-time verification code. Enter it to complete your sign-in.</p>

    <div class="credential-box">
      <div class="credential-label">Your Login Code</div>
      <div class="credential-value">${otp}</div>
      <div class="expiry-note">This code expires in 5 minutes.</div>
    </div>

    <div class="security-notice">
      <i class="fas fa-lock security-icon"></i>
      <span>If you didn't request this code, you can safely ignore this email.</span>
    </div>
  </div>

  <div class="footer">
    <div class="footer-copyright">
      © ${new Date().getFullYear()} RGSL Internal Portal. All rights reserved.
    </div>
  </div>
</div>
</body>
</html>`;
};
