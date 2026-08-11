export const buildPasswordUpdatedEmailHtml = ({
  recipientName,
  newPassword,
  loginUrl,
}) => {
  const safeName = recipientName?.trim() || "Valued User";

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Password Reset Confirmation · RGSL</title>
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
  .credential-value { font-family: 'Courier New', Courier, monospace; font-size: 22px; font-weight: 700; color: #097D76; letter-spacing: 2px; word-break: break-all; }
  .btn-wrapper { text-align: center; margin: 32px 0; }
  .btn { display: inline-block; background: #097D76; color: white; text-decoration: none; padding: 14px 36px; border-radius: 40px; font-weight: 600; font-size: 16px; box-shadow: 0 6px 20px rgba(9, 125, 118, 0.3); }
  .btn-icon { margin-right: 8px; }
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
    <h2 class="header-title">Password Successfully Updated</h2>
    <p class="header-subtitle">Your account credentials have been updated by an administrator.</p>
  </div>
  <div class="content">
    <p class="greeting">Hello ${safeName},</p>
    <p class="body-text">Your password for the RGSL Portal has been updated. Below is your new temporary password:</p>

    <div class="credential-box">
      <div class="credential-label">Your New Password</div>
      <div class="credential-value">${newPassword}</div>
    </div>

    <p class="body-text">Please sign in to your account using this new password. For security, all existing active sessions have been signed out.</p>

    <div class="btn-wrapper">
      <a class="btn" href="${loginUrl}" style="display: inline-block; background: #097D76; color: #ffffff !important; text-decoration: none; padding: 14px 36px; border-radius: 40px; font-weight: 600; font-size: 16px;">
        <i class="fas fa-sign-in-alt btn-icon"></i>
        Sign In to Portal
      </a>
    </div>

    <div class="security-notice">
      <i class="fas fa-lock security-icon"></i>
      <span>If you did not request this change, please reach out to your administrator immediately.</span>
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
