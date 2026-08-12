export const buildAdminResetRequestEmailHtml = ({
  userName,
  userEmail,
  requestTime,
}) => {
  const safeName = userName?.trim() || "N/A";

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Password Reset Action Required · RGSL Admin</title>
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
  .info-box { background: #f8fafc; border: 1px solid #e2e8f0; border-left: 4px solid #F38120; border-radius: 12px; padding: 20px; margin-top: 24px; }
  .info-title { font-size: 15px; font-weight: 600; color: #097D76; margin-bottom: 12px; }
  .detail-row { display: flex; justify-content: space-between; padding: 6px 0; border-bottom: 1px dashed #e2e8f0; font-size: 14px; }
  .detail-row:last-child { border-bottom: none; }
  .detail-label { font-weight: 600; color: #475569; }
  .detail-value { color: #0f172a; font-weight: 500; }
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
    <h2 class="header-title">Password Reset Request</h2>
    <p class="header-subtitle">Admin Action Required</p>
  </div>
  <div class="content">
    <p class="greeting">Attention System Administrator,</p>
    <p class="body-text">A user has requested a password reset. Below are the user details for verification:</p>

    <div class="info-box">
      <div class="info-title">
        <i class="fas fa-user-shield"></i> User Request Details
      </div>
      <div class="detail-row">
        <span class="detail-label">User Name: </span>
        <span class="detail-value">${safeName}</span>
      </div>
      <div class="detail-row">
        <span class="detail-label">Email Address: </span>
        <span class="detail-value">${userEmail}</span>
      </div>
      <div class="detail-row">
        <span class="detail-label">Requested At: </span>
        <span class="detail-value">${requestTime}</span>
      </div>
    </div>
   </div>

  <div class="footer">
    <div class="footer-copyright">
      © ${new Date().getFullYear()} RGSL Internal Administration System.
    </div>
  </div>
</div>
</body>
</html>`;
};
