import pool from "../db/pool.js";
import { transporter } from "../middleware/nodeMailer.js";

function buildOrderConfirmationHTML({
  name,
  orderId,
  bookingRef,
  rglBookingNumber,
}) {
  return `
  <!DOCTYPE html>
  <html>
  <head>
    <meta charset="UTF-8">
  </head>
  <body style="margin:0; padding:0; background-color:#f4f4f4; font-family: Arial, sans-serif;">
    <table width="100%" cellpadding="0" cellspacing="0" style="background-color:#f4f4f4; padding: 24px 0;">
      <tr>
        <td align="center">
          <table width="600" cellpadding="0" cellspacing="0" style="background:#fff; border-radius:8px; overflow:hidden; box-shadow: 0 2px 8px rgba(0,0,0,0.08);">
            <tr>
              <td style="background-color:#0d6c6a; padding: 20px 24px;">
                <span style="color:#ffffff; font-size:20px; font-weight:bold; letter-spacing:0.5px;">
                  Royal Gulf Shipping &amp; Logistics
                </span>
              </td>
            </tr>
            <tr>
              <td style="padding: 32px 24px 8px;">
                <h2 style="margin:0 0 8px; color:#0d6c6a; font-size:20px;">Order Confirmation</h2>
                <p style="margin:0 0 16px; color:#333; font-size:14px; line-height:1.5;">
                  Dear ${name || "Customer"},
                </p>
                <p style="margin:0 0 16px; color:#333; font-size:14px; line-height:1.5;">
                  Your order has been successfully created and is now being processed by our team.
                </p>
              </td>
            </tr>
            <tr>
              <td style="padding: 0 24px 24px;">
                <table width="100%" cellpadding="8" cellspacing="0" style="border:1px solid #eee; border-radius:6px; font-size:13px; color:#333;">
                  <tr style="background:#fafafa;">
                    <td style="font-weight:bold; width:40%;">Booking Reference</td>
                    <td>${bookingRef || "-"}</td>
                  </tr>
                  <tr>
                    <td style="font-weight:bold;">RGSL Booking Number</td>
                    <td>${rglBookingNumber || "-"}</td>
                  </tr>
                  <tr style="background:#fafafa;">
                    <td style="font-weight:bold;">Order ID</td>
                    <td>${orderId}</td>
                  </tr>
                </table>
              </td>
            </tr>
            <tr>
              <td style="padding: 0 24px 32px;">
                <p style="margin:0; color:#666; font-size:12px; line-height:1.5;">
                  If you have any questions about this order, please contact our team and reference
                  your booking number above.
                </p>
              </td>
            </tr>
            <tr>
              <td style="background:#fafafa; padding:16px 24px; text-align:center; border-top:1px solid #eee;">
                <span style="color:#999; font-size:11px;">
                  This is an automated message from Royal Gulf Shipping &amp; Logistics.
                </span>
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </body>
  </html>
  `;
}

export async function sendOrderConfirmationEmail({ to, name, orderId }) {
  if (!to) {
    throw new Error("Missing recipient email");
  }

  const { rows } = await pool.query(
    `SELECT booking_ref, rgl_booking_number FROM orders WHERE id = $1`,
    [orderId],
  );
  const order = rows[0] || {};

  const html = buildOrderConfirmationHTML({
    name,
    orderId,
    bookingRef: order.booking_ref,
    rglBookingNumber: order.rgl_booking_number,
  });

  await transporter.sendMail({
    from:
      process.env.SMTP_FROM || `"RGSL Logistics" <${process.env.SMTP_USER}>`,
    to,
    subject: `Order Confirmation — ${order.booking_ref || `#${orderId}`}`,
    html,
  });
}
