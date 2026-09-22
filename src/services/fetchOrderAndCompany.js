import pool from "../db/pool.js";

export const fetchOrderAndCompany = async (bookingFormId) => {
  const { rows: bcRows } = await pool.query(
    `SELECT bc.mode, bc.form_id,
            c.company, c.logo_url, c.primary_color, c.secondary_color,
            c.address AS company_address, c.phone AS company_phone, c.email AS company_email
     FROM booking_confirmations bc
     JOIN companies c ON c.id = bc.company_id
     WHERE bc.id = $1`,
    [bookingFormId],
  );
  const bc = bcRows[0];

  const [itemsRes, senderRes, receiverRes] = await Promise.all([
    pool.query(
      `SELECT qty, weight, category, subcategory, type,
              place_of_loading AS "portOfLoading",
              place_of_destination AS "portOfDestination"
       FROM booking_confirmation_items WHERE booking_form_id = $1 ORDER BY id`,
      [bookingFormId],
    ),
    pool.query(
      `SELECT name, address, phone, email FROM booking_confirmation_senders WHERE booking_form_id = $1`,
      [bookingFormId],
    ),
    pool.query(
      `SELECT name, address, phone, email, company_name FROM booking_confirmation_receivers WHERE booking_form_id = $1`,
      [bookingFormId],
    ),
  ]);

  const sender = senderRes.rows[0] || {};
  const receiver = receiverRes.rows[0] || {};

  return {
    formId: bc.form_id,
    company: {
      company: bc.company,
      logoUrl: bc.logo_url,
      primaryColor: bc.primary_color,
      secondaryColor: bc.secondary_color,
      address: bc.company_address,
      phone: bc.company_phone,
      email: bc.company_email,
    },
    order: {
      mode: bc.mode,
      senderName: sender.name || "",
      senderAddress: sender.address || "",
      senderContact: sender.phone || "",
      senderEmail: sender.email || "",
      receiverName: receiver.name || "",
      receiverAddress: receiver.address || "",
      receiverContact: receiver.phone || "",
      receiverEmail: receiver.email || "",
      receiverCompany: receiver.company_name || "",
      items: itemsRes.rows,
    },
  };
};
