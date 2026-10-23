export const INVOICE_LOOKUP = `
  SELECT id, invoice_id, amount, status, ngenius_order_ref, public_ref, 'overstayed'::varchar
    FROM overstay_invoices
   WHERE q.email_type = 'overstayed' AND id = q.invoice_id
  UNION ALL
  SELECT id, invoice_id, amount, status, ngenius_order_ref, public_ref, 'storage'::varchar
    FROM storage_invoices
   WHERE q.email_type = 'storage' AND id = q.invoice_id
  UNION ALL
  SELECT id, invoice_id, amount, status, ngenius_order_ref, public_ref, 'delivery'::varchar
    FROM delivery_invoices
   WHERE q.email_type = 'delivery' AND id = q.invoice_id
  UNION ALL
  SELECT id, invoice_id, amount, status, ngenius_order_ref, public_ref, 'dropoff'::varchar
    FROM dropoff_invoices
   WHERE q.email_type = 'dropoff' AND id = q.invoice_id
`;
