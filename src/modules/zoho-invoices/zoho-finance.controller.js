import pool from "../../db/pool.js";
import logger from "../../services/logger.js";

const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
const BASE_CURRENCY = "AED";

const ORDERS_SQL = `
WITH keys AS (
  SELECT regexp_replace(lower(btrim(order_number)), '[a-z]+$', '') AS k
    FROM zoho_invoices WHERE btrim(COALESCE(order_number, '')) <> ''
  UNION
  SELECT regexp_replace(lower(btrim(order_number)), '[a-z]+$', '') AS k
    FROM zoho_bills WHERE btrim(COALESCE(order_number, '')) <> ''
),
ord AS (
  SELECT o.id, o.booking_ref, btrim(o.rgl_booking_number) AS form_no,
         lower(btrim(o.rgl_booking_number)) AS k
    FROM orders o
   WHERE lower(btrim(o.rgl_booking_number)) IN (SELECT k FROM keys)
),
item_rfd AS (
  SELECT oi.order_id,
         (SELECT MAX(t.created_time)
            FROM order_tracking t
           WHERE t.order_id = oi.order_id
             AND t.item_ref = oi.item_ref
             AND t.status = 'Ready for Delivery') AS rfd
    FROM order_items oi
   WHERE oi.order_id IN (SELECT id FROM ord)
),
ord_rfd AS (
  SELECT order_id,
         CASE WHEN COUNT(rfd) = COUNT(*) THEN MAX(rfd) END AS rfd_date
    FROM item_rfd
   GROUP BY order_id
)
SELECT ord.id AS "orderId", ord.form_no AS "formNo", ord.booking_ref AS "bookingRef",
       ord.k, to_char(r.rfd_date::date, 'YYYY-MM-DD') AS "rfdDate"
  FROM ord
  LEFT JOIN ord_rfd r ON r.order_id = ord.id
 WHERE (NOT $1::boolean OR r.rfd_date IS NOT NULL)
 ORDER BY r.rfd_date DESC NULLS LAST, ord.id DESC
`;

const INVOICES_SQL = `
SELECT zoho_invoice_id AS id, invoice_number AS number,
       customer_name AS customer, status, currency_code AS currency,
       total::float8 AS total, balance::float8 AS balance,
       exchange_rate::float8 AS "exchangeRate",
       consignment_number AS "consignmentNumber",
       regexp_replace(lower(btrim(order_number)), '[a-z]+$', '') AS k,
       to_char(invoice_date, 'YYYY-MM-DD') AS date,
       to_char(due_date, 'YYYY-MM-DD') AS "dueDate"
  FROM zoho_invoices
 WHERE regexp_replace(lower(btrim(order_number)), '[a-z]+$', '') = ANY($1::text[])
   AND ($2::date IS NULL OR invoice_date >= $2::date)
   AND ($3::date IS NULL OR invoice_date <= $3::date)
 ORDER BY invoice_date, id`;

const BILLS_SQL = `
SELECT zoho_bill_id AS id, bill_number AS number,
       vendor_name AS vendor, status, currency_code AS currency,
       total::float8 AS total, balance::float8 AS balance,
       exchange_rate::float8 AS "exchangeRate",
       consignment_number AS "consignmentNumber",
       regexp_replace(lower(btrim(order_number)), '[a-z]+$', '') AS k,
       to_char(bill_date, 'YYYY-MM-DD') AS date,
       to_char(due_date, 'YYYY-MM-DD') AS "dueDate"
  FROM zoho_bills
 WHERE regexp_replace(lower(btrim(order_number)), '[a-z]+$', '') = ANY($1::text[])
   AND ($2::date IS NULL OR bill_date >= $2::date)
   AND ($3::date IS NULL OR bill_date <= $3::date)
 ORDER BY bill_date, id`;

// Same conversion as getOrderBilling
const withAed = ({ k, ...d }) => {
  const rate =
    d.currency === BASE_CURRENCY ? 1 : Number(d.exchangeRate) || null;
  return {
    ...d,
    exchangeRate: rate,
    totalAed: rate ? Number((d.total * rate).toFixed(2)) : null,
    balanceAed: rate ? Number((d.balance * rate).toFixed(2)) : null,
  };
};

export const getFinanceOverview = async (req, res) => {
  const { from, to } = req.query;
  const rfdOnly = req.query.rfdOnly === "true";

  if ((from && !DATE_RE.test(from)) || (to && !DATE_RE.test(to))) {
    return res.status(400).json({
      success: false,
      message: "Dates must be in YYYY-MM-DD format",
    });
  }
  if (from && to && from > to) {
    return res.status(400).json({
      success: false,
      message: "'from' date must not be after 'to' date",
    });
  }

  try {
    const { rows: ords } = await pool.query(ORDERS_SQL, [rfdOnly]);
    const keys = ords.map((o) => o.k);

    const [invRes, billRes] = keys.length
      ? await Promise.all([
          pool.query(INVOICES_SQL, [keys, from || null, to || null]),
          pool.query(BILLS_SQL, [keys, from || null, to || null]),
        ])
      : [{ rows: [] }, { rows: [] }];

    const group = (rows) =>
      rows.reduce((m, d) => {
        (m[d.k] ||= []).push(withAed(d));
        return m;
      }, {});
    const invBy = group(invRes.rows);
    const billBy = group(billRes.rows);

    const ranged = !!(from || to);
    const orders = ords
      .map((o) => ({
        orderId: o.orderId,
        formNo: o.formNo,
        bookingRef: o.bookingRef,
        rfdDate: o.rfdDate,
        invoices: invBy[o.k] || [],
        vendorBills: billBy[o.k] || [],
      }))
      .filter((o) => !ranged || o.invoices.length + o.vendorBills.length > 0);

    return res.json({
      success: true,
      currency: BASE_CURRENCY,
      from: from || null,
      to: to || null,
      orders,
    });
  } catch (err) {
    logger.error("Failed to build finance overview", {
      err: err.response?.data || err.message,
    });
    return res
      .status(500)
      .json({ success: false, message: "Failed to load finance overview" });
  }
};
