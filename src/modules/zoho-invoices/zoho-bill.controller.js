import axios from "axios";
import pool from "../../db/pool.js";
import logger from "../../services/logger.js";
import { getZohoAccessToken } from "../../services/getZohoAccessToken.js";
import {
  getBooksBaseUrl,
  getConsignmentFormNumbers,
  mapWithLimit,
  withRetry,
  syncInvoicesForConsignment,
} from "./zoho-invoice.controller.js";

const EXCLUDED_STATUSES = ["void", "draft"];
const key = (v) =>
  String(v ?? "")
    .trim()
    .toLowerCase();

export const zohoHeaders = (token) => ({
  Authorization: `Zoho-oauthtoken ${token}`,
});
export const zohoParams = () => ({
  organization_id: process.env.ZOHO_BOOKS_ORG_ID,
});

const BILL_UPSERT_SQL = `
  INSERT INTO zoho_bills (
    zoho_bill_id, bill_number, vendor_id, vendor_name, reference_number,
    status, bill_date, due_date, currency_code, total, balance,
    order_number, consignment_number, last_modified_time, synced_at
  )
  VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,NOW())
  ON CONFLICT (zoho_bill_id) DO UPDATE SET
    bill_number = EXCLUDED.bill_number,
    vendor_id = EXCLUDED.vendor_id,
    vendor_name = EXCLUDED.vendor_name,
    reference_number = EXCLUDED.reference_number,
    status = EXCLUDED.status,
    bill_date = EXCLUDED.bill_date,
    due_date = EXCLUDED.due_date,
    currency_code = EXCLUDED.currency_code,
    total = EXCLUDED.total,
    balance = EXCLUDED.balance,
    order_number = EXCLUDED.order_number,
    consignment_number = EXCLUDED.consignment_number,
    last_modified_time = EXCLUDED.last_modified_time,
    synced_at = NOW()
`;

const fetchAllZohoBillList = async (
  token,
  filters = {},
  maxPages = Infinity,
) => {
  const all = [];
  let page = 1;
  while (true) {
    const zohoRes = await withRetry(
      () =>
        axios.get(`${getBooksBaseUrl()}/bills`, {
          headers: zohoHeaders(token),
          params: {
            ...zohoParams(),
            page,
            per_page: 200,
            sort_column: "created_time",
            sort_order: "D",
            ...filters,
          },
        }),
      6,
    );
    if (zohoRes.data.code !== 0) {
      throw new Error(zohoRes.data.message || "Zoho returned an error");
    }
    all.push(...(zohoRes.data.bills || []));
    if (!zohoRes.data.page_context?.has_more_page) break;
    if (page >= maxPages) {
      throw new Error(
        "Zoho filter returned too many bills. Check the custom field API name (ZOHO_BILL_CONSIGNMENT_FILTER)",
      );
    }
    page++;
  }
  return all;
};

const syncBillRows = async (list, token) => {
  if (!list.length) return 0;

  const existing = await pool.query(
    "SELECT zoho_bill_id, last_modified_time FROM zoho_bills WHERE zoho_bill_id = ANY($1::text[])",
    [list.map((b) => b.bill_id)],
  );
  const known = new Map(
    existing.rows.map((r) => [r.zoho_bill_id, r.last_modified_time]),
  );
  const changed = list.filter(
    (b) => known.get(b.bill_id) !== b.last_modified_time,
  );

  await mapWithLimit(changed, 4, async (b) => {
    await pool.query(BILL_UPSERT_SQL, [
      b.bill_id,
      b.bill_number,
      b.vendor_id,
      b.vendor_name,
      b.reference_number,
      b.status,
      b.date || null,
      b.due_date || null,
      b.currency_code,
      b.total ?? 0,
      b.balance ?? 0,
      b.bill_number,
      null,
      b.last_modified_time || null,
    ]);
  });

  return changed.length;
};

export const runZohoBillSync = async () => {
  const token = await getZohoAccessToken();
  const list = await fetchAllZohoBillList(token);
  const synced = await syncBillRows(list, token);

  if (list.length > 0) {
    await pool.query(
      "DELETE FROM zoho_bills WHERE zoho_bill_id <> ALL($1::text[])",
      [list.map((b) => b.bill_id)],
    );
  }

  return { synced, total: list.length };
};

const syncBillsForConsignment = async (consignmentNumber, token) => {
  const forms = await getConsignmentFormNumbers(consignmentNumber);
  const counts = await mapWithLimit(forms, 2, async (formNo) => {
    const list = await fetchAllZohoBillList(token, { bill_number: formNo }, 1);
    return syncBillRows(list, token);
  });
  logger.info("Zoho bills fetched for consignment", {
    consignmentNumber,
    forms: forms.length,
  });
  return counts.reduce((s, n) => s + n, 0);
};

const CONSIGNMENT_SYNC_TTL_MS = 60_000;
const lastConsignmentSync = new Map();

const runConsignmentSync = async (
  consignmentNumber,
  { force = false } = {},
) => {
  const k = key(consignmentNumber);
  if (
    !force &&
    Date.now() - (lastConsignmentSync.get(k) || 0) < CONSIGNMENT_SYNC_TTL_MS
  ) {
    return [null, null];
  }
  lastConsignmentSync.set(k, Date.now());

  let token;
  try {
    token = await getZohoAccessToken();
  } catch (err) {
    logger.error("Failed to get Zoho token for consignment sync", {
      err: err.response?.data || err.message,
    });
    return [err, err];
  }

  const run = async (label, fn) => {
    try {
      await fn(consignmentNumber, token);
      return null;
    } catch (err) {
      logger.error("Failed to sync consignment billing", {
        label,
        err: err.response?.data || err.message,
        status: err.response?.status,
        consignmentNumber,
      });
      return err;
    }
  };

  const results = await Promise.all([
    run("invoices", syncInvoicesForConsignment),
    run("bills", syncBillsForConsignment),
  ]);
  if (results.every(Boolean)) lastConsignmentSync.delete(k);
  return results;
};

const inflightSyncs = new Map();
const syncConsignmentFromZoho = (consignmentNumber, opts) => {
  const k = key(consignmentNumber);
  if (!inflightSyncs.has(k)) {
    inflightSyncs.set(
      k,
      runConsignmentSync(consignmentNumber, opts).finally(() =>
        inflightSyncs.delete(k),
      ),
    );
  }
  return inflightSyncs.get(k);
};

export const getZohoBillPdf = async (req, res) => {
  const { id } = req.params;
  try {
    const token = await getZohoAccessToken();
    const zohoRes = await axios.get(
      `${getBooksBaseUrl()}/bills/${encodeURIComponent(id)}`,
      {
        headers: zohoHeaders(token),
        params: { ...zohoParams(), accept: "pdf" },
        responseType: "arraybuffer",
      },
    );
    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `inline; filename="bill-${id}.pdf"`);
    return res.send(Buffer.from(zohoRes.data));
  } catch (err) {
    logger.error("Failed to fetch Zoho bill PDF", {
      err: err.response?.status || err.message,
      billId: id,
    });
    return res
      .status(500)
      .json({ success: false, message: "Failed to fetch bill PDF" });
  }
};

const groupByOrder = (docs) =>
  docs.reduce((acc, d) => {
    const k = key(d.order_number);
    (acc[k] = acc[k] || []).push(d);
    return acc;
  }, {});

const counted = (docs) =>
  docs.filter((d) => !EXCLUDED_STATUSES.includes(d.status));

const sum = (docs) => Number(docs.reduce((s, d) => s + d.total, 0).toFixed(2));

const strip = ({ order_number, ...rest }) => rest;

const paymentStatusOf = (activeInvoices) => {
  if (!activeInvoices.length) return "no_invoice";
  if (activeInvoices.every((d) => d.balance <= 0)) return "paid";
  return activeInvoices.some((d) => d.balance < d.total)
    ? "partially_paid"
    : "unpaid";
};

const normalizeName = (v) =>
  String(v ?? "")
    .toLowerCase()
    .replace(/[.,]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/^(mr|mrs|ms|miss|dr|eng|sir|madam)\b\s*/, "")
    .trim();

const computeReceiverPayments = (receivers, invoices) => {
  const byCustomer = {};
  for (const inv of counted(invoices)) {
    const k = normalizeName(inv.customer);
    (byCustomer[k] = byCustomer[k] || []).push(inv);
  }
  return Object.fromEntries(
    receivers.map((r) => [
      r.id,
      paymentStatusOf(byCustomer[normalizeName(r.receiver_name)] || []),
    ]),
  );
};

export const getReceiverPayments = async (orderId) => {
  const orderRes = await pool.query(
    "SELECT rgl_booking_number FROM orders WHERE id = $1",
    [orderId],
  );
  if (!orderRes.rows[0]) return null;
  const orderKey = key(orderRes.rows[0].rgl_booking_number);

  const [recRes, invRes] = await Promise.all([
    pool.query("SELECT id, receiver_name FROM receivers WHERE order_id = $1", [
      orderId,
    ]),
    orderKey
      ? pool.query(
          `SELECT customer_name AS customer, status,
                  total::float8 AS total, balance::float8 AS balance
             FROM zoho_invoices
            WHERE lower(btrim(order_number)) = $1`,
          [orderKey],
        )
      : { rows: [] },
  ]);
  return computeReceiverPayments(recRes.rows, invRes.rows);
};

export const getOrderPaymentStatus = async (req, res) => {
  const orderId = parseInt(req.params.orderId, 10);
  if (isNaN(orderId) || orderId <= 0) {
    return res
      .status(400)
      .json({ success: false, message: "Invalid order ID" });
  }
  try {
    const receiverPayments = await getReceiverPayments(orderId);
    if (!receiverPayments) {
      return res
        .status(404)
        .json({ success: false, message: "Order not found" });
    }
    return res.json({ success: true, receiverPayments });
  } catch (err) {
    logger.error("Failed to build order payment status", {
      err: err.response?.data || err.message,
      orderId,
    });
    return res
      .status(500)
      .json({ success: false, message: "Failed to load payment status" });
  }
};

const buildBillingRow = (order, invoices, vendorBills) => {
  const activeInv = counted(invoices);
  const activeBills = counted(vendorBills);
  const currencies = new Set(
    [...activeInv, ...activeBills].map((d) => d.currency),
  );
  const mixedCurrency = currencies.size > 1;
  const invoiceTotal = sum(activeInv);
  const vendorTotal = sum(activeBills);
  return {
    orderId: order.id,
    bookingRef: order.booking_ref,
    formNo: order.rgl_booking_number,
    invoices: invoices.map(strip),
    vendorBills: vendorBills.map(strip),
    invoiceTotal,
    vendorTotal,
    currency:
      [...currencies][0] ||
      invoices[0]?.currency ||
      vendorBills[0]?.currency ||
      null,
    mixedCurrency,
    gross: mixedCurrency
      ? null
      : Number((invoiceTotal - vendorTotal).toFixed(2)),
    paymentStatus: paymentStatusOf(activeInv),
  };
};

export const getConsignmentBilling = async (req, res) => {
  const consignmentId = parseInt(req.params.consignmentId, 10);
  if (isNaN(consignmentId) || consignmentId <= 0) {
    return res
      .status(400)
      .json({ success: false, message: "Invalid consignment ID" });
  }

  try {
    const cons = await pool.query(
      "SELECT consignment_number FROM consignments WHERE id = $1",
      [consignmentId],
    );
    const consignmentNumber = cons.rows[0]?.consignment_number;
    if (!consignmentNumber) {
      return res
        .status(404)
        .json({ success: false, message: "Consignment not found" });
    }

    const ordersRes = await pool.query(
      `SELECT DISTINCT o.id, o.booking_ref, o.rgl_booking_number
         FROM container_assignment_history h
         JOIN orders o ON o.id = h.order_id
        WHERE h.consignment_id = $1
          AND btrim(COALESCE(o.rgl_booking_number, '')) <> ''
        ORDER BY o.id`,
      [consignmentId],
    );
    const orders = ordersRes.rows;
    const orderKeys = orders.map((o) => key(o.rgl_booking_number));
    const consKey = key(consignmentNumber);

    const [invRes, billRes] = await Promise.all([
      pool.query(
        `SELECT zoho_invoice_id AS id, invoice_number AS number,
                customer_name AS customer, status, currency_code AS currency,
                total::float8 AS total, balance::float8 AS balance,
                order_number,
                to_char(invoice_date, 'YYYY-MM-DD') AS date,
                to_char(due_date, 'YYYY-MM-DD') AS "dueDate"
           FROM zoho_invoices
          WHERE lower(btrim(consignment_number)) = $1
            AND lower(btrim(order_number)) = ANY($2::text[])
          ORDER BY invoice_date, id`,
        [consKey, orderKeys],
      ),
      pool.query(
        `SELECT zoho_bill_id AS id, bill_number AS number,
                vendor_name AS vendor, status, currency_code AS currency,
                total::float8 AS total, balance::float8 AS balance,
                bill_number AS order_number,
                to_char(bill_date, 'YYYY-MM-DD') AS date,
                to_char(due_date, 'YYYY-MM-DD') AS "dueDate"
           FROM zoho_bills
          WHERE lower(btrim(bill_number)) = ANY($1::text[])
          ORDER BY bill_date, id`,
        [orderKeys],
      ),
    ]);

    const invByOrder = groupByOrder(invRes.rows);
    const billByOrder = groupByOrder(billRes.rows);

    const rows = orders
      .filter((o) => {
        const k = key(o.rgl_booking_number);
        return (invByOrder[k] || []).length + (billByOrder[k] || []).length > 0;
      })
      .map((o) => {
        const k = key(o.rgl_booking_number);
        return buildBillingRow(o, invByOrder[k] || [], billByOrder[k] || []);
      });

    return res.json({ success: true, consignmentNumber, rows });
  } catch (err) {
    logger.error("Failed to build consignment billing", {
      err: err.response?.data || err.message,
      consignmentId,
    });
    return res
      .status(500)
      .json({ success: false, message: "Failed to load billing data" });
  }
};

export const getOrderBilling = async (req, res) => {
  const orderId = parseInt(req.params.orderId, 10);
  if (isNaN(orderId) || orderId <= 0) {
    return res
      .status(400)
      .json({ success: false, message: "Invalid order ID" });
  }

  try {
    const orderRes = await pool.query(
      "SELECT id, booking_ref, rgl_booking_number FROM orders WHERE id = $1",
      [orderId],
    );
    const order = orderRes.rows[0];
    if (!order) {
      return res
        .status(404)
        .json({ success: false, message: "Order not found" });
    }

    const orderKey = key(order.rgl_booking_number);
    if (!orderKey) return res.json({ success: true, row: null });

    const [invRes, billRes, recRes] = await Promise.all([
      pool.query(
        `SELECT zoho_invoice_id AS id, invoice_number AS number,
                customer_name AS customer, status, currency_code AS currency,
                total::float8 AS total, balance::float8 AS balance,
                order_number, consignment_number AS "consignmentNumber",
                to_char(invoice_date, 'YYYY-MM-DD') AS date,
                to_char(due_date, 'YYYY-MM-DD') AS "dueDate"
           FROM zoho_invoices
          WHERE lower(btrim(order_number)) = $1
          ORDER BY invoice_date, id`,
        [orderKey],
      ),
      pool.query(
        `SELECT zoho_bill_id AS id, bill_number AS number,
                vendor_name AS vendor, status, currency_code AS currency,
                total::float8 AS total, balance::float8 AS balance,
                order_number, consignment_number AS "consignmentNumber",
                to_char(bill_date, 'YYYY-MM-DD') AS date,
                to_char(due_date, 'YYYY-MM-DD') AS "dueDate"
           FROM zoho_bills
          WHERE lower(btrim(order_number)) = $1
          ORDER BY bill_date, id`,
        [orderKey],
      ),
      pool.query(
        "SELECT id, receiver_name FROM receivers WHERE order_id = $1",
        [orderId],
      ),
    ]);

    return res.json({
      success: true,
      receiverPayments: computeReceiverPayments(recRes.rows, invRes.rows),
    });
  } catch (err) {
    logger.error("Failed to build order billing", {
      err: err.response?.data || err.message,
      orderId,
    });
    return res
      .status(500)
      .json({ success: false, message: "Failed to load billing data" });
  }
};

export const syncConsignmentBilling = async (req, res) => {
  const consignmentId = parseInt(req.params.consignmentId, 10);
  if (isNaN(consignmentId) || consignmentId <= 0) {
    return res
      .status(400)
      .json({ success: false, message: "Invalid consignment ID" });
  }

  try {
    const cons = await pool.query(
      "SELECT consignment_number FROM consignments WHERE id = $1",
      [consignmentId],
    );
    const consignmentNumber = cons.rows[0]?.consignment_number;
    if (!consignmentNumber) {
      return res
        .status(404)
        .json({ success: false, message: "Consignment not found" });
    }

    const errors = await syncConsignmentFromZoho(consignmentNumber, {
      force: true,
    });

    if (errors.every(Boolean)) {
      const rateLimited = errors.some((e) => e.response?.status === 429);
      return res.status(rateLimited ? 429 : 502).json({
        success: false,
        message: rateLimited
          ? "Zoho rate limit reached, please try again shortly"
          : "Could not reach Zoho. Check your network connection and try again.",
      });
    }

    return getConsignmentBilling(req, res);
  } catch (err) {
    logger.error("Failed to sync consignment billing", {
      err: err.response?.data || err.message,
      consignmentId,
    });
    return res
      .status(500)
      .json({ success: false, message: "Failed to sync billing data" });
  }
};
