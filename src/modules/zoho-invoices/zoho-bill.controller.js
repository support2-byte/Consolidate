import axios from "axios";
import pool from "../../db/pool.js";
import logger from "../../services/logger.js";
import { getZohoAccessToken } from "../../services/getZohoAccessToken.js";
import {
  getBooksBaseUrl,
  CUSTOM_FIELD_LABELS,
  extractCustomField,
  mapWithLimit,
  withRetry,
  runZohoSync,
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

const fetchAllZohoBillList = async (token) => {
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
          },
        }),
      6,
    );
    if (zohoRes.data.code !== 0) {
      throw new Error(zohoRes.data.message || "Zoho returned an error");
    }
    all.push(...(zohoRes.data.bills || []));
    if (!zohoRes.data.page_context?.has_more_page) break;
    page++;
  }
  return all;
};

export const runZohoBillSync = async () => {
  const token = await getZohoAccessToken();
  const list = await fetchAllZohoBillList(token);

  const existing = await pool.query(
    "SELECT zoho_bill_id, last_modified_time FROM zoho_bills",
  );
  const known = new Map(
    existing.rows.map((r) => [r.zoho_bill_id, r.last_modified_time]),
  );
  const changed = list.filter(
    (b) => known.get(b.bill_id) !== b.last_modified_time,
  );

  await mapWithLimit(changed, 4, async (b) => {
    let orderNumber = null;
    let consignmentNumber = null;
    let lastModified = b.last_modified_time || null;
    try {
      const res = await withRetry(
        () =>
          axios.get(`${getBooksBaseUrl()}/bills/${b.bill_id}`, {
            headers: zohoHeaders(token),
            params: zohoParams(),
          }),
        6,
      );
      if (res.data.code !== 0) {
        throw new Error(res.data.message || "Zoho error");
      }
      const customFields = res.data.bill.custom_fields;
      orderNumber = extractCustomField(
        customFields,
        CUSTOM_FIELD_LABELS.orderNumber,
      );
      consignmentNumber = extractCustomField(
        customFields,
        CUSTOM_FIELD_LABELS.consignmentNumber,
      );
    } catch (err) {
      lastModified = null;
      logger.error("Failed to fetch Zoho bill detail", {
        err: err.response?.data || err.message,
        billId: b.bill_id,
      });
    }
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
      orderNumber,
      consignmentNumber,
      lastModified,
    ]);
  });

  if (list.length > 0) {
    await pool.query(
      "DELETE FROM zoho_bills WHERE zoho_bill_id <> ALL($1::text[])",
      [list.map((b) => b.bill_id)],
    );
  }

  return { synced: changed.length, total: list.length };
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

    const billCount = await pool.query(
      "SELECT COUNT(*)::int AS n FROM zoho_bills",
    );

    if (billCount.rows[0].n === 0) {
      try {
        await runZohoBillSync();
      } catch (err) {
        logger.warn("Initial Zoho bill sync failed", {
          err: err.response?.data || err.message,
        });
      }
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
                order_number,
                to_char(bill_date, 'YYYY-MM-DD') AS date,
                to_char(due_date, 'YYYY-MM-DD') AS "dueDate"
           FROM zoho_bills
          WHERE lower(btrim(consignment_number)) = $1
            AND lower(btrim(order_number)) = ANY($2::text[])
          ORDER BY bill_date, id`,
        [consKey, orderKeys],
      ),
    ]);

    const invByOrder = groupByOrder(invRes.rows);
    const billByOrder = groupByOrder(billRes.rows);
    const strip = ({ order_number, ...rest }) => rest;

    const rows = orders
      .filter((o) => {
        const k = key(o.rgl_booking_number);
        return (invByOrder[k] || []).length + (billByOrder[k] || []).length > 0;
      })
      .map((o) => {
        const k = key(o.rgl_booking_number);
        const invoices = invByOrder[k] || [];
        const vendorBills = billByOrder[k] || [];
        const activeInv = counted(invoices);
        const activeBills = counted(vendorBills);
        const currencies = new Set(
          [...activeInv, ...activeBills].map((d) => d.currency),
        );
        const mixedCurrency = currencies.size > 1;
        const invoiceTotal = sum(activeInv);
        const vendorTotal = sum(activeBills);
        return {
          orderId: o.id,
          bookingRef: o.booking_ref,
          formNo: o.rgl_booking_number,
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
        };
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

export const syncConsignmentBilling = async (req, res) => {
  const attempt = async (fn) => {
    try {
      await fn();
      return null;
    } catch (err) {
      logger.error("Failed to sync billing", {
        err: err.response?.data || err.message,
      });
      return err;
    }
  };

  const errors = [await attempt(runZohoSync), await attempt(runZohoBillSync)];

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
};
