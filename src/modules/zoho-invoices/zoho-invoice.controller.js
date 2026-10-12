import axios from "axios";
import { getZohoAccessToken } from "../../services/getZohoAccessToken.js";
import logger from "../../services/logger.js";
import pool from "../../db/pool.js";

export const CUSTOM_FIELD_LABELS = {
  orderNumber: "Order Number",
  consignmentNumber: "Consignment Number",
};

export const extractCustomField = (customFields = [], label) => {
  const match = customFields.find(
    (f) => f.label?.trim().toLowerCase() === label.toLowerCase(),
  );
  return match?.value || null;
};

export const mapWithLimit = async (items, limit, fn) => {
  const results = new Array(items.length);
  let i = 0;
  const workers = Array.from(
    { length: Math.min(limit, items.length) },
    async () => {
      while (i < items.length) {
        const idx = i++;
        results[idx] = await fn(items[idx], idx);
      }
    },
  );
  await Promise.all(workers);
  return results;
};

export const getBooksBaseUrl = () => {
  const domain = (
    process.env.ZOHO_API_DOMAIN || "https://www.zohoapis.com"
  ).replace(/\/+$/, "");
  return domain.includes("/books/") ? domain : `${domain}/books/v3`;
};

const ALLOWED_STATUS = [
  "draft",
  "sent",
  "viewed",
  "unpaid",
  "partially_paid",
  "overdue",
  "paid",
  "void",
];

const invoiceDetailCache = new Map();
const DETAIL_CACHE_TTL_MS = 60_000;

const getInvoiceDetailCached = async (invoiceId, token) => {
  const hit = invoiceDetailCache.get(invoiceId);
  if (hit && Date.now() - hit.at < DETAIL_CACHE_TTL_MS) return hit.data;

  const res = await axios.get(`${getBooksBaseUrl()}/invoices/${invoiceId}`, {
    headers: { Authorization: `Zoho-oauthtoken ${token}` },
    params: { organization_id: process.env.ZOHO_BOOKS_ORG_ID },
  });
  if (res.data.code !== 0) throw new Error(res.data.message || "Zoho error");

  const data = res.data.invoice;
  invoiceDetailCache.set(invoiceId, { data, at: Date.now() });
  if (invoiceDetailCache.size > 500) {
    invoiceDetailCache.delete(invoiceDetailCache.keys().next().value);
  }
  return data;
};

export const getZohoInvoices = async (req, res) => {
  try {
    const count = await pool.query(
      "SELECT COUNT(*)::int AS n FROM zoho_invoices",
    );
    if (count.rows[0].n === 0) await runZohoSync();

    const { rows } = await pool.query(`
      SELECT
        zoho_invoice_id AS invoice_id,
        invoice_number,
        customer_id,
        customer_name,
        reference_number,
        status,
        to_char(invoice_date, 'YYYY-MM-DD') AS date,
        to_char(due_date, 'YYYY-MM-DD') AS due_date,
        currency_code,
        total::float8 AS total,
        balance::float8 AS balance,
        order_number,
        consignment_number,
        payment_mode,
        (status = 'paid') AS is_paid
      FROM zoho_invoices
      ORDER BY invoice_date DESC NULLS LAST, id DESC
    `);
    return res.json({ success: true, invoices: rows });
  } catch (err) {
    logger.error("Failed to fetch Zoho invoices", {
      err: err.response?.data || err.message,
    });
    return res.status(500).json({
      success: false,
      message: err.message || "Failed to fetch invoices",
    });
  }
};

export const getZohoInvoicePdf = async (req, res) => {
  const { id } = req.params;

  try {
    const token = await getZohoAccessToken();

    const zohoRes = await axios.get(
      `${getBooksBaseUrl()}/invoices/${encodeURIComponent(id)}`,
      {
        headers: { Authorization: `Zoho-oauthtoken ${token}` },
        params: {
          organization_id: process.env.ZOHO_BOOKS_ORG_ID,
          accept: "pdf",
        },
        responseType: "arraybuffer",
      },
    );

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader(
      "Content-Disposition",
      `inline; filename="invoice-${id}.pdf"`,
    );
    return res.send(Buffer.from(zohoRes.data));
  } catch (err) {
    logger.error("Failed to fetch Zoho invoice PDF", {
      err: err.response?.status || err.message,
      invoiceId: id,
    });
    return res.status(500).json({
      success: false,
      message: "Failed to fetch invoice PDF",
    });
  }
};

const TRANSIENT_CODES = ["EAI_AGAIN", "ECONNRESET", "ETIMEDOUT"];

export const withRetry = async (fn, attempts = 6) => {
  for (let i = 1; ; i++) {
    try {
      return await fn();
    } catch (err) {
      if (i >= attempts || !TRANSIENT_CODES.includes(err.code)) throw err;
      await new Promise((r) => setTimeout(r, 500 * i));
    }
  }
};

const fetchAllZohoInvoiceList = async (token) => {
  const all = [];
  let page = 1;
  while (true) {
    const zohoRes = await withRetry(() =>
      axios.get(`${getBooksBaseUrl()}/invoices`, {
        headers: { Authorization: `Zoho-oauthtoken ${token}` },
        params: {
          organization_id: process.env.ZOHO_BOOKS_ORG_ID,
          page,
          per_page: 200,
          sort_column: "created_time",
          sort_order: "D",
        },
      }),
    );

    if (zohoRes.data.code !== 0) {
      throw new Error(zohoRes.data.message || "Zoho returned an error");
    }

    all.push(...(zohoRes.data.invoices || []));

    if (!zohoRes.data.page_context?.has_more_page) break;
    page++;
  }
  return all;
};

export const UPSERT_SQL = `
  INSERT INTO zoho_invoices (
    zoho_invoice_id, invoice_number, customer_id, customer_name,
    reference_number, status, invoice_date, due_date, currency_code,
    total, balance, order_number, consignment_number, last_modified_time,
    synced_at
  )
  VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,NOW())
  ON CONFLICT (zoho_invoice_id) DO UPDATE SET
    invoice_number = EXCLUDED.invoice_number,
    customer_id = EXCLUDED.customer_id,
    customer_name = EXCLUDED.customer_name,
    reference_number = EXCLUDED.reference_number,
    status = EXCLUDED.status,
    invoice_date = EXCLUDED.invoice_date,
    due_date = EXCLUDED.due_date,
    currency_code = EXCLUDED.currency_code,
    total = EXCLUDED.total,
    balance = EXCLUDED.balance,
    order_number = EXCLUDED.order_number,
    consignment_number = EXCLUDED.consignment_number,
    last_modified_time = EXCLUDED.last_modified_time,
    synced_at = NOW()
`;

export const runZohoSync = async () => {
  const token = await getZohoAccessToken();
  const list = await fetchAllZohoInvoiceList(token);

  const existing = await pool.query(
    "SELECT zoho_invoice_id, last_modified_time FROM zoho_invoices",
  );
  const known = new Map(
    existing.rows.map((r) => [r.zoho_invoice_id, r.last_modified_time]),
  );
  const changed = list.filter(
    (i) => known.get(i.invoice_id) !== i.last_modified_time,
  );

  await mapWithLimit(changed, 4, async (i) => {
    let orderNumber = null;
    let consignmentNumber = null;
    let paymentMode = null;
    let lastModified = i.last_modified_time || null;
    try {
      const detail = await getInvoiceDetailCached(i.invoice_id, token);
      orderNumber = extractCustomField(
        detail.custom_fields,
        CUSTOM_FIELD_LABELS.orderNumber,
      );
      consignmentNumber = extractCustomField(
        detail.custom_fields,
        CUSTOM_FIELD_LABELS.consignmentNumber,
      );

      let payments = detail.payments;
      if (!payments?.length && ["paid", "partially_paid"].includes(i.status)) {
        const pr = await withRetry(() =>
          axios.get(`${getBooksBaseUrl()}/invoices/${i.invoice_id}/payments`, {
            headers: { Authorization: `Zoho-oauthtoken ${token}` },
            params: { organization_id: process.env.ZOHO_BOOKS_ORG_ID },
          }),
        );
        payments = pr.data.payments;
      }
      paymentMode =
        [
          ...new Set(
            (payments || []).map((p) => p.payment_mode).filter(Boolean),
          ),
        ].join(", ") || null;
    } catch (err) {
      lastModified = null;
      logger.error("Failed to fetch Zoho invoice detail", {
        err: err.response?.data || err.message,
        invoiceId: i.invoice_id,
      });
    }
    await pool.query(UPSERT_SQL, [
      i.invoice_id,
      i.invoice_number,
      i.customer_id,
      i.customer_name,
      i.reference_number,
      i.status,
      i.date || null,
      i.due_date || null,
      i.currency_code,
      i.total ?? 0,
      i.balance ?? 0,
      orderNumber,
      consignmentNumber,
      lastModified,
    ]);
    if (paymentMode) {
      await pool.query(
        `UPDATE zoho_invoices SET payment_mode = $1 WHERE zoho_invoice_id = $2`,
        [paymentMode, i.invoice_id],
      );
    }
  });

  if (list.length > 0) {
    await pool.query(
      "DELETE FROM zoho_invoices WHERE zoho_invoice_id <> ALL($1::text[])",
      [list.map((i) => i.invoice_id)],
    );
  }

  return { synced: changed.length, total: list.length };
};

export const syncZohoInvoices = async (req, res) => {
  try {
    const result = await runZohoSync();
    return res.json({ success: true, ...result });
  } catch (err) {
    logger.error("Failed to sync Zoho invoices", {
      err: err.response?.data || err.message,
    });
    const rateLimited = err.response?.status === 429;
    return res.status(rateLimited ? 429 : 500).json({
      success: false,
      message: rateLimited
        ? "Zoho rate limit reached, please try again shortly"
        : err.response?.data?.message ||
          err.message ||
          "Failed to sync invoices",
    });
  }
};
