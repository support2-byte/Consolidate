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
  syncInvoicesForOrder,
  stripConsignmentPrefix,
  toZohoConsignment,
  baseOrderNumber,
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
    order_number, consignment_number, last_modified_time, exchange_rate, synced_at
  )
  VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,NOW())
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
    exchange_rate = COALESCE(EXCLUDED.exchange_rate, zoho_bills.exchange_rate),
    synced_at = NOW()
`;

const BILL_ORDER_FIELD =
  process.env.ZOHO_BILL_ORDER_FILTER || "cf_bb_order_number";
const BILL_CONSIGNMENT_FIELD =
  process.env.ZOHO_BILL_CONSIGNMENT_FILTER || "cf_bb_consignment_number";

const billCustom = (bill, apiName, label) => {
  const direct = bill?.[apiName];
  if (direct != null && String(direct).trim() !== "") {
    return String(direct).trim();
  }
  const f = (bill?.custom_fields || []).find(
    (c) =>
      c.api_name === apiName ||
      (label && c.label?.trim().toLowerCase() === label),
  );
  return f?.value != null && String(f.value).trim() !== ""
    ? String(f.value).trim()
    : null;
};

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

const syncBillRows = async (list, token, syncedBy = null) => {
  if (!list.length) return 0;

  const existing = await pool.query(
    "SELECT zoho_bill_id, last_modified_time, exchange_rate FROM zoho_bills WHERE zoho_bill_id = ANY($1::text[])",
    [list.map((b) => b.bill_id)],
  );
  const known = new Map(existing.rows.map((r) => [r.zoho_bill_id, r]));
  const changed = list.filter((b) => {
    const k = known.get(b.bill_id);
    return (
      !k ||
      k.last_modified_time !== b.last_modified_time ||
      k.exchange_rate == null
    );
  });

  await mapWithLimit(changed, 2, async (b) => {
    let rate = b.currency_code === "AED" ? 1 : Number(b.exchange_rate) || null;
    let orderNumber = billCustom(b, BILL_ORDER_FIELD, "");
    let consignmentNumber = billCustom(b, BILL_CONSIGNMENT_FIELD, "");

    if (!rate || !orderNumber || !consignmentNumber) {
      try {
        const r = await withRetry(() =>
          axios.get(`${getBooksBaseUrl()}/bills/${b.bill_id}`, {
            headers: zohoHeaders(token),
            params: zohoParams(),
          }),
        );
        const detail = r.data?.bill;
        rate = rate || Number(detail?.exchange_rate) || null;
        orderNumber = orderNumber || billCustom(detail, BILL_ORDER_FIELD, "");
        consignmentNumber =
          consignmentNumber || billCustom(detail, BILL_CONSIGNMENT_FIELD, "");
      } catch (err) {
        logger.error("Failed to fetch Zoho bill detail", {
          billId: b.bill_id,
          status: err.response?.status,
          err: err.response?.data || err.message,
        });
      }
    }
    logger.info("[BILL] rate", {
      bill: b.bill_number,
      currency: b.currency_code,
      rate,
      orderNumber,
      consignmentNumber,
    });

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
      stripConsignmentPrefix(consignmentNumber) || null,
      b.last_modified_time || null,
      rate,
    ]);
  });

  if (syncedBy) {
    await pool.query(
      `UPDATE zoho_bills SET synced_by = $1, synced_at = NOW()
        WHERE zoho_bill_id = ANY($2::text[])`,
      [syncedBy, list.map((b) => b.bill_id)],
    );
  }

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

const syncBillsForOrder = async (formNo, token, syncedBy = null) => {
  const wantedForm = key(formNo);
  if (!wantedForm) return 0;

  try {
    const list = await fetchAllZohoBillList(
      token,
      { [BILL_ORDER_FIELD]: formNo },
      1,
    );

    const matched = list.filter((b) => {
      const v = String(b[BILL_ORDER_FIELD] ?? "").trim();
      return v && baseOrderNumber(v) === wantedForm;
    });

    logger.info("[BILL-ORDER] lookup", {
      formNo,
      returned: list.length,
      matched: matched.length,
      sampleOrderValues: list
        .slice(0, 3)
        .map((b) => b[BILL_ORDER_FIELD] ?? null),
    });

    if (!matched.length) return 0;
    return await syncBillRows(matched, token, syncedBy);
  } catch (err) {
    logger.error("[BILL-ORDER] lookup failed", {
      formNo,
      status: err.response?.status,
      err: err.response?.data || err.message,
    });
    return 0;
  }
};

const syncBillsForConsignment = async (
  consignmentNumber,
  token,
  syncedBy = null,
) => {
  const wanted = stripConsignmentPrefix(consignmentNumber).toLowerCase();
  if (!wanted) return 0;

  const MAX_PAGES = 5;
  const matched = [];
  let scanned = 0;

  for (let page = 1; page <= MAX_PAGES; page++) {
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
            [BILL_CONSIGNMENT_FIELD]: stripConsignmentPrefix(consignmentNumber),
          },
        }),
      6,
    );
    if (zohoRes.data.code !== 0) {
      throw new Error(zohoRes.data.message || "Zoho returned an error");
    }

    const bills = zohoRes.data.bills || [];
    scanned += bills.length;
    matched.push(
      ...bills.filter(
        (b) =>
          stripConsignmentPrefix(b[BILL_CONSIGNMENT_FIELD]).toLowerCase() ===
          wanted,
      ),
    );

    if (page === 1) {
      logger.info("[BILL-BY-CONS] sample", {
        field: BILL_CONSIGNMENT_FIELD,
        sampleValues: bills
          .slice(0, 5)
          .map((b) => b[BILL_CONSIGNMENT_FIELD] ?? null),
      });
    }

    if (!zohoRes.data.page_context?.has_more_page) break;
  }

  logger.info("[BILL-BY-CONS]", {
    consignmentNumber,
    scanned,
    matched: matched.length,
  });
  return matched.length ? syncBillRows(matched, token, syncedBy) : 0;
};

const CONSIGNMENT_SYNC_TTL_MS = 60_000;
const lastConsignmentSync = new Map();

const runConsignmentSync = async (
  consignmentNumber,
  { force = false, syncedBy = null } = {},
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
      await fn(consignmentNumber, token, syncedBy);
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

const inflightOrderSyncs = new Map();

const syncOrderFromZoho = (formNo, syncedBy = null) => {
  const k = key(formNo);
  if (!inflightOrderSyncs.has(k)) {
    const run = async (label, fn) => {
      try {
        await fn();
        return null;
      } catch (err) {
        logger.error("Failed to sync order billing", {
          label,
          formNo,
          status: err.response?.status,
          err: err.response?.data || err.message,
        });
        return err;
      }
    };

    inflightOrderSyncs.set(
      k,
      (async () => {
        let token;
        try {
          token = await getZohoAccessToken();
        } catch (err) {
          logger.error("Failed to get Zoho token for order sync", {
            err: err.response?.data || err.message,
          });
          return [err, err];
        }
        return Promise.all([
          run("invoices", () => syncInvoicesForOrder(formNo, syncedBy)),
          run("bills", () => syncBillsForOrder(formNo, token, syncedBy)),
        ]);
      })().finally(() => inflightOrderSyncs.delete(k)),
    );
  }
  return inflightOrderSyncs.get(k);
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
    const k = baseOrderNumber(d.order_number);
    (acc[k] = acc[k] || []).push(d);
    return acc;
  }, {});

const counted = (docs) =>
  docs.filter((d) => !EXCLUDED_STATUSES.includes(d.status));

const sum = (docs) => Number(docs.reduce((s, d) => s + d.total, 0).toFixed(2));

const strip = ({ order_number, ...rest }) => rest;

const paymentStatusOf = (invoices) => {
  const live = invoices.filter((d) => d.status !== "void");
  if (!live.length) return "no_invoice";

  const issued = live.filter((d) => d.status !== "draft");
  if (!issued.length) return "unpaid";

  if (issued.every((d) => d.balance <= 0)) return "paid";
  return issued.some((d) => d.balance < d.total) ? "partially_paid" : "unpaid";
};

const normalizeName = (v) =>
  String(v ?? "")
    .toLowerCase()
    .replace(/[.,]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/^(mr|mrs|ms|miss|dr|eng|sir|madam)\b\s*/, "")
    .trim();

const computeReceiverPayments = (receivers, invoices, senderName = "") => {
  const byCustomer = {};
  for (const inv of invoices) {
    const k = normalizeName(inv.customer);
    (byCustomer[k] = byCustomer[k] || []).push(inv);
  }
  const senderKey = normalizeName(senderName);
  return Object.fromEntries(
    receivers.map((r) => {
      const keys = new Set([normalizeName(r.receiver_name)]);
      if (senderKey) keys.add(senderKey);
      const docs = [...keys].flatMap((k) => byCustomer[k] || []);
      return [r.id, paymentStatusOf(docs)];
    }),
  );
};

export const loadPaymentData = async (orderId) => {
  const [orderRes, recRes, invRes] = await Promise.all([
    pool.query(
      `SELECT o.rgl_booking_number, s.sender_name
         FROM orders o
         LEFT JOIN senders s ON s.order_id = o.id
        WHERE o.id = $1`,
      [orderId],
    ),
    pool.query("SELECT id, receiver_name FROM receivers WHERE order_id = $1", [
      orderId,
    ]),
    pool.query(
      `SELECT invoice_number AS number, customer_name AS customer, status,
              total::float8 AS total, balance::float8 AS balance,
              order_number, consignment_number,
              to_char(last_payment_date, 'YYYY-MM-DD') AS "paidDate"
         FROM zoho_invoices
        WHERE regexp_replace(lower(btrim(order_number)), '[a-z]+$', '') =
              (SELECT NULLIF(lower(btrim(rgl_booking_number)), '')
                 FROM orders WHERE id = $1)`,
      [orderId],
    ),
  ]);
  if (!orderRes.rows[0]) return null;
  return {
    orderKey: key(orderRes.rows[0].rgl_booking_number),
    senderName: orderRes.rows[0].sender_name,
    receivers: recRes.rows,
    invoices: invRes.rows,
  };
};

export const getReceiverPayments = async (orderId, data) => {
  const d = data === undefined ? await loadPaymentData(orderId) : data;
  if (!d) return null;
  return computeReceiverPayments(d.receivers, d.invoices, d.senderName);
};

export const getReceiverApprovals = async (orderId) => {
  const { rows } = await pool.query(
    `SELECT id, payment_approved, payment_approved_by,
          payment_approved_by_email, payment_approved_at
     FROM receivers
    WHERE order_id = $1`,
    [orderId],
  );
  return Object.fromEntries(
    rows.map((r) => [
      r.id,
      {
        approved: r.payment_approved === true,
        approvedBy: r.payment_approved_by,
        approvedByEmail: r.payment_approved_by_email,
        approvedAt: r.payment_approved_at,
      },
    ]),
  );
};

const countPaid = (map) =>
  Object.values(map || {}).filter((s) => s === "paid").length;

export const getReceiverPaymentMeta = async (orderId, data) => {
  const d = data === undefined ? await loadPaymentData(orderId) : data;
  if (!d) return {};
  const senderKey = normalizeName(d.senderName);
  const recRes = { rows: d.receivers };
  const invRes = { rows: d.invoices };

  const byCustomer = {};
  for (const inv of invRes.rows) {
    const k = normalizeName(inv.customer);
    (byCustomer[k] = byCustomer[k] || []).push(inv);
  }

  return Object.fromEntries(
    recRes.rows.map((r) => {
      const keys = new Set([normalizeName(r.receiver_name)]);
      if (senderKey) keys.add(senderKey);
      const docs = [...keys].flatMap((k) => byCustomer[k] || []);
      const live = docs.filter((d) => !EXCLUDED_STATUSES.includes(d.status));
      const status = paymentStatusOf(docs);
      const paidDates = live
        .map((d) => d.paidDate)
        .filter(Boolean)
        .sort();
      return [
        r.id,
        {
          status,
          invoiceCount: live.length,
          total: sum(live),
          balance: Number(live.reduce((s, d) => s + d.balance, 0).toFixed(2)),
          paidDate: status === "paid" ? paidDates.pop() || null : null,
        },
      ];
    }),
  );
};

const partOf = (orderNumber) =>
  (
    String(orderNumber ?? "")
      .trim()
      .toLowerCase()
      .match(/[a-z]+$/)?.[0] ?? ""
  ).toUpperCase();

const consKey = (v) => stripConsignmentPrefix(v).toLowerCase();

export const getReceiverParts = async (orderId, data) => {
  const d = data === undefined ? await loadPaymentData(orderId) : data;
  if (!d) return {};
  const senderKey = normalizeName(d.senderName);
  const recRes = { rows: d.receivers };
  const invRes = { rows: d.invoices };

  const byCustomer = {};
  for (const inv of invRes.rows) {
    const k = normalizeName(inv.customer);
    (byCustomer[k] = byCustomer[k] || []).push(inv);
  }

  return Object.fromEntries(
    recRes.rows.map((r) => {
      const keys = new Set([normalizeName(r.receiver_name)]);
      if (senderKey) keys.add(senderKey);
      const docs = [...keys].flatMap((k) => byCustomer[k] || []);

      const groups = {};
      for (const d of docs) (groups[partOf(d.order_number)] ||= []).push(d);

      const parts = Object.keys(groups)
        .sort()
        .map((part) => {
          const g = groups[part];
          const live = g.filter((d) => !EXCLUDED_STATUSES.includes(d.status));
          const paidDates = live
            .map((d) => d.paidDate)
            .filter(Boolean)
            .sort();
          const status = paymentStatusOf(g);
          return {
            part,
            status,
            invoices: live.map((d) => d.number),
            consignments: [
              ...new Set(
                live
                  .map((d) => stripConsignmentPrefix(d.consignment_number))
                  .filter(Boolean),
              ),
            ],
            total: sum(live),
            balance: Number(live.reduce((s, d) => s + d.balance, 0).toFixed(2)),
            paidDate: status === "paid" ? paidDates.pop() || null : null,
          };
        });
      return [r.id, parts];
    }),
  );
};

export const itemReleaseState = (
  parts,
  itemConsignments,
  { approved = false, receiverPaid = false } = {},
) => {
  if (approved) return { ok: true };
  if (!(parts || []).some((p) => p.part)) {
    return receiverPaid
      ? { ok: true }
      : { ok: false, reason: "Payment pending" };
  }
  const keys = (itemConsignments || []).map(consKey).filter(Boolean);
  const hit = parts.filter((p) =>
    (p.consignments || []).some((c) => keys.includes(consKey(c))),
  );
  const applicable = hit.length
    ? hit
    : parts.filter((p) => !(p.consignments || []).length);
  if (!applicable.length) {
    return { ok: false, reason: "No invoice for this shipment yet" };
  }
  const bad = applicable.filter((p) => p.status !== "paid");
  return bad.length
    ? {
        ok: false,
        reason: `Part ${bad.map((p) => p.part || "-").join(", ")} not paid`,
      }
    : { ok: true };
};

export const getUnreleasedItems = async (orderId, orderItemIds) => {
  const ids = [...new Set((orderItemIds || []).map(Number).filter(Boolean))];
  if (!ids.length) return [];

  const data = await loadPaymentData(orderId);
  const [payments, approvals, parts, itemsRes] = await Promise.all([
    getReceiverPayments(orderId, data),
    getReceiverApprovals(orderId),
    getReceiverParts(orderId, data),
    pool.query(
      `SELECT oi.id, oi.item_ref, oi.receiver_id,
              COALESCE(array_agg(DISTINCT cons.consignment_number)
                FILTER (WHERE cons.consignment_number IS NOT NULL), '{}') AS consignments
         FROM order_items oi
         JOIN receivers r ON r.id = oi.receiver_id AND r.order_id = $1
         LEFT JOIN (
           SELECT DISTINCT ON (h.detail_id, h.cid)
                  h.detail_id, h.consignment_id, h.action_type
             FROM container_assignment_history h
            WHERE h.detail_id = ANY($2::int[])
            ORDER BY h.detail_id, h.cid, h.id DESC
         ) a ON a.detail_id = oi.id AND a.action_type = 'ASSIGN'
         LEFT JOIN consignments cons ON cons.id = a.consignment_id
        WHERE oi.id = ANY($2::int[])
        GROUP BY oi.id, oi.item_ref, oi.receiver_id`,
      [orderId, ids],
    ),
  ]);
  if (!payments) return null;

  const found = new Map(itemsRes.rows.map((r) => [Number(r.id), r]));
  const blocked = [];
  for (const id of ids) {
    const row = found.get(id);
    if (!row) {
      blocked.push({
        orderItemId: id,
        itemRef: null,
        reason: "Item not on this order",
      });
      continue;
    }
    const rid = row.receiver_id;
    const st = itemReleaseState(parts[rid] || [], row.consignments, {
      approved: approvals[rid]?.approved === true,
      receiverPaid: payments[rid] === "paid",
    });
    if (!st.ok) {
      blocked.push({
        orderItemId: id,
        itemRef: row.item_ref,
        receiverId: rid,
        reason: st.reason,
      });
    }
  }
  return blocked;
};

export const getLastPaymentSync = async (orderId) => {
  const { rows } = await pool.query(
    `SELECT synced_by_email AS "syncedByEmail", status, message,
            invoices_found AS "invoicesFound", invoices_updated AS "invoicesUpdated",
            paid_before AS "paidBefore", paid_after AS "paidAfter",
            receivers_total AS "receiversTotal", synced_at AS "syncedAt"
       FROM order_payment_sync_log
      WHERE order_id = $1
      ORDER BY id DESC
      LIMIT 1`,
    [orderId],
  );
  return rows[0] || null;
};

const logPaymentSync = async (orderId, user, d) => {
  try {
    await pool.query(
      `INSERT INTO order_payment_sync_log
         (order_id, synced_by_id, synced_by_email, status, message,
          invoices_found, invoices_updated, paid_before, paid_after, receivers_total)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
      [
        orderId,
        user?.id ?? null,
        user?.email ?? null,
        d.status,
        d.message,
        d.found ?? 0,
        d.updated ?? 0,
        d.paidBefore ?? 0,
        d.paidAfter ?? 0,
        d.total ?? 0,
      ],
    );
  } catch (err) {
    logger.error("Failed to write payment sync log", {
      err: err.message,
      orderId,
    });
  }
};

export const getUnreleasedReceivers = async (orderId, receiverIds) => {
  const [payments, approvals] = await Promise.all([
    getReceiverPayments(orderId),
    getReceiverApprovals(orderId),
  ]);
  if (!payments) return null;
  return receiverIds.filter(
    (id) => payments[id] !== "paid" && !approvals[id]?.approved,
  );
};

async function withUserNames(receiverApprovals = {}, lastSync = null) {
  const emails = new Set();
  const ids = new Set();
  const addEmail = (e) => e && emails.add(String(e).trim().toLowerCase());
  const addId = (i) => Number(i) > 0 && ids.add(Number(i));

  Object.values(receiverApprovals || {}).forEach((a) => {
    addEmail(a?.approvedByEmail);
    addId(a?.approvedBy);
  });
  if (lastSync) {
    addEmail(lastSync.syncedByEmail);
    addId(lastSync.syncedBy);
  }
  if (!emails.size && !ids.size) return { receiverApprovals, lastSync };

  const { rows } = await pool.query(
    `SELECT id, name, LOWER(TRIM(email)) AS email
       FROM users
      WHERE LOWER(TRIM(email)) = ANY($1::text[]) OR id = ANY($2::int[])`,
    [[...emails], [...ids]],
  );
  const byEmail = new Map(rows.map((r) => [r.email, r.name]));
  const byId = new Map(rows.map((r) => [Number(r.id), r.name]));
  const nameOf = (email, id) =>
    byEmail.get(
      String(email || "")
        .trim()
        .toLowerCase(),
    ) ||
    byId.get(Number(id)) ||
    null;

  return {
    receiverApprovals: Object.fromEntries(
      Object.entries(receiverApprovals || {}).map(([k, a]) => [
        k,
        a
          ? { ...a, approvedByName: nameOf(a.approvedByEmail, a.approvedBy) }
          : a,
      ]),
    ),
    lastSync: lastSync
      ? {
          ...lastSync,
          syncedByName: nameOf(lastSync.syncedByEmail, lastSync.syncedBy),
        }
      : lastSync,
  };
}

export const getOrderPaymentStatus = async (req, res) => {
  const orderId = parseInt(req.params.orderId, 10);
  if (isNaN(orderId) || orderId <= 0) {
    return res
      .status(400)
      .json({ success: false, message: "Invalid order ID" });
  }
  try {
    const data = await loadPaymentData(orderId);
    const [receiverApprovals, lastSync] = await Promise.all([
      getReceiverApprovals(orderId),
      getLastPaymentSync(orderId),
    ]);
    const receiverPayments = await getReceiverPayments(orderId, data);
    const receiverPaymentMeta = await getReceiverPaymentMeta(orderId, data);
    const receiverParts = await getReceiverParts(orderId, data);
    if (!receiverPayments) {
      return res
        .status(404)
        .json({ success: false, message: "Order not found" });
    }
    const named = await withUserNames(receiverApprovals, lastSync);
    return res.json({
      success: true,
      receiverPayments,
      receiverApprovals: named.receiverApprovals,
      receiverPaymentMeta,
      receiverParts,
      lastSync: named.lastSync,
    });
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

export const refreshOrderPaymentStatus = async (req, res) => {
  const orderId = parseInt(req.params.orderId, 10);
  if (isNaN(orderId) || orderId <= 0) {
    return res
      .status(400)
      .json({ success: false, message: "Invalid order ID" });
  }
  try {
    const orderRes = await pool.query(
      "SELECT rgl_booking_number FROM orders WHERE id = $1",
      [orderId],
    );
    if (!orderRes.rows[0]) {
      return res
        .status(404)
        .json({ success: false, message: "Order not found" });
    }

    const formNo = String(orderRes.rows[0].rgl_booking_number ?? "").trim();
    if (!formNo) {
      return res.status(400).json({
        success: false,
        message: "Order has no form number to check",
      });
    }

    const paidBefore = countPaid(await getReceiverPayments(orderId));

    let updated = 0;
    try {
      updated = await syncInvoicesForOrder(formNo, req.user?.email ?? null);
    } catch (err) {
      logger.error("Failed to sync order invoices for payment status", {
        formNo,
        status: err.response?.status,
        err: err.response?.data || err.message,
      });
      const rateLimited = err.response?.status === 429;
      const message = rateLimited
        ? "Zoho rate limit reached, please try again shortly"
        : "Could not reach Zoho. Check your network connection and try again.";
      await logPaymentSync(orderId, req.user, {
        status: rateLimited ? "rate_limited" : "failed",
        message,
        paidBefore,
        paidAfter: paidBefore,
      });
      return res.status(rateLimited ? 429 : 502).json({
        success: false,
        message,
        lastSync: await getLastPaymentSync(orderId),
      });
    }

    const data = await loadPaymentData(orderId);
    const receiverApprovals = await getReceiverApprovals(orderId);
    const receiverPayments = await getReceiverPayments(orderId, data);
    const receiverPaymentMeta = await getReceiverPaymentMeta(orderId, data);
    const receiverParts = await getReceiverParts(orderId, data);

    const found = data.invoices.length;
    const total = Object.keys(receiverPayments).length;
    const paidAfter = countPaid(receiverPayments);

    const message =
      found === 0
        ? "No invoice found in Zoho for this order"
        : paidAfter > paidBefore
          ? `Updated: ${paidBefore} → ${paidAfter} of ${total} receiver(s) paid`
          : `No change: ${paidAfter} of ${total} receiver(s) paid`;

    await logPaymentSync(orderId, req.user, {
      status: "success",
      message,
      found,
      updated,
      paidBefore,
      paidAfter,
      total,
    });

    const named = await withUserNames(
      receiverApprovals,
      await getLastPaymentSync(orderId),
    );
    return res.json({
      success: true,
      receiverPayments,
      receiverApprovals: named.receiverApprovals,
      receiverPaymentMeta,
      receiverParts,
      lastSync: named.lastSync,
    });
  } catch (err) {
    logger.error("Failed to refresh order payment status", {
      err: err.response?.data || err.message,
      orderId,
    });
    return res
      .status(500)
      .json({ success: false, message: "Failed to check payment status" });
  }
};

export const setReceiverPaymentApproval = async (req, res) => {
  const orderId = parseInt(req.params.orderId, 10);
  const { receiverId } = req.params;
  const { approved } = req.body;

  if (isNaN(orderId) || orderId <= 0) {
    return res
      .status(400)
      .json({ success: false, message: "Invalid order ID" });
  }
  if (typeof approved !== "boolean") {
    return res
      .status(400)
      .json({ success: false, message: "approved must be true or false" });
  }

  try {
    const { rows } = await pool.query(
      `UPDATE receivers
      SET payment_approved = $1,
          payment_approved_by = CASE WHEN $1 THEN $2::int ELSE NULL END,
          payment_approved_by_email = CASE WHEN $1 THEN $3::text ELSE NULL END,
          payment_approved_at = CASE WHEN $1 THEN NOW() ELSE NULL END
    WHERE id = $4 AND order_id = $5
    RETURNING payment_approved, payment_approved_by, payment_approved_by_email, payment_approved_at`,
      [
        approved,
        req.user?.id ?? null,
        req.user?.email ?? null,
        receiverId,
        orderId,
      ],
    );
    if (!rows[0]) {
      return res
        .status(404)
        .json({ success: false, message: "Receiver not found on this order" });
    }

    logger.info("Receiver payment approval changed", {
      orderId,
      receiverId,
      approved,
      by: req.user?.id,
    });

    return res.json({
      success: true,
      approval: {
        approved: rows[0].payment_approved,
        approvedBy: rows[0].payment_approved_by,
        approvedByEmail: rows[0].payment_approved_by_email,
        approvedAt: rows[0].payment_approved_at,
      },
    });
  } catch (err) {
    logger.error("Failed to update receiver payment approval", {
      err: err.response?.data || err.message,
      orderId,
      receiverId,
    });
    return res
      .status(500)
      .json({ success: false, message: "Failed to update approval" });
  }
};

const BASE_CURRENCY = "AED";

const withAed = (d) => {
  const rate =
    d.currency === BASE_CURRENCY ? 1 : Number(d.exchangeRate) || null;
  return {
    ...d,
    exchangeRate: rate,
    totalAed: rate ? Number((d.total * rate).toFixed(2)) : null,
    balanceAed: rate ? Number((d.balance * rate).toFixed(2)) : null,
  };
};

const buildBillingRow = (order, rawInvoices, rawBills) => {
  const invoices = rawInvoices.map(withAed);
  const vendorBills = rawBills.map(withAed);
  const activeInv = counted(invoices);
  const liveInv = invoices.filter((d) => d.status !== "void");
  const liveBills = vendorBills.filter((d) => d.status !== "void");

  const rateMissing = [...liveInv, ...liveBills].some(
    (d) => d.totalAed == null,
  );
  const sumOf = (docs, fn) =>
    Number(docs.reduce((s, d) => s + (fn(d) || 0), 0).toFixed(2));

  const invoiceTotal = sumOf(liveInv, (d) => d.totalAed);
  const vendorTotal = sumOf(liveBills, (d) => d.totalAed);
  const received = sumOf(liveInv, (d) => d.totalAed - d.balanceAed);
  const toCollect = sumOf(liveInv, (d) => d.balanceAed);
  const paidOut = sumOf(liveBills, (d) => d.totalAed - d.balanceAed);
  const toPay = sumOf(liveBills, (d) => d.balanceAed);

  const expectedProfit = Number((invoiceTotal - vendorTotal).toFixed(2));
  const currentProfit = Number((received - paidOut).toFixed(2));

  return {
    orderId: order.id,
    bookingRef: order.booking_ref,
    formNo: order.rgl_booking_number,
    invoices: invoices.map(strip),
    vendorBills: vendorBills.map(strip),
    invoiceTotal,
    vendorTotal,
    received,
    toCollect,
    paidOut,
    toPay,
    currency: BASE_CURRENCY,
    mixedCurrency: false,
    rateMissing,
    gross: rateMissing ? null : expectedProfit,
    expectedProfit: rateMissing ? null : expectedProfit,
    currentProfit: rateMissing ? null : currentProfit,
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

    const billConsKey = stripConsignmentPrefix(consKey);

    const [invRes, billRes] = await Promise.all([
      pool.query(
        `SELECT zoho_invoice_id AS id, invoice_number AS number,
                customer_name AS customer, status, currency_code AS currency,
                total::float8 AS total, balance::float8 AS balance,
                exchange_rate::float8 AS "exchangeRate",
                order_number,
                to_char(invoice_date, 'YYYY-MM-DD') AS date,
                to_char(due_date, 'YYYY-MM-DD') AS "dueDate",
                synced_at,
                (SELECT u.name FROM users u
                  WHERE LOWER(TRIM(u.email)) = LOWER(TRIM(zoho_invoices.synced_by))
                  LIMIT 1) AS synced_by
           FROM zoho_invoices
          WHERE lower(btrim(consignment_number)) = $1
          ORDER BY invoice_date, id`,
        [billConsKey],
      ),
      pool.query(
        `SELECT zoho_bill_id AS id, bill_number AS number,
                vendor_name AS vendor, status, currency_code AS currency,
                total::float8 AS total, balance::float8 AS balance,
                exchange_rate::float8 AS "exchangeRate",
                order_number,
                to_char(bill_date, 'YYYY-MM-DD') AS date,
                to_char(due_date, 'YYYY-MM-DD') AS "dueDate",
                synced_at,
                (SELECT u.name FROM users u
                  WHERE LOWER(TRIM(u.email)) = LOWER(TRIM(zoho_bills.synced_by))
                  LIMIT 1) AS synced_by
           FROM zoho_bills
          WHERE lower(btrim(consignment_number)) = $1
          ORDER BY bill_date, id`,
        [billConsKey],
      ),
    ]);

    const invByOrder = groupByOrder(invRes.rows);
    const billByOrder = groupByOrder(billRes.rows);
    const orderKeySet = new Set(orderKeys);

    const rows = orders
      .filter((o) => {
        const k = key(o.rgl_booking_number);
        return (invByOrder[k] || []).length + (billByOrder[k] || []).length > 0;
      })
      .map((o) => {
        const k = key(o.rgl_booking_number);
        return buildBillingRow(o, invByOrder[k] || [], billByOrder[k] || []);
      });

    const withForm = (d) => ({ ...d, formNumber: d.order_number || null });
    const looseInv = invRes.rows
      .filter((d) => !orderKeySet.has(baseOrderNumber(d.order_number)))
      .map(withForm);
    const looseBills = billRes.rows
      .filter((d) => !orderKeySet.has(baseOrderNumber(d.order_number)))
      .map(withForm);

    if (looseInv.length + looseBills.length > 0) {
      rows.push(
        buildBillingRow(
          { id: null, booking_ref: null, rgl_booking_number: null },
          looseInv,
          looseBills,
        ),
      );
    }

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
    const invoicesOnly = String(req.query?.invoicesOnly ?? "") === "1";

    const [invRes, billRes, recRes] = await Promise.all([
      pool.query(
        `SELECT zoho_invoice_id AS id, invoice_number AS number,
                customer_name AS customer, status, currency_code AS currency,
                total::float8 AS total, balance::float8 AS balance, exchange_rate::float8 AS "exchangeRate",
                order_number, consignment_number AS "consignmentNumber",
                to_char(invoice_date, 'YYYY-MM-DD') AS date,
                to_char(due_date, 'YYYY-MM-DD') AS "dueDate",
                synced_at,
                (SELECT u.name FROM users u
                  WHERE LOWER(TRIM(u.email)) = LOWER(TRIM(zoho_invoices.synced_by))
                  LIMIT 1) AS synced_by
           FROM zoho_invoices
          WHERE regexp_replace(lower(btrim(order_number)), '[a-z]+$', '') = $1
          ORDER BY invoice_date, id`,
        [orderKey],
      ),
      invoicesOnly
        ? { rows: [] }
        : pool.query(
            `SELECT zoho_bill_id AS id, bill_number AS number,
                vendor_name AS vendor, status, currency_code AS currency,
                total::float8 AS total, balance::float8 AS balance, exchange_rate::float8 AS "exchangeRate",
                order_number, consignment_number AS "consignmentNumber",
                to_char(bill_date, 'YYYY-MM-DD') AS date,
                to_char(due_date, 'YYYY-MM-DD') AS "dueDate",
                synced_at,
                (SELECT u.name FROM users u
                  WHERE LOWER(TRIM(u.email)) = LOWER(TRIM(zoho_bills.synced_by))
                  LIMIT 1) AS synced_by
           FROM zoho_bills
          WHERE regexp_replace(lower(btrim(order_number)), '[a-z]+$', '') = $1
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
      row: buildBillingRow(order, invRes.rows, billRes.rows),
      receiverPayments: computeReceiverPayments(recRes.rows, invRes.rows),
      receiverApprovals: await getReceiverApprovals(orderId),
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

export const syncOrderBilling = async (req, res) => {
  const orderId = parseInt(req.params.orderId, 10);
  if (isNaN(orderId) || orderId <= 0) {
    return res
      .status(400)
      .json({ success: false, message: "Invalid order ID" });
  }

  try {
    const orderRes = await pool.query(
      "SELECT rgl_booking_number FROM orders WHERE id = $1",
      [orderId],
    );
    if (!orderRes.rows[0]) {
      return res
        .status(404)
        .json({ success: false, message: "Order not found" });
    }

    const formNo = String(orderRes.rows[0].rgl_booking_number ?? "").trim();
    if (!formNo) {
      return res.status(400).json({
        success: false,
        message: "Order has no form number to sync",
      });
    }

    const errors = await syncOrderFromZoho(formNo, req.user?.email ?? null);

    if (errors.every(Boolean)) {
      const rateLimited = errors.some((e) => e.response?.status === 429);
      return res.status(rateLimited ? 429 : 502).json({
        success: false,
        message: rateLimited
          ? "Zoho rate limit reached, please try again shortly"
          : "Could not reach Zoho. Check your network connection and try again.",
      });
    }

    return getOrderBilling(req, res);
  } catch (err) {
    logger.error("Failed to sync order billing", {
      err: err.response?.data || err.message,
      orderId,
    });
    return res
      .status(500)
      .json({ success: false, message: "Failed to sync billing data" });
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
      syncedBy: req.user?.email ?? null,
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
