import axios from "axios";
import pool from "../../db/pool.js";
import logger from "../../services/logger.js";
import { getZohoAccessToken } from "../../services/getZohoAccessToken.js";
import { getBooksBaseUrl, UPSERT_SQL } from "./zoho-invoice.controller.js";

const NON_DUE_STATUSES = ["paid", "void"];

const PARTY_BY_TYPE = {
  delivery: "receiver",
  storage: "receiver",
  overstayed: "receiver",
  dropoff: "sender",
};

const PARTY_SQL = {
  receiver: `SELECT receiver_name AS name FROM receivers WHERE receiver_ref = $1 AND order_id = $2 LIMIT 1`,
  sender: `SELECT sender_name AS name FROM senders WHERE sender_ref = $1 AND order_id = $2 LIMIT 1`,
};

const normalize = (v) =>
  String(v ?? "")
    .trim()
    .toLowerCase();

const normalizeName = (v) =>
  String(v ?? "")
    .toLowerCase()
    .replace(/[.,]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/^(mr|mrs|ms|miss|dr|eng|sir|madam)\b\s*/, "")
    .trim();

const ZOHO_DESCRIPTIONS = {
  delivery: "Delivery Charges",
  storage: "Storage Charges",
  dropoff: "Drop-off Pickup Charges",
  overstayed: "Cargo Overstay Charges",
};

const EMPTY_CONTEXT = {
  orderNumber: null,
  consignmentNumber: null,
  customerId: null,
  dueAmount: 0,
  dueInvoices: [],
};

const zohoHeaders = (token) => ({ Authorization: `Zoho-oauthtoken ${token}` });
const zohoParams = () => ({ organization_id: process.env.ZOHO_BOOKS_ORG_ID });

export const getZohoDueContext = async ({ type, itemRef, customerRef }) => {
  const party = PARTY_BY_TYPE[type];
  if (!party || !itemRef || !customerRef) return EMPTY_CONTEXT;

  const orderRes = await pool.query(
    `SELECT o.id, o.rgl_booking_number
       FROM order_items oi
       JOIN orders o ON o.id = oi.order_id
      WHERE oi.item_ref = $1
      LIMIT 1`,
    [itemRef],
  );
  const order = orderRes.rows[0];
  const orderNumber = order?.rgl_booking_number?.trim();
  if (!orderNumber) return EMPTY_CONTEXT;

  const partyRes = await pool.query(PARTY_SQL[party], [customerRef, order.id]);
  const customerName = partyRes.rows[0]?.name;
  if (!customerName) return EMPTY_CONTEXT;

  const { rows: candidates } = await pool.query(
    `SELECT zoho_invoice_id, invoice_number, customer_id, customer_name, consignment_number,
            status, currency_code,
            balance::float8 AS balance,
            to_char(invoice_date, 'YYYY-MM-DD') AS date,
            to_char(due_date, 'YYYY-MM-DD') AS due_date
       FROM zoho_invoices
      WHERE btrim(order_number) = $1
      ORDER BY invoice_date, id`,
    [orderNumber],
  );

  const linkRes = await pool.query(
    `SELECT DISTINCT c.consignment_number
       FROM container_assignment_history h
       JOIN consignments c ON c.id = h.consignment_id
      WHERE h.order_id = $1`,
    [order.id],
  );
  const linked = new Set(
    linkRes.rows.map((r) => normalize(r.consignment_number)),
  );

  const matches = candidates.filter((m) => {
    if (normalizeName(m.customer_name) !== normalizeName(customerName)) {
      return false;
    }
    if (!m.consignment_number) return true;
    const ok = linked.has(normalize(m.consignment_number));
    if (!ok) {
      logger.warn("Zoho invoice consignment not linked to order", {
        orderNumber,
        zohoInvoice: m.invoice_number,
        consignmentNumber: m.consignment_number,
      });
    }
    return ok;
  });

  const open = matches.filter((m) => !["paid", "void"].includes(m.status));
  if (open.length) {
    try {
      const token = await getZohoAccessToken();
      await Promise.all(
        open.map(async (m) => {
          const res = await axios.get(
            `${getBooksBaseUrl()}/invoices/${m.zoho_invoice_id}`,
            { headers: zohoHeaders(token), params: zohoParams() },
          );
          if (res.data.code !== 0) {
            throw new Error(res.data.message || "Zoho error");
          }
          m.status = res.data.invoice.status;
          m.balance = Number(res.data.invoice.balance ?? 0);
          await pool.query(
            `UPDATE zoho_invoices
                SET status = $1, balance = $2, synced_at = NOW()
              WHERE zoho_invoice_id = $3`,
            [m.status, m.balance, m.zoho_invoice_id],
          );
        }),
      );
    } catch (err) {
      logger.warn("Failed to refresh Zoho invoices before due calculation", {
        itemRef,
        err: err.response?.data || err.message,
      });
    }
  }

  const due = matches.filter(
    (m) =>
      !NON_DUE_STATUSES.includes(m.status) &&
      m.balance > 0 &&
      m.currency_code === "AED",
  );

  let customerId = matches[0]?.customer_id || null;
  if (!customerId) {
    const cust = await pool.query(
      `SELECT DISTINCT customer_id, customer_name FROM zoho_invoices`,
    );
    customerId =
      cust.rows.find(
        (c) => normalizeName(c.customer_name) === normalizeName(customerName),
      )?.customer_id || null;
  }

  return {
    orderNumber,
    consignmentNumber:
      matches.find((m) => m.consignment_number)?.consignment_number || null,
    customerId,
    dueAmount: Number(due.reduce((s, m) => s + m.balance, 0).toFixed(2)),
    dueInvoices: due.map((m) => ({
      zohoInvoiceId: m.zoho_invoice_id,
      invoiceNumber: m.invoice_number,
      date: m.date,
      dueDate: m.due_date,
      status: m.status,
      balance: m.balance,
    })),
  };
};

export const uploadInvoiceToZoho = async ({
  type,
  table,
  localId,
  invoiceId,
  context,
  rate,
  quantity = 1,
  taxPercent = 0,
  discount = 0,
}) => {
  if (!context.customerId) {
    await pool.query(
      `UPDATE ${table} SET zoho_sync_status = 'skipped' WHERE id = $1`,
      [localId],
    );
    logger.warn("Zoho upload skipped: no Zoho customer found", {
      invoiceId,
      orderNumber: context.orderNumber,
    });
    return;
  }

  try {
    const token = await getZohoAccessToken();
    const today = new Date().toISOString().slice(0, 10);
    const description = ZOHO_DESCRIPTIONS[type];

    const body = {
      customer_id: context.customerId,
      date: today,
      reference_number: invoiceId,
      line_items: [
        {
          name: description,
          description,
          rate,
          quantity: quantity || 1,
          ...(taxPercent > 0 && process.env.ZOHO_TAX_ID
            ? { tax_id: process.env.ZOHO_TAX_ID }
            : {}),
        },
      ],
      custom_fields: [
        {
          customfield_id: process.env.ZOHO_ORDER_NUMBER_FIELD_ID,
          value: context.orderNumber,
        },
        {
          customfield_id: process.env.ZOHO_CONSIGNMENT_FIELD_ID,
          value: context.consignmentNumber,
        },
      ].filter((f) => f.customfield_id && f.value),
      ...(discount > 0 && {
        discount,
        discount_type: "entity_level",
        is_discount_before_tax: true,
      }),
    };

    const created = await axios.post(`${getBooksBaseUrl()}/invoices`, body, {
      headers: zohoHeaders(token),
      params: zohoParams(),
    });
    if (created.data.code !== 0) {
      throw new Error(created.data.message || "Zoho error");
    }
    const z = created.data.invoice;

    await axios.post(
      `${getBooksBaseUrl()}/invoices/${z.invoice_id}/status/sent`,
      null,
      { headers: zohoHeaders(token), params: zohoParams() },
    );

    await pool.query(UPSERT_SQL, [
      z.invoice_id,
      z.invoice_number,
      z.customer_id,
      z.customer_name,
      z.reference_number,
      "sent",
      z.date || today,
      z.due_date || null,
      z.currency_code,
      z.total ?? 0,
      z.balance ?? z.total ?? 0,
      context.orderNumber,
      context.consignmentNumber,
      z.last_modified_time || null,
    ]);

    await pool.query(
      `UPDATE ${table}
          SET zoho_invoice_id = $1, zoho_sync_status = 'synced'
        WHERE id = $2`,
      [z.invoice_id, localId],
    );
  } catch (err) {
    logger.error("Failed to upload invoice to Zoho", {
      invoiceId,
      err: err.response?.data || err.message,
    });
    await pool.query(
      `UPDATE ${table} SET zoho_sync_status = 'failed' WHERE id = $1`,
      [localId],
    );
  }
};

export const uploadConsignmentInvoicesToZoho = async ({
  consignmentNumber,
  orderIds,
  consignmentValue,
}) => {
  const result = { created: [], skipped: [], failed: [] };
  const consNo = String(consignmentNumber ?? "").trim();
  if (!consNo || !orderIds?.length) return result;

  const { rows: orders } = await pool.query(
    `SELECT o.id,
            btrim(o.rgl_booking_number) AS order_number,
            COALESCE((
              SELECT SUM(oi.assigned_weight_kg)
                FROM order_items oi
               WHERE oi.order_id = o.id
            ), 0)::float8 AS weight
       FROM orders o
      WHERE o.id = ANY($1::int[])
      ORDER BY o.id`,
    [orderIds],
  );

  const totalValue = Number(consignmentValue) || 0;
  const totalWeight = orders.reduce((s, o) => s + o.weight, 0);
  const today = new Date().toISOString().slice(0, 10);

  let token = null;
  const customerCache = new Map();

  const resolveCustomerId = async (name) => {
    const k = normalizeName(name);
    if (customerCache.has(k)) return customerCache.get(k);
    token = token || (await getZohoAccessToken());
    const found = await axios.get(`${getBooksBaseUrl()}/contacts`, {
      headers: zohoHeaders(token),
      params: { ...zohoParams(), contact_name_contains: name },
    });
    const id =
      (found.data.contacts || []).find(
        (c) =>
          c.contact_type === "customer" && normalizeName(c.contact_name) === k,
      )?.contact_id || null;
    customerCache.set(k, id);
    return id;
  };

  const sideShares = (rows, weightOf) => {
    const total = rows.reduce((s, r) => s + weightOf(r), 0);
    return rows.map((r) => ({
      name: r.name,
      share: total > 0 ? weightOf(r) / total : 1 / rows.length,
    }));
  };

  for (const o of orders) {
    const ref = { orderId: o.id, orderNumber: o.order_number };
    try {
      if (!o.order_number) {
        result.skipped.push({ ...ref, reason: "No order number" });
        continue;
      }

      const { rows: existing } = await pool.query(
        `SELECT invoice_number, status FROM zoho_invoices
          WHERE btrim(order_number) = $1
            AND lower(btrim(consignment_number)) = lower($2)
            AND status <> 'void'
          LIMIT 1`,
        [o.order_number, consNo],
      );
      if (existing.length) {
        result.skipped.push({
          ...ref,
          reason: `Already in Zoho (${existing[0].invoice_number}, ${existing[0].status})`,
        });
        continue;
      }

      const orderAmount =
        totalWeight > 0
          ? (totalValue * o.weight) / totalWeight
          : totalValue / orders.length;
      if (!(orderAmount > 0)) {
        result.skipped.push({ ...ref, reason: "No invoice amount" });
        continue;
      }

      const [{ rows: receivers }, { rows: senders }] = await Promise.all([
        pool.query(
          `SELECT r.receiver_name AS name,
                  COALESCE(SUM(oi.assigned_weight_kg), 0)::float8 AS weight
             FROM receivers r
             LEFT JOIN order_items oi ON oi.receiver_id = r.id
            WHERE r.order_id = $1
            GROUP BY r.id, r.receiver_name`,
          [o.id],
        ),
        pool.query(
          `SELECT DISTINCT sender_name AS name
             FROM senders
            WHERE order_id = $1 AND sender_name IS NOT NULL`,
          [o.id],
        ),
      ]);

      const sides = [
        sideShares(receivers, (r) => r.weight),
        sideShares(senders, () => 1),
      ].filter((s) => s.length);

      const parties = new Map();
      sides.forEach((side) =>
        side.forEach((p) => {
          const k = normalizeName(p.name);
          if (!k) return;
          const cur = parties.get(k);
          parties.set(k, {
            name: cur?.name || p.name,
            amount: (cur?.amount || 0) + (orderAmount * p.share) / sides.length,
          });
        }),
      );

      if (!parties.size) {
        result.skipped.push({ ...ref, reason: "No sender or receiver" });
        continue;
      }

      for (const party of parties.values()) {
        const partyRef = { ...ref, customer: party.name };
        try {
          const amount = Number(party.amount.toFixed(2));
          if (!(amount > 0)) {
            result.skipped.push({ ...partyRef, reason: "No invoice amount" });
            continue;
          }

          const customerId = await resolveCustomerId(party.name);
          if (!customerId) {
            result.skipped.push({
              ...partyRef,
              reason: "Customer not found in Zoho",
            });
            continue;
          }

          const body = {
            customer_id: customerId,
            date: today,
            line_items: [
              {
                name: "Freight Charges",
                description: `Consignment ${consNo} - Order ${o.order_number}`,
                rate: amount,
                quantity: 1,
              },
            ],
            custom_fields: [
              {
                customfield_id: process.env.ZOHO_ORDER_NUMBER_FIELD_ID,
                value: o.order_number,
              },
              {
                customfield_id: process.env.ZOHO_CONSIGNMENT_FIELD_ID,
                value: consNo,
              },
            ].filter((f) => f.customfield_id && f.value),
          };

          const created = await axios.post(
            `${getBooksBaseUrl()}/invoices`,
            body,
            { headers: zohoHeaders(token), params: zohoParams() },
          );
          if (created.data.code !== 0) {
            throw new Error(created.data.message || "Zoho error");
          }
          const z = created.data.invoice;

          await axios.post(
            `${getBooksBaseUrl()}/invoices/${z.invoice_id}/status/sent`,
            null,
            { headers: zohoHeaders(token), params: zohoParams() },
          );

          await pool.query(UPSERT_SQL, [
            z.invoice_id,
            z.invoice_number,
            z.customer_id,
            z.customer_name,
            z.reference_number,
            "sent",
            z.date || today,
            z.due_date || null,
            z.currency_code,
            z.total ?? 0,
            z.balance ?? z.total ?? 0,
            o.order_number,
            consNo,
            z.last_modified_time || null,
          ]);

          result.created.push({ ...partyRef, invoiceNumber: z.invoice_number });
        } catch (err) {
          logger.error("Failed to upload consignment invoice to Zoho", {
            ...partyRef,
            consignmentNumber: consNo,
            err: err.response?.data || err.message,
          });
          result.failed.push({
            ...partyRef,
            reason: err.response?.data?.message || err.message,
          });
        }
      }
    } catch (err) {
      logger.error("Failed to process order for Zoho invoices", {
        ...ref,
        consignmentNumber: consNo,
        err: err.response?.data || err.message,
      });
      result.failed.push({ ...ref, reason: err.message });
    }
  }

  return result;
};
