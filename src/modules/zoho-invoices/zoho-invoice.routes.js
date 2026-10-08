import express from "express";
import {
  getZohoInvoicePdf,
  getZohoInvoices,
  syncZohoInvoices,
} from "./zoho-invoice.controller.js";
import {
  getConsignmentBilling,
  getZohoBillPdf,
  syncConsignmentBilling,
  getOrderBilling,
} from "./zoho-bill.controller.js";
import { requireAuth, requireRole } from "../auth/auth.middleware.js";

const router = express.Router();
const jobDetailsAccess = requireRole("super admin", "admin");

router.get("/", requireAuth, getZohoInvoices);
router.post("/sync", requireAuth, syncZohoInvoices);
router.get(
  "/consignment/:consignmentId/billing",
  requireAuth,
  jobDetailsAccess,
  getConsignmentBilling,
);
router.post(
  "/consignment/:consignmentId/billing/sync",
  requireAuth,
  jobDetailsAccess,
  syncConsignmentBilling,
);
router.get(
  "/order/:orderId/billing",
  requireAuth,
  jobDetailsAccess,
  getOrderBilling,
);
router.get("/bill/:id/pdf", requireAuth, jobDetailsAccess, getZohoBillPdf);
router.get("/:id/pdf", requireAuth, getZohoInvoicePdf);

export default router;
