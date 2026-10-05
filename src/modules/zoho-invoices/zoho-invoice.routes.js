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
} from "./zoho-bill.controller.js";
import { requireAuth } from "../auth/auth.middleware.js";

const router = express.Router();

router.get("/", requireAuth, getZohoInvoices);
router.post("/sync", requireAuth, syncZohoInvoices);
router.get(
  "/consignment/:consignmentId/billing",
  requireAuth,
  getConsignmentBilling,
);
router.post(
  "/consignment/:consignmentId/billing/sync",
  requireAuth,
  syncConsignmentBilling,
);
router.get("/bill/:id/pdf", requireAuth, getZohoBillPdf);
router.get("/:id/pdf", requireAuth, getZohoInvoicePdf);

export default router;
