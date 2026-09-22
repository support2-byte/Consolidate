import { Router } from "express";
import {
  confirmNgeniusPayment,
  createOverstayedInvoice,
  createWebNgeniusOrder,
  getDeliveryInvoices,
  getDropoffInvoices,
  getInvoicePayment,
  getOverstayInvoices,
  getStorageInvoices,
  verifyInvoiceOtp,
} from "./invoice.controller.js";
import { requireAuth } from "../auth/auth.middleware.js";

const router = Router();

router.post("/overstayed", requireAuth, createOverstayedInvoice);
router.get("/:invoiceId", getInvoicePayment);
router.post("/:invoiceId/verify-otp", verifyInvoiceOtp);
router.post("/:invoiceId/ngenius", createWebNgeniusOrder);
router.get("/:invoiceId/confirm", confirmNgeniusPayment);
router.get("/overstayed/list", requireAuth, getOverstayInvoices);
router.get("/storage/list", requireAuth, getStorageInvoices);
router.get("/delivery/list", requireAuth, getDeliveryInvoices);
router.get("/dropoff/list", requireAuth, getDropoffInvoices);

export default router;
