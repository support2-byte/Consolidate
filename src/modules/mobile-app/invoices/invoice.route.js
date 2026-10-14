import { Router } from "express";
import {
  confirmNgeniusPayment,
  createNgeniusOrder,
  getInvoiceDetails,
  getInvoices,
} from "./invoice.controller.js";
import { requireAppAuth } from "../../../middleware/appAuthMiddleware.js";

const router = Router();

router.get("/:id", requireAppAuth, getInvoices);
router.get("/details/:invoiceId", getInvoiceDetails);
router.post("/:invoiceId/ngenius/order", createNgeniusOrder);
router.get("/:orderReferenceId/confirm", confirmNgeniusPayment);

export default router;
