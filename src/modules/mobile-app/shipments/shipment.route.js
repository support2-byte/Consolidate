import { Router } from "express";
import {
  getShipments,
  purchaseStorage,
  createDelivery,
  getShipment,
  getGatepasses,
} from "./shipment.controller.js";
import { requireAppAuth } from "../../../middleware/appAuthMiddleware.js";
import { getOrderByItemRef } from "../../orders/order.controller.js";

const router = Router();

router.get("/:id", requireAppAuth, getShipments);
router.get("/:id/shipment", getShipment);
router.get("/track/item/:ref", requireAppAuth, getOrderByItemRef);
router.get("/gatepass/:id", requireAppAuth, getGatepasses);
router.post("/purchase-storage/:id", requireAppAuth, purchaseStorage);
router.post("/delivery/:id", requireAppAuth, createDelivery);

export default router;
