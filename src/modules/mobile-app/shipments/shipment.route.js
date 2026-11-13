import { Router } from "express";
import {
  getShipments,
  purchaseStorage,
  createDelivery,
  getShipment,
  getGatepasses,
  createDropOff,
  getShipmentRoles,
} from "./shipment.controller.js";
import { requireAppAuth } from "../../../middleware/appAuthMiddleware.js";
import {
  getOrderByItemRef,
  getOrderByOrderId,
} from "../../orders/order.controller.js";
import { requireFeature } from "../../../middleware/featureAccess.js";

const router = Router();

router.get("/:id", requireAppAuth, getShipments);
router.get("/:id/shipment", requireAppAuth, getShipment);
router.get("/:id/roles", requireAppAuth, getShipmentRoles);
router.get("/track/item/:ref", requireAppAuth, getOrderByItemRef);
router.get("/track/order/:ref", requireAppAuth, getOrderByOrderId);
router.get(
  "/gatepass/:id",
  requireAppAuth,
  requireFeature("gatepass"),
  getGatepasses,
);
router.post(
  "/purchase-storage/:id",
  requireAppAuth,
  requireFeature("storage"),
  purchaseStorage,
);
router.post(
  "/delivery/:id",
  requireAppAuth,
  requireFeature("delivery"),
  createDelivery,
);
router.post("/drop-off/:id", requireAppAuth, createDropOff);

export default router;
