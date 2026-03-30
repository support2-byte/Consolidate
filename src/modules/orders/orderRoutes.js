import express from "express";
import multer from "multer";                  // only if you define multer inline (optional)
import path from "path";                      // can remove if not used elsewhere
import upload from "../../middleware/upload.js"; // ← your Cloudinary multer instance
import { requireAuth } from "../../modules/auth/auth.middleware.js";

import {
  createOrder,
  updateOrder,
  getOrders,
  getOrderById,
  getOrderByTrackingId,
  getOrderByItemRef,
  assignContainersToOrders,
  updateReceiverStatus,
  updateSpecificItemsStatus,
  getOrdersConsignments,
  getOrderByOrderId,
  getMyOrdersByRef,
  removeContainerAssignments,
  getOrderByRglBookingNo,
  assignOneContainerToMultipleReceivers,
  assignContainersBatch,
  sendShipmentEmail,
  removeReceiver,
  removeOrderItem,
} from "./order.controller.js";

const router = express.Router();

// ────────────────────────────────────────────────
// Protected routes – require authentication
// ────────────────────────────────────────────────

// POST /api/orders - Create new order (with file uploads)
router.post(
  "/",
  requireAuth,
  upload.fields([
    { name: "attachments", maxCount: 10 },
    { name: "gatepass", maxCount: 10 },
  ]),
  createOrder
);

// PUT /api/orders/:id - Update order (with possible new files)
router.put(
  "/:id",
  requireAuth,
  upload.fields([
    { name: "attachments", maxCount: 10 },
    { name: "gatepass", maxCount: 10 },
  ]),
  updateOrder
);

// PUT /api/orders/:id/shipping - Update shipping details (if it also accepts files)
router.put(
  "/:id/shipping",
  requireAuth,
  upload.fields([
    { name: "attachments", maxCount: 10 },
    { name: "gatepass", maxCount: 10 },
  ]),
  updateOrder   // ← or create a dedicated handler if different logic needed
);

// Other routes remain unchanged (no file uploads)
router.put("/:orderId/receivers/:receiverId/items/:itemRef/status", requireAuth, updateSpecificItemsStatus);
router.delete('/:orderId/receivers/:receiverId', requireAuth,removeReceiver);
router.delete('/:orderId/order-items/:itemId', removeOrderItem);
router.post("/assign-container", requireAuth, assignContainersToOrders);
router.post("/assign-containers-batch", requireAuth, assignContainersBatch);
router.post(
  "/assign-containers-to-orders",
  requireAuth,
  assignOneContainerToMultipleReceivers
);
router.post("/remove-assign-container", requireAuth, removeContainerAssignments);

// Read / tracking routes
router.get("/track/item/:ref", getOrderByItemRef);
router.get("/track/order/:ref", getOrderByOrderId);
router.get("/track/rgl/:rglBookingNo", getOrderByRglBookingNo);
router.get("/track/consignment_no/:id", getOrderByTrackingId);
router.get('/consignmentsOrders', requireAuth, getOrdersConsignments);

router.post('/notify/me', async (req, res) => {
  const { email } = req.query;
  const shipmentData = req.body;
  console.log('hit', email, shipmentData);

  // await sendShipmentEmail(email, shipmentData);
  res.json({ success: true });
});

// Protected reads
router.get("/", requireAuth, getOrders);
router.get("/:id", requireAuth, getOrderById);
router.get("/myOrderByRef", requireAuth, getMyOrdersByRef);

export default router;