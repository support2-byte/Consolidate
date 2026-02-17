import express from "express";
import multer from "multer";
import path from "path";
import upload from "../../middleware/upload.js"; // assuming this is your configured multer
import { requireAuth } from "../../modules/auth/auth.middleware.js"; // ← use this one

import {
  createOrder,
  updateOrder,
  getOrders,
  getOrderById,
  getOrderByTrackingId,
  getOrderByItemRef,
  assignContainersToOrders,
  updateReceiverStatus,
  getOrdersConsignments,
  getOrderByOrderId,
  getMyOrdersByRef,
  removeContainerAssignments,
  getOrderByRglBookingNo,
  assignOneContainerToMultipleReceivers,
  assignContainersBatch,
  sendShipmentEmail,
} from "./order.controller.js";

const router = express.Router();

// Multer configuration (moved here for clarity – you can keep it in middleware/upload.js too)
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, "uploads/");
  },
  filename: (req, file, cb) => {
    const ext = path.extname(file.originalname);
    cb(null, `${Date.now()}${ext}`);
  },
});

// If you want custom multer per route, you can define it here
// But assuming ../../middleware/upload.js already exports a configured multer instance

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

// PUT /api/orders/:id - Update order (with files)
router.put(
  "/:id",
  requireAuth,
  upload.fields([
    { name: "attachments", maxCount: 10 },
    { name: "gatepass", maxCount: 10 },
  ]),
  updateOrder
);

// PUT /api/orders/:id/shipping - Update shipping details
router.put(
  "/:id/shipping",
  requireAuth,
  upload.fields([
    { name: "attachments", maxCount: 10 },
    { name: "gatepass", maxCount: 10 },
  ]),
  updateOrder // ← assuming same handler, or create dedicated if needed
);

// PUT /api/orders/:orderId/receivers/:id/status - Update receiver status
router.put("/:orderId/receivers/:id/status", requireAuth, updateReceiverStatus);

// ────────────────────────────────────────────────
// Container assignment routes (protected)
// ────────────────────────────────────────────────

router.post("/assign-container", requireAuth, assignContainersToOrders);
router.post("/assign-containers-batch", requireAuth, assignContainersBatch);
router.post(
  "/assign-containers-to-orders",
  requireAuth,
  assignOneContainerToMultipleReceivers
);
router.post("/remove-assign-container", requireAuth, removeContainerAssignments);

// ────────────────────────────────────────────────
// Read routes – some public, some protected
// ────────────────────────────────────────────────

// Public or semi-public tracking routes (no auth required?)
router.get("/track/item/:ref", getOrderByItemRef);
router.get("/track/order/:ref", getOrderByOrderId);
router.get("/track/rgl/:rglBookingNo", getOrderByRglBookingNo);
router.get("/track/consignment_no/:id", getOrderByTrackingId);
router.get('/consignmentsOrders', requireAuth, getOrdersConsignments);
// Probably in your Express route handler
router.post('/notify/me', async (req, res) => {
  const { email } = req.query;

  // You probably also expect body with shipment data
  const shipmentData = req.body; // ← most likely
  console.log('hit',email,shipmentData)

  await sendShipmentEmail(email, shipmentData);
  res.json({ success: true });
});
// Protected reads
router.get("/", requireAuth, getOrders);                    // all orders – probably admin only
router.get("/:id", requireAuth, getOrderById);  
// User-specific orders (protected)
router.get("/myOrderByRef", requireAuth, getMyOrdersByRef);

export default router;