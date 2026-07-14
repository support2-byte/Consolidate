import express from "express";
import upload from "../../middleware/upload.js";
import { requireAuth } from "../../modules/auth/auth.middleware.js";

import {
  createOrder,
  updateOrder,
  getOrders,
  getOrderById,
  getOrderByTrackingId,
  getOrderByItemRef,
  assignContainersToOrders,
  updateSpecificItemsStatus,
  getOrdersConsignments,
  getOrderByOrderId,
  removeContainerAssignments,
  getOrderByRglBookingNo,
  assignContainersBatch,
  removeReceiver,
  removeOrderItem,
  getAssignedOrderById,
} from "./order.controller.js";
import {
  sendShipmentEmail,
  subscribeToShipment,
} from "../../services/sendOrderEmail.js";
import logger from "../../services/logger.js";

const router = express.Router();

/**
 * @swagger
 * /api/orders/consignmentsOrders:
 *   get:
 *     summary: Get orders grouped by consignments
 *     tags: [Orders]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: Orders grouped by consignment
 */
router.get("/consignmentsOrders", requireAuth, getOrdersConsignments);

/**
 * @swagger
 * /api/orders/track/item/{ref}:
 *   get:
 *     summary: Track an order by item reference (public)
 *     tags: [Orders]
 *     parameters:
 *       - in: path
 *         name: ref
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Order found
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Order'
 *       404:
 *         description: Order not found
 */
router.get("/track/item/:ref", getOrderByItemRef);

/**
 * @swagger
 * /api/orders/track/order/{ref}:
 *   get:
 *     summary: Track an order by order ID (public)
 *     tags: [Orders]
 *     parameters:
 *       - in: path
 *         name: ref
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Order found
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Order'
 *       404:
 *         description: Order not found
 */
router.get("/track/order/:ref", getOrderByOrderId);

/**
 * @swagger
 * /api/orders/track/rgl/{rglBookingNo}:
 *   get:
 *     summary: Track an order by RGL booking number (public)
 *     tags: [Orders]
 *     parameters:
 *       - in: path
 *         name: rglBookingNo
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Order found
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Order'
 *       404:
 *         description: Order not found
 */
router.get("/track/rgl/:rglBookingNo", getOrderByRglBookingNo);

/**
 * @swagger
 * /api/orders/track/consignment_no/{id}:
 *   get:
 *     summary: Track an order by consignment tracking ID (public)
 *     tags: [Orders]
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Order found
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Order'
 *       404:
 *         description: Order not found
 */
router.get("/track/consignment_no/:id", getOrderByTrackingId);

/**
 * @swagger
 * /api/orders:
 *   get:
 *     summary: List all orders
 *     tags: [Orders]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: List of orders
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/Order'
 */
router.get("/", requireAuth, getOrders);

/**
 * @swagger
 * /api/orders/{id}:
 *   get:
 *     summary: Get an order by ID
 *     tags: [Orders]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Order found
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Order'
 *       404:
 *         description: Order not found
 */
router.get("/:id", requireAuth, getOrderById);

/**
 * @swagger
 * /api/orders/{id}/assigned:
 *   get:
 *     summary: Get an order's container assignment details by ID
 *     tags: [Orders]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Assigned order details
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Order'
 *       404:
 *         description: Order not found
 */
router.get("/:id/assigned", requireAuth, getAssignedOrderById);

/**
 * @swagger
 * /api/orders:
 *   post:
 *     summary: Create a new order (with optional attachments/gatepass files)
 *     tags: [Orders]
 *     security:
 *       - cookieAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         multipart/form-data:
 *           schema:
 *             $ref: '#/components/schemas/CreateOrderRequest'
 *     responses:
 *       201:
 *         description: Order created
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Order'
 *       400:
 *         description: Invalid input
 */
router.post(
  "/",
  requireAuth,
  upload.fields([
    { name: "attachments", maxCount: 10 },
    { name: "gatepass", maxCount: 10 },
  ]),
  createOrder,
);

/**
 * @swagger
 * /api/orders/assign-container:
 *   post:
 *     summary: Assign a single container to an order
 *     tags: [Orders]
 *     security:
 *       - cookieAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/AssignContainerRequest'
 *     responses:
 *       200:
 *         description: Container assigned
 *       404:
 *         description: Order or container not found
 */
router.post("/assign-container", requireAuth, assignContainersToOrders);

/**
 * @swagger
 * /api/orders/assign-containers-batch:
 *   post:
 *     summary: Assign containers to multiple orders in batch
 *     tags: [Orders]
 *     security:
 *       - cookieAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/AssignContainersBatchRequest'
 *     responses:
 *       200:
 *         description: Containers assigned
 */
router.post("/assign-containers-batch", requireAuth, assignContainersBatch);

/**
 * @swagger
 * /api/orders/remove-assign-container:
 *   post:
 *     summary: Remove a container assignment from an order
 *     tags: [Orders]
 *     security:
 *       - cookieAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/RemoveContainerAssignmentRequest'
 *     responses:
 *       200:
 *         description: Container assignment removed
 *       404:
 *         description: Assignment not found
 */
router.post(
  "/remove-assign-container",
  requireAuth,
  removeContainerAssignments,
);

/**
 * @swagger
 * /api/orders/notify/me:
 *   post:
 *     summary: Subscribe an email to order status updates and send the first notification
 *     tags: [Orders]
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required: [email, orderId]
 *             properties:
 *               email: { type: string, format: email }
 *               orderId: { type: integer }
 *               referenceId: { type: string }
 *               statusLabel: { type: string }
 *               statusMsg: { type: string }
 *     responses:
 *       200:
 *         description: Notification sent
 */
router.post("/notify/me", async (req, res) => {
  try {
    const result = await subscribeToShipment(req.body);
    res.json(result);
  } catch (err) {
    logger.error("notify/me failed", { error: err.message });
    res.status(500).json({ success: false, error: "Internal error" });
  }
});

/**
 * @swagger
 * /api/orders/{id}:
 *   put:
 *     summary: Update an order (with optional attachments/gatepass files)
 *     tags: [Orders]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     requestBody:
 *       required: true
 *       content:
 *         multipart/form-data:
 *           schema:
 *             $ref: '#/components/schemas/UpdateOrderRequest'
 *     responses:
 *       200:
 *         description: Order updated
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Order'
 *       404:
 *         description: Order not found
 */
router.put(
  "/:id",
  requireAuth,
  upload.fields([
    { name: "attachments", maxCount: 10 },
    { name: "gatepass", maxCount: 10 },
  ]),
  updateOrder,
);

/**
 * @swagger
 * /api/orders/{id}/shipping:
 *   put:
 *     summary: Update an order's shipping details (with optional attachments/gatepass files)
 *     tags: [Orders]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     requestBody:
 *       required: true
 *       content:
 *         multipart/form-data:
 *           schema:
 *             $ref: '#/components/schemas/UpdateOrderRequest'
 *     responses:
 *       200:
 *         description: Order shipping details updated
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Order'
 *       404:
 *         description: Order not found
 */
router.put(
  "/:id/shipping",
  requireAuth,
  upload.fields([
    { name: "attachments", maxCount: 10 },
    { name: "gatepass", maxCount: 10 },
  ]),
  updateOrder,
);

/**
 * @swagger
 * /api/orders/{orderId}/receivers/{receiverId}/items/{itemRef}/status:
 *   put:
 *     summary: Update the status of a specific item for a receiver
 *     tags: [Orders]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: orderId
 *         required: true
 *         schema: { type: string }
 *       - in: path
 *         name: receiverId
 *         required: true
 *         schema: { type: string }
 *       - in: path
 *         name: itemRef
 *         required: true
 *         schema: { type: string }
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/UpdateItemStatusRequest'
 *     responses:
 *       200:
 *         description: Item status updated
 *       404:
 *         description: Order, receiver, or item not found
 */
router.put(
  "/:orderId/receivers/:receiverId/items/:itemRef/status",
  requireAuth,
  updateSpecificItemsStatus,
);

/**
 * @swagger
 * /api/orders/{orderId}/order-items/{itemId}:
 *   delete:
 *     summary: Remove an item from an order
 *     tags: [Orders]
 *     parameters:
 *       - in: path
 *         name: orderId
 *         required: true
 *         schema: { type: string }
 *       - in: path
 *         name: itemId
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Item removed
 *       404:
 *         description: Order or item not found
 */
router.delete("/:orderId/order-items/:itemId", removeOrderItem);

/**
 * @swagger
 * /api/orders/{orderId}/receivers/{receiverId}:
 *   delete:
 *     summary: Remove a receiver from an order
 *     tags: [Orders]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: orderId
 *         required: true
 *         schema: { type: string }
 *       - in: path
 *         name: receiverId
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Receiver removed
 *       404:
 *         description: Order or receiver not found
 */
router.delete("/:orderId/receivers/:receiverId", requireAuth, removeReceiver);

export default router;
