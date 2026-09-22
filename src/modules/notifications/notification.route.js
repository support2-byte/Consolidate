import express from "express";
import {
  getAllNotifications,
  getEmailSubscriptions,
  resendNotification,
  deleteEmailQueue,
  getConfirmationEmails,
  resendConfirmationEmail,
  deleteConfirmationEmail,
  getAllInvoiceEmails,
  resendInvoiceNotification,
  deleteInvoiceEmailQueue,
} from "./notification.controller.js";
import { requireAuth } from "../auth/auth.middleware.js";

const router = express.Router();

/**
 * @swagger
 * /api/notifications:
 *   get:
 *     summary: List all notifications
 *     tags: [Notifications]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: List of notifications
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/Notification'
 */
router.get("/", requireAuth, getAllNotifications);

/**
 * @swagger
 * /api/notifications/subscriptions:
 *   get:
 *     summary: List email subscriptions for shipment updates
 *     tags: [Notifications]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: List of email subscriptions
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/EmailSubscription'
 */
router.get("/subscriptions", requireAuth, getEmailSubscriptions);

/**
 * @swagger
 * /api/notifications/{id}/resend:
 *   post:
 *     summary: Resend a queued or failed notification
 *     tags: [Notifications]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Notification resent
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Notification'
 *       404:
 *         description: Notification not found
 */
router.post("/:id/resend", requireAuth, resendNotification);

/**
 * @swagger
 * /api/notifications/{id}/delete:
 *   delete:
 *     summary: Delete a queued email notification
 *     tags: [Notifications]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Notification deleted
 *       404:
 *         description: Notification not found
 */
router.delete("/:id/delete", requireAuth, deleteEmailQueue);

router.get("/confirmation-emails", requireAuth, getConfirmationEmails);
router.post(
  "/confirmation-emails/:id/resend",
  requireAuth,
  resendConfirmationEmail,
);
router.delete(
  "/confirmation-emails/:id/delete",
  requireAuth,
  deleteConfirmationEmail,
);

router.get("/invoice-emails", getAllInvoiceEmails);
router.post("/invoice-emails/:id/resend", resendInvoiceNotification);
router.delete("/invoice-emails/:id/delete", deleteInvoiceEmailQueue);

export default router;
