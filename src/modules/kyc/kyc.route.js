import express from "express";
import {
  queueKycEmails,
  getKycEmailStatus,
  getAllKycEmails,
  resendKycEmail,
  deleteKycEmail,
  getAllKycLogs,
  getCustomerKycProfile,
  getAllKycSubmissions,
  getKycSubmissionStats,
  updateKycSubmission,
  getKycCustomerStatusMap,
  updateCustomerFromKyc,
} from "./kyc.controller.js";
import { requireAuth } from "../auth/auth.middleware.js";

const router = express.Router();

/**
 * @swagger
 * /api/kyc/queue-email:
 *   post:
 *     summary: Queue KYC-related emails for sending
 *     tags: [KYC]
 *     security:
 *       - cookieAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/QueueKycEmailRequest'
 *     responses:
 *       200:
 *         description: Emails queued
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/QueueProcessResult'
 */
router.post("/queue-email", requireAuth, queueKycEmails);

/**
 * @swagger
 * /api/kyc/email-status:
 *   get:
 *     summary: Get aggregate status of KYC email queue
 *     tags: [KYC]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: Email queue status summary
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/KycEmailStatusSummary'
 */
router.get("/email-status", requireAuth, getKycEmailStatus);

/**
 * @swagger
 * /api/kyc/emails:
 *   get:
 *     summary: List all KYC emails
 *     tags: [KYC]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: List of KYC emails
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/KycEmail'
 */
router.get("/emails", requireAuth, getAllKycEmails);

/**
 * @swagger
 * /api/kyc/emails/{id}/resend:
 *   post:
 *     summary: Resend a specific KYC email
 *     tags: [KYC]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: KYC email resent
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/KycEmail'
 *       404:
 *         description: KYC email not found
 */
router.post("/emails/:id/resend", requireAuth, resendKycEmail);

/**
 * @swagger
 * /api/kyc/emails/{id}/delete:
 *   delete:
 *     summary: Delete a queued KYC email
 *     tags: [KYC]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: KYC email deleted
 *       404:
 *         description: KYC email not found
 */
router.delete("/emails/:id/delete", requireAuth, deleteKycEmail);

/**
 * @swagger
 * /api/kyc/logs:
 *   get:
 *     summary: List all KYC activity logs
 *     tags: [KYC]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: List of KYC logs
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/KycLog'
 */
router.get("/logs", requireAuth, getAllKycLogs);

/**
 * @swagger
 * /api/kyc/submissions:
 *   get:
 *     summary: List all KYC submissions
 *     tags: [KYC]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: List of KYC submissions
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/KycSubmission'
 */
router.get("/submissions", requireAuth, getAllKycSubmissions);

/**
 * @swagger
 * /api/kyc/submissions-stats:
 *   get:
 *     summary: Get aggregate statistics on KYC submissions
 *     tags: [KYC]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: Submission statistics
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/KycSubmissionStats'
 */
router.get("/submissions-stats", requireAuth, getKycSubmissionStats);

/**
 * @swagger
 * /api/kyc/customer-status-map:
 *   get:
 *     summary: Get a map of customer IDs to their current KYC status (public)
 *     tags: [KYC]
 *     responses:
 *       200:
 *         description: Customer ID to KYC status map
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               additionalProperties:
 *                 type: string
 */
router.get("/customer-status-map", getKycCustomerStatusMap);

/**
 * @swagger
 * /api/kyc/customer-profile/{customerId}:
 *   get:
 *     summary: Get a customer's full KYC profile
 *     tags: [KYC]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: customerId
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Customer KYC profile
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/CustomerKycProfile'
 *       404:
 *         description: Customer not found
 */
router.get("/customer-profile/:customerId", requireAuth, getCustomerKycProfile);

/**
 * @swagger
 * /api/kyc/submissions/{formId}:
 *   patch:
 *     summary: Update a KYC submission (e.g. approve/reject)
 *     tags: [KYC]
 *     parameters:
 *       - in: path
 *         name: formId
 *         required: true
 *         schema: { type: string }
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/UpdateKycSubmissionRequest'
 *     responses:
 *       200:
 *         description: KYC submission updated
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/KycSubmission'
 *       404:
 *         description: Submission not found
 */
router.patch("/submissions/:formId", requireAuth, updateKycSubmission);

router.patch(
  "/submissions/:id/update-customer",
  requireAuth,
  updateCustomerFromKyc,
);

export default router;
