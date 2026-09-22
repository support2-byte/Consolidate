import express from "express";
import {
  processEmailQueue,
  verifyRecaptcha,
  createKycForm,
  getCustomerByZohoId,
  processKycEmailQueue,
  getRequests,
  approveRequest,
  rejectRequest,
} from "./internal.controller.js";
import { requireAuth } from "../auth/auth.middleware.js";
import { kycUpload } from "../../middleware/upload.js";

const router = express.Router();

/**
 * @swagger
 * /api/internal/process-email-queue:
 *   post:
 *     summary: Process pending emails in the notification queue
 *     tags: [Internal]
 *     responses:
 *       200:
 *         description: Queue processed
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/QueueProcessResult'
 */
router.post("/process-email-queue", processEmailQueue);

/**
 * @swagger
 * /api/internal/process-kyc-email-queue:
 *   post:
 *     summary: Process pending KYC-related emails in the queue
 *     tags: [Internal]
 *     responses:
 *       200:
 *         description: KYC email queue processed
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/QueueProcessResult'
 */
router.post("/process-kyc-email-queue", processKycEmailQueue);

/**
 * @swagger
 * /api/internal/verify-recaptcha:
 *   post:
 *     summary: Verify a Google reCAPTCHA token
 *     tags: [Internal]
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/RecaptchaVerifyRequest'
 *     responses:
 *       200:
 *         description: Verification result
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/RecaptchaVerifyResult'
 *       400:
 *         description: Invalid or missing token
 */
router.post("/verify-recaptcha", verifyRecaptcha);

/**
 * @swagger
 * /api/internal/customer/{zohoId}:
 *   get:
 *     summary: Get customer details by Zoho CRM ID
 *     tags: [Internal]
 *     parameters:
 *       - in: path
 *         name: zohoId
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Customer found
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Customer'
 *       404:
 *         description: Customer not found
 */
router.get("/customer/:zohoId", getCustomerByZohoId);

/**
 * @swagger
 * /api/internal/submit-kyc:
 *   post:
 *     summary: Submit a KYC form with supporting documents
 *     tags: [Internal]
 *     requestBody:
 *       required: true
 *       content:
 *         multipart/form-data:
 *           schema:
 *             $ref: '#/components/schemas/SubmitKycRequest'
 *     responses:
 *       201:
 *         description: KYC form submitted
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/KycSubmission'
 *       400:
 *         description: Invalid input
 */
router.post("/submit-kyc", kycUpload, createKycForm);

router.get("/get-requests", requireAuth, getRequests);
router.post("/requests/:type/:id/approve", requireAuth, approveRequest);
router.post("/requests/:type/:id/reject", requireAuth, rejectRequest);

export default router;
