/**
 * @swagger
 * components:
 *   schemas:
 *     QueueKycEmailRequest:
 *       type: object
 *       required: [customerIds]
 *       properties:
 *         customerIds:
 *           type: array
 *           items: { type: string }
 *     KycEmailStatusSummary:
 *       type: object
 *       properties:
 *         pending: { type: integer }
 *         sent: { type: integer }
 *         failed: { type: integer }
 *     KycEmail:
 *       type: object
 *       properties:
 *         id: { type: string }
 *         customerId: { type: string }
 *         email: { type: string, format: email }
 *         status: { type: string, enum: [pending, sent, failed] }
 *         createdAt: { type: string, format: date-time }
 *     KycLog:
 *       type: object
 *       properties:
 *         id: { type: string }
 *         customerId: { type: string }
 *         action: { type: string }
 *         actor: { type: string }
 *         createdAt: { type: string, format: date-time }
 *     KycSubmissionStats:
 *       type: object
 *       properties:
 *         total: { type: integer }
 *         approved: { type: integer }
 *         pending: { type: integer }
 *         rejected: { type: integer }
 *     CustomerKycProfile:
 *       type: object
 *       properties:
 *         customerId: { type: string }
 *         status: { type: string }
 *         documents:
 *           type: array
 *           items: { type: string }
 *         submittedAt: { type: string, format: date-time }
 *         reviewedAt: { type: string, format: date-time }
 *     UpdateKycSubmissionRequest:
 *       type: object
 *       required: [status]
 *       properties:
 *         status: { type: string, enum: [approved, pending, rejected] }
 *         reviewerNote: { type: string }
 */
