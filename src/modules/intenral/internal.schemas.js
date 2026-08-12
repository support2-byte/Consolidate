/**
 * @swagger
 * components:
 *   schemas:
 *     QueueProcessResult:
 *       type: object
 *       properties:
 *         processed: { type: integer }
 *         failed: { type: integer }
 *         success: { type: boolean }
 *     RecaptchaVerifyRequest:
 *       type: object
 *       required: [token]
 *       properties:
 *         token: { type: string }
 *     RecaptchaVerifyResult:
 *       type: object
 *       properties:
 *         success: { type: boolean }
 *         score: { type: number }
 *     Customer:
 *       type: object
 *       properties:
 *         id: { type: string }
 *         zohoId: { type: string }
 *         name: { type: string }
 *         email: { type: string, format: email }
 *         phone: { type: string }
 *     SubmitKycRequest:
 *       type: object
 *       required: [customerId]
 *       properties:
 *         customerId: { type: string }
 *         documents:
 *           type: array
 *           items: { type: string, format: binary }
 *     KycSubmission:
 *       type: object
 *       properties:
 *         id: { type: string }
 *         customerId: { type: string }
 *         status: { type: string }
 *         createdAt: { type: string, format: date-time }
 */
