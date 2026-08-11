/**
 * @swagger
 * components:
 *   schemas:
 *     Notification:
 *       type: object
 *       properties:
 *         id: { type: string }
 *         orderId: { type: string }
 *         email: { type: string, format: email }
 *         statusLabel: { type: string }
 *         statusMsg: { type: string }
 *         status: { type: string, enum: [pending, sent, failed] }
 *         createdAt: { type: string, format: date-time }
 *     EmailSubscription:
 *       type: object
 *       properties:
 *         id: { type: string }
 *         orderId: { type: string }
 *         email: { type: string, format: email }
 *         subscribedAt: { type: string, format: date-time }
 */
