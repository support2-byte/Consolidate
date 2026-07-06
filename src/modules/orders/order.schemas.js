/**
 * @swagger
 * components:
 *   schemas:
 *     Order:
 *       type: object
 *       properties:
 *         id: { type: string }
 *         orderId: { type: string }
 *         trackingId: { type: string }
 *         rglBookingNo: { type: string }
 *         consignmentId: { type: string }
 *         status: { type: string }
 *         createdAt: { type: string, format: date-time }
 *         updatedAt: { type: string, format: date-time }
 *     CreateOrderRequest:
 *       type: object
 *       properties:
 *         orderId: { type: string }
 *         consignmentId: { type: string }
 *         attachments:
 *           type: array
 *           items: { type: string, format: binary }
 *         gatepass:
 *           type: array
 *           items: { type: string, format: binary }
 *     UpdateOrderRequest:
 *       type: object
 *       properties:
 *         status: { type: string }
 *         attachments:
 *           type: array
 *           items: { type: string, format: binary }
 *         gatepass:
 *           type: array
 *           items: { type: string, format: binary }
 *     AssignContainerRequest:
 *       type: object
 *       required: [orderId, containerId]
 *       properties:
 *         orderId: { type: string }
 *         containerId: { type: string }
 *     AssignContainersBatchRequest:
 *       type: object
 *       properties:
 *         assignments:
 *           type: array
 *           items:
 *             $ref: '#/components/schemas/AssignContainerRequest'
 *     RemoveContainerAssignmentRequest:
 *       type: object
 *       required: [orderId, containerId]
 *       properties:
 *         orderId: { type: string }
 *         containerId: { type: string }
 *     UpdateItemStatusRequest:
 *       type: object
 *       required: [status]
 *       properties:
 *         status: { type: string }
 */
