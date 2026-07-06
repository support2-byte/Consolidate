/**
 * @swagger
 * components:
 *   schemas:
 *     Consignment:
 *       type: object
 *       properties:
 *         id: { type: string }
 *         origin: { type: string }
 *         destination: { type: string }
 *         customerId: { type: string }
 *         vendorId: { type: string }
 *         containerId: { type: string }
 *         status: { type: string }
 *         createdAt: { type: string, format: date-time }
 *         updatedAt: { type: string, format: date-time }
 *     CreateConsignmentRequest:
 *       type: object
 *       required: [origin, destination]
 *       properties:
 *         origin: { type: string }
 *         destination: { type: string }
 *         customerId: { type: string }
 *         vendorId: { type: string }
 *         containerId: { type: string }
 *     UpdateConsignmentRequest:
 *       type: object
 *       properties:
 *         origin: { type: string }
 *         destination: { type: string }
 *         customerId: { type: string }
 *         vendorId: { type: string }
 *         containerId: { type: string }
 *         status: { type: string }
 *     ConsignmentStatus:
 *       type: object
 *       properties:
 *         name: { type: string }
 *         order: { type: integer }
 *     ChangeStatusRequest:
 *       type: object
 *       required: [status]
 *       properties:
 *         status: { type: string }
 */
