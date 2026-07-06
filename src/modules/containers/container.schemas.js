/**
 * @swagger
 * components:
 *   schemas:
 *     Container:
 *       type: object
 *       properties:
 *         id: { type: string }
 *         containerNumber: { type: string }
 *         type: { type: string }
 *         size: { type: string }
 *         ownershipType: { type: string }
 *         location: { type: string }
 *         status: { type: string }
 *         createdAt: { type: string, format: date-time }
 *         updatedAt: { type: string, format: date-time }
 *     CreateContainerRequest:
 *       type: object
 *       required: [containerNumber, type, size]
 *       properties:
 *         containerNumber: { type: string }
 *         type: { type: string }
 *         size: { type: string }
 *         ownershipType: { type: string }
 *         location: { type: string }
 *     UpdateContainerRequest:
 *       type: object
 *       properties:
 *         containerNumber: { type: string }
 *         type: { type: string }
 *         size: { type: string }
 *         ownershipType: { type: string }
 *         location: { type: string }
 *         status: { type: string }
 *     UpdateContainerStatusRequest:
 *       type: object
 *       required: [status]
 *       properties:
 *         status: { type: string }
 *     ContainerAssignment:
 *       type: object
 *       properties:
 *         id: { type: string }
 *         containerId: { type: string }
 *         consignmentId: { type: string }
 *         assignedAt: { type: string, format: date-time }
 *         releasedAt: { type: string, format: date-time, nullable: true }
 *     UsageHistoryEntry:
 *       type: object
 *       properties:
 *         id: { type: string }
 *         containerId: { type: string }
 *         consignmentId: { type: string }
 *         startedAt: { type: string, format: date-time }
 *         endedAt: { type: string, format: date-time, nullable: true }
 */
