/**
 * @swagger
 * components:
 *   schemas:
 *     SlowQuery:
 *       type: object
 *       properties:
 *         query: { type: string }
 *         calls: { type: integer }
 *         totalTimeMs: { type: number }
 *         meanTimeMs: { type: number }
 *     TableActivity:
 *       type: object
 *       properties:
 *         tableName: { type: string }
 *         seqScans: { type: integer }
 *         indexScans: { type: integer }
 *         rowsInserted: { type: integer }
 *         rowsUpdated: { type: integer }
 *         rowsDeleted: { type: integer }
 *     QueryLogEntry:
 *       type: object
 *       properties:
 *         timestamp: { type: string, format: date-time }
 *         query: { type: string }
 *         durationMs: { type: number }
 *     MissingIndex:
 *       type: object
 *       properties:
 *         tableName: { type: string }
 *         columnName: { type: string }
 *         seqScans: { type: integer }
 */
