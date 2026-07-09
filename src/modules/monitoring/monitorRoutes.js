import express from "express";
import {
  getSlowQueries,
  getTableActivity,
  getQueryLogs,
  getMissingIndexes,
  resetStats,
} from "./monitor.controller.js";

const router = express.Router();

/**
 * @swagger
 * /api/monitoring/slow-queries:
 *   get:
 *     summary: List slow database queries
 *     tags: [Monitoring]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: List of slow queries
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/SlowQuery'
 */
router.get("/slow-queries", getSlowQueries);

/**
 * @swagger
 * /api/monitoring/table-activity:
 *   get:
 *     summary: Get table read/write activity stats
 *     tags: [Monitoring]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: Table activity stats
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/TableActivity'
 */
router.get("/table-activity", getTableActivity);

/**
 * @swagger
 * /api/monitoring/query-logs:
 *   get:
 *     summary: Get recent query logs
 *     tags: [Monitoring]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: List of query log entries
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/QueryLogEntry'
 */
router.get("/query-logs", getQueryLogs);

/**
 * @swagger
 * /api/monitoring/missing-indexes:
 *   get:
 *     summary: List tables/columns that may need indexes
 *     tags: [Monitoring]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: List of missing index suggestions
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/MissingIndex'
 */
router.get("/missing-indexes", getMissingIndexes);

/**
 * @swagger
 * /api/monitoring/reset-stats:
 *   post:
 *     summary: Reset collected database statistics
 *     tags: [Monitoring]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: Stats reset successfully
 */
router.post("/reset-stats", resetStats);

export default router;
