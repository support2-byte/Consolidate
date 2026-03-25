// src/modules/monitoring/monitoring.routes.js
import express from 'express';
import {
    getSlowQueries,
    getTableActivity,
    getQueryLogs,
    getMissingIndexes,
    resetStats,
} from './monitor.controller.js';

const router = express.Router();

router.get('/slow-queries',   getSlowQueries);
router.get('/table-activity', getTableActivity);
router.get('/query-logs',     getQueryLogs);
router.get('/missing-indexes',getMissingIndexes);
router.post('/reset-stats',   resetStats);

export default router;