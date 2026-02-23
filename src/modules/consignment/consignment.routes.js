// routes/consignmentRoutes.js - Express Routes for Consignment Module
// Usage: In your main app.js or index.js: app.use('/api/consignments', consignmentRoutes);

import express from 'express';
import {
  getConsignments,
  getConsignmentById,
  createConsignment,
  updateConsignment,
  deleteConsignment,
  getStatuses,
  calculateETAEndpoint
} from '../consignment/consignment.controller.js'; // Adjust path as needed
import { advanceStatus, changeConsignmentStatus } from '../orders/order.controller.js'; // Import advanceStatus
const router = express.Router();

// GET /api/consignments - Fetch all consignments (with pagination and filters)
router.get('/', getConsignments);

// GET /api/consignments/statuses - Fetch available statuses (with colors) - SPECIFIC ROUTE FIRST
router.get('/statuses', getStatuses);  // Moved UP: Before /:id to avoid interception

// GET /api/consignments/:id - Fetch single consignment by ID - PARAMETRIC ROUTE LAST
router.get('/:id', getConsignmentById);
router.get('/calculate-eta?status=', calculateETAEndpoint);

// POST /api/consignments - Create new consignment
router.post('/', createConsignment);
calculateETAEndpoint
// PUT /api/consignments/:id - Full update consignment
router.put('/:id', updateConsignment);

// PATCH /api/consignments/:id - Partial update (general)
router.patch('/:id', updateConsignment); // Reuse update for partial; adjust if separate needed

// PATCH /api/consignments/:id/next - Advance status (workflow) - Note: PUT used; consider PATCH for partial
router.put('/:id/next', advanceStatus);
router.put('/:id/status', changeConsignmentStatus);


// DELETE /api/consignments/:id - Delete consignment
router.delete('/:id', deleteConsignment);

export default router;