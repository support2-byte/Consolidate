// routes/consignmentRoutes.js - Express Routes for Consignment Module
// Usage: In your main app.js or index.js: app.use('/api/consignments', consignmentRoutes);

import express from 'express';
import {
  getConsignments,
  getConsignmentById,
  createConsignment,
  updateConsignment,
  advanceStatus,
  deleteConsignment,
  getStatuses
} from '../consignment/consignment.controller.js'; // Adjust path as needed

const router = express.Router();

// GET /api/consignments - Fetch all consignments (with pagination and filters)
router.get('/', getConsignments);

// GET /api/consignments/:id - Fetch single consignment by ID
router.get('/:id', getConsignmentById);

// POST /api/consignments - Create new consignment
router.post('/', createConsignment);

// PUT /api/consignments/:id - Full update consignment
router.put('/:id', updateConsignment);

// PATCH /api/consignments/:id - Partial update (general)
router.patch('/:id', updateConsignment); // Reuse update for partial; adjust if separate needed

// PATCH /api/consignments/:id/next - Advance status (workflow)
router.patch('/:id/next', advanceStatus);

// DELETE /api/consignments/:id - Delete consignment
router.delete('/:id', deleteConsignment);

// GET /api/consignments/statuses - Fetch available statuses (with colors)
router.get('/statuses', getStatuses);

export default router;