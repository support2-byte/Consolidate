import express from "express";
import { createOrder, updateOrder, getOrders, getOrderById,getOrderByTrackingId, getOrderByItemRef, assignContainersToOrders } from './order.controller.js'
// import express from "express";
import multer from "multer"
import upload from "../../middleware/upload.js";
import path from "path"; // Add this import for path.extname

const router = express.Router();

// Configure multer for file uploads (stores in 'uploads/' directory)
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, 'uploads/'); // Ensure this directory exists
  },
  filename: (req, file, cb) => {
    cb(null, Date.now() + path.extname(file.originalname));
  }
});
// const upload = multer({ storage });

// POST /api/orders - Create a new order (expects multipart/form-data with 'attachments' field)
router.post('/', upload.fields([
  { name: 'attachments', maxCount: 10 },
  { name: 'gatepass', maxCount: 10 }
]),createOrder);

// PUT /api/orders/:id - Update an existing order
router.put('/:id', upload.fields([
  { name: 'attachments', maxCount: 10 },
  { name: 'gatepass', maxCount: 10 }
]), updateOrder)

// PUT /api/orders/:id/shipping - Update shipping details for an existing order (receivers and order_items)
router.put('/:id/shipping', upload.fields([
  { name: 'attachments', maxCount: 10 },
  { name: 'gatepass', maxCount: 10 }
]), updateOrder)

// GET /api/orders - Fetch all orders
router.get('/', getOrders);
router.get('/track/:trackingId', getOrderByTrackingId);
router.get('/track/item/:itemRef', getOrderByItemRef);
// GET /api/orders/:id - Fetch a specific order
router.get('/:id', getOrderById);

// POST /api/orders/assign-container - Assign container to multiple orders
router.post('/assign-container', assignContainersToOrders);

export default router;