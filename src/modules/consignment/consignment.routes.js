import express from "express";
import {
  getConsignments,
  getConsignmentById,
  createConsignment,
  updateConsignment,
  deleteConsignment,
  getStatuses,
  advanceStatus,
  changeConsignmentStatus,
} from "../consignment/consignment.controller.js";

const router = express.Router();

/**
 * @swagger
 * /api/consignments:
 *   get:
 *     summary: List all consignments
 *     tags: [Consignments]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: List of consignments
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/Consignment'
 */
router.get("/", getConsignments);

/**
 * @swagger
 * /api/consignments/statuses:
 *   get:
 *     summary: List all possible consignment statuses
 *     tags: [Consignments]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: List of statuses
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/ConsignmentStatus'
 */
router.get("/statuses", getStatuses);

/**
 * @swagger
 * /api/consignments/{id}:
 *   get:
 *     summary: Get a consignment by ID
 *     tags: [Consignments]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Consignment found
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Consignment'
 *       404:
 *         description: Consignment not found
 */
router.get("/:id", getConsignmentById);

/**
 * @swagger
 * /api/consignments:
 *   post:
 *     summary: Create a new consignment
 *     tags: [Consignments]
 *     security:
 *       - cookieAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/CreateConsignmentRequest'
 *     responses:
 *       201:
 *         description: Consignment created
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Consignment'
 *       400:
 *         description: Invalid input
 */
router.post("/", createConsignment);

/**
 * @swagger
 * /api/consignments/{id}:
 *   put:
 *     summary: Update a consignment (full update)
 *     tags: [Consignments]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/UpdateConsignmentRequest'
 *     responses:
 *       200:
 *         description: Consignment updated
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Consignment'
 *       404:
 *         description: Consignment not found
 */
router.put("/:id", updateConsignment);

/**
 * @swagger
 * /api/consignments/{id}:
 *   patch:
 *     summary: Update a consignment (partial update)
 *     tags: [Consignments]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/UpdateConsignmentRequest'
 *     responses:
 *       200:
 *         description: Consignment updated
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Consignment'
 *       404:
 *         description: Consignment not found
 */
router.patch("/:id", updateConsignment);

/**
 * @swagger
 * /api/consignments/{id}/next:
 *   put:
 *     summary: Advance a consignment to its next status
 *     tags: [Consignments]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Consignment advanced to next status
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Consignment'
 *       404:
 *         description: Consignment not found
 */
router.put("/:id/next", advanceStatus);

/**
 * @swagger
 * /api/consignments/{id}/status:
 *   put:
 *     summary: Set a consignment's status explicitly
 *     tags: [Consignments]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/ChangeStatusRequest'
 *     responses:
 *       200:
 *         description: Consignment status updated
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Consignment'
 *       404:
 *         description: Consignment not found
 */
router.put("/:id/status", changeConsignmentStatus);

/**
 * @swagger
 * /api/consignments/{id}:
 *   delete:
 *     summary: Delete a consignment
 *     tags: [Consignments]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Consignment deleted
 *       404:
 *         description: Consignment not found
 */
router.delete("/:id", deleteConsignment);

export default router;
