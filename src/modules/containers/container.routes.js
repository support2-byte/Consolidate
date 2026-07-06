import express from "express";
import {
  createContainer,
  getAllContainers,
  getContainerById,
  updateContainer,
  deleteContainer,
  getStatuses,
  getLocations,
  getSizes,
  getTypes,
  getOwnershipTypes,
  getUsageHistory,
  getContainerAssignments,
  releaseContainer,
  getAllContainersForConsignment,
  getUnassignedOrders,
  updateContainerStatus,
  // getContainers
} from "./container.controller.js";
const router = express.Router();

/**
 * @swagger
 * /api/containers/statuses:
 *   get:
 *     summary: List all possible container statuses
 *     tags: [Containers]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: List of statuses
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items: { type: string }
 */
router.get("/statuses", getStatuses);

/**
 * @swagger
 * /api/containers/locations:
 *   get:
 *     summary: List all container locations
 *     tags: [Containers]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: List of locations
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items: { type: string }
 */
router.get("/locations", getLocations);

/**
 * @swagger
 * /api/containers/sizes:
 *   get:
 *     summary: List all container sizes
 *     tags: [Containers]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: List of sizes
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items: { type: string }
 */
router.get("/sizes", getSizes);

/**
 * @swagger
 * /api/containers/types:
 *   get:
 *     summary: List all container types
 *     tags: [Containers]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: List of types
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items: { type: string }
 */
router.get("/types", getTypes);

/**
 * @swagger
 * /api/containers/ownership-types:
 *   get:
 *     summary: List all container ownership types
 *     tags: [Containers]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: List of ownership types
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items: { type: string }
 */
router.get("/ownership-types", getOwnershipTypes);

/**
 * @swagger
 * /api/containers/container-consignments:
 *   get:
 *     summary: List container-to-consignment assignments
 *     tags: [Containers]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: List of container assignments
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/ContainerAssignment'
 */
router.get("/container-consignments", getContainerAssignments);

/**
 * @swagger
 * /api/containers/container-consignments/{id}/release:
 *   put:
 *     summary: Release a container from its current consignment assignment
 *     tags: [Containers]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Container released
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/ContainerAssignment'
 *       404:
 *         description: Assignment not found
 */
router.put("/container-consignments/:id/release", releaseContainer);

/**
 * @swagger
 * /api/containers:
 *   get:
 *     summary: List all containers
 *     tags: [Containers]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: List of containers
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/Container'
 */
router.get("/", getAllContainers);

/**
 * @swagger
 * /api/containers/consignment-containers:
 *   get:
 *     summary: List all containers grouped for consignment selection
 *     tags: [Containers]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: List of containers available for consignments
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/Container'
 */
router.get("/consignment-containers", getAllContainersForConsignment);

/**
 * @swagger
 * /api/containers:
 *   post:
 *     summary: Create a new container
 *     tags: [Containers]
 *     security:
 *       - cookieAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/CreateContainerRequest'
 *     responses:
 *       201:
 *         description: Container created
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Container'
 *       400:
 *         description: Invalid input
 */
router.post("/", createContainer);

/**
 * @swagger
 * /api/containers/{cid}:
 *   get:
 *     summary: Get a container by ID
 *     tags: [Containers]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: cid
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Container found
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Container'
 *       404:
 *         description: Container not found
 */
router.get("/:cid", getContainerById);

/**
 * @swagger
 * /api/containers/{cid}/usage-history:
 *   get:
 *     summary: Get usage history for a container
 *     tags: [Containers]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: cid
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Usage history entries
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/UsageHistoryEntry'
 *       404:
 *         description: Container not found
 */
router.get("/:cid/usage-history", getUsageHistory);

/**
 * @swagger
 * /api/containers/{cid}:
 *   put:
 *     summary: Update a container
 *     tags: [Containers]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: cid
 *         required: true
 *         schema: { type: string }
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/UpdateContainerRequest'
 *     responses:
 *       200:
 *         description: Container updated
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Container'
 *       404:
 *         description: Container not found
 */
router.put("/:cid", updateContainer);

/**
 * @swagger
 * /api/containers/status/{cid}:
 *   put:
 *     summary: Update a container's status
 *     tags: [Containers]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: cid
 *         required: true
 *         schema: { type: string }
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/UpdateContainerStatusRequest'
 *     responses:
 *       200:
 *         description: Container status updated
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Container'
 *       404:
 *         description: Container not found
 */
router.put("/status/:cid/", updateContainerStatus);

/**
 * @swagger
 * /api/containers/{cid}:
 *   delete:
 *     summary: Delete a container
 *     tags: [Containers]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: cid
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Container deleted
 *       404:
 *         description: Container not found
 */
router.delete("/:cid", deleteContainer);

/**
 * @swagger
 * /api/containers/{cid}/unassigned-orders:
 *   get:
 *     summary: Get orders not yet assigned within this container
 *     tags: [Containers]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: cid
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: List of unassigned orders
 *       404:
 *         description: Container not found
 */
router.get("/:cid/unassigned-orders", getUnassignedOrders);

export default router;
