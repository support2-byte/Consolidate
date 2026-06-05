import express from "express";
import {
  createContainer,
  getAllContainers,
  getContainerById,
  // updateContainer,
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
  // getContainers
} from "./container.controller.js";
import { updateContainer } from "../orders/order.controller.js";
const router = express.Router();

// Specific dynamic option routes first to avoid :cid capture
router.get("/statuses", getStatuses);
router.get("/locations", getLocations);
router.get("/sizes", getSizes);
router.get("/types", getTypes);
router.get("/ownership-types", getOwnershipTypes);

router.get("/container-consignments", getContainerAssignments);
router.put("/container-consignments/:id/release", releaseContainer);

// General routes
router.get("/", getAllContainers);
router.get("/consignment-containers", getAllContainersForConsignment);
router.post("/", createContainer);

// Parameterized routes
router.get("/:cid", getContainerById);
router.get("/:cid/usage-history", getUsageHistory);
router.put("/:cid", updateContainer);
router.delete("/:cid", deleteContainer);

export default router;
