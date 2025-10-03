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
  getContainers
} from "./container.controller.js";

const router = express.Router();

// Specific dynamic option routes first to avoid :cid capture
router.get('/statuses', getStatuses);
router.get('/locations', getLocations);
router.get('/sizes', getSizes);
router.get('/types', getTypes);
router.get('/ownership-types', getOwnershipTypes);

// General routes
router.get('/', getAllContainers);
router.post('/', createContainer);

// Parameterized routes
router.get('/:cid', getContainerById);
router.get('/:cid/usage-history', getUsageHistory);
router.put('/:cid', updateContainer);
router.delete('/:cid', deleteContainer);

export default router;