import { Router } from "express";
import {
  createDeliveryRequest,
  getItemForDeliveryRequest,
} from "./delivery.controller.js";

const router = Router();

router.post("/", createDeliveryRequest);
router.get("/:itemRef", getItemForDeliveryRequest);

export default router;
