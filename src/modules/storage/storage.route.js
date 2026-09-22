import { Router } from "express";
import {
  createStoragePurchase,
  getItemForStoragePurchase,
} from "./storage.controller.js";

const router = Router();

router.post("/", createStoragePurchase);
router.get("/:itemRef", getItemForStoragePurchase);

export default router;
