import { Router } from "express";
import {
  createDropOffRequest,
  getItemForDropOffRequest,
} from "./drop-off.controller.js";

const router = Router();

router.post("/", createDropOffRequest);
router.get("/:itemRef", getItemForDropOffRequest);

export default router;
