import express from "express";
import { addContainer,getContainers } from "./container.controller.js";

const router = express.Router();

// POST → Add container
router.post("/", addContainer);
router.get("/", getContainers);    // Get all containers

export default router;
