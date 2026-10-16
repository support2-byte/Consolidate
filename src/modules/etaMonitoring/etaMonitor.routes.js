import { Router } from "express";
import {
  runNow,
  getNotifications,
  addGraceDays,
  markDelayed,
  getMonitorItems,
  getMonitorLog,
  getStatusOptions,
} from "./etaMonitor.controller.js";
import { requireAuth } from "../auth/auth.middleware.js";

const router = Router();

router.post("/run", requireAuth, runNow);
router.get("/notifications", requireAuth, getNotifications);
router.post("/notifications/:id/grace", requireAuth, addGraceDays);
router.post("/notifications/:id/delay", requireAuth, markDelayed);
router.get("/items", requireAuth, getMonitorItems);
router.get("/log", requireAuth, getMonitorLog);
router.get("/statuses", getStatusOptions);

export default router;
