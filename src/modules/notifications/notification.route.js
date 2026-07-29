import express from "express";
import {
  getAllNotifications,
  getEmailSubscriptions,
  resendNotification,
  deleteEmailQueue,
} from "./notification.controller.js";
import { requireAuth } from "../auth/auth.middleware.js";

const router = express.Router();

router.get("/", requireAuth, getAllNotifications);
router.get("/subscriptions", requireAuth, getEmailSubscriptions);
router.post("/:id/resend", requireAuth, resendNotification);
router.delete("/:id/delete", requireAuth, deleteEmailQueue);

export default router;
