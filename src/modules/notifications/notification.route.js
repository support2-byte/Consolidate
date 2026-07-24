import express from "express";
import {
  getAllNotifications,
  getEmailSubscriptions,
  resendNotification,
} from "./notification.controller.js";

const router = express.Router();

router.get("/", getAllNotifications);
router.get("/subscriptions", getEmailSubscriptions);
router.post("/:id/resend", resendNotification);

export default router;
