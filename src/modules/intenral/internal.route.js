import express from "express";
import { processEmailQueue, verifyRecaptcha } from "./internal.controller.js";
import { requireAuth } from "../auth/auth.middleware.js";

const router = express.Router();

router.post("/process-email-queue", requireAuth, processEmailQueue);
router.post("/verify-recaptcha", verifyRecaptcha);

export default router;
