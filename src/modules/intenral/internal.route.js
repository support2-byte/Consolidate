import express from "express";
import { processEmailQueue } from "./internal.controller";

const router = express.Router();

const BATCH_SIZE = 5;
const INTERNAL_SECRET = process.env.EMAIL_QUEUE_SECRET;

router.post("/process-email-queue", processEmailQueue);

export default router;
