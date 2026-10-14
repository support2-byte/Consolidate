import express from "express";
import {
  getBookingOrder,
  saveConfirmationData,
  submitBookingConfirmation,
  verifyBookingOtp,
  listBookingConfirmations,
  getBookingConfirmationById,
  listSubmissions,
  getSubmissionDetail,
} from "./booking.controller.js";
import { requireAuth } from "../auth/auth.middleware.js";
import { bookingConfirmationUpload } from "../../middleware/upload.js";

const router = express.Router();

router.get("/list", requireAuth, listBookingConfirmations);
router.get("/submissions", requireAuth, listSubmissions);
router.get("/submissions/:submissionId", requireAuth, getSubmissionDetail);
router.get("/:id", requireAuth, getBookingConfirmationById);

router.post("/confirmation-email-queue", requireAuth, saveConfirmationData);
router.get("/:formId/:type", getBookingOrder);
router.post("/verify-otp", verifyBookingOtp);
router.post("/", bookingConfirmationUpload, submitBookingConfirmation);

export default router;
