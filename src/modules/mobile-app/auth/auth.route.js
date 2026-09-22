import { Router } from "express";
import {
  signup,
  login,
  me,
  refresh,
  logout,
  sendOTP,
  verifyOTP,
} from "./auth.controller.js";
import { requireAppAuth } from "../../../middleware/appAuthMiddleware.js";

const router = Router();

router.post("/signup", signup);
router.post("/login", login);
router.post("/sendOTP", sendOTP);
router.post("/verifyOTP", verifyOTP);
router.post("/refresh", refresh);
router.post("/logout", requireAppAuth, logout);
router.get("/me", requireAppAuth, me);

export default router;
