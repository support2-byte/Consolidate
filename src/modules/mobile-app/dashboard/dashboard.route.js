import { Router } from "express";
import { getDashboardData, getRates } from "./dashboard.controller.js";
import { requireAppAuth } from "../../../middleware/appAuthMiddleware.js";

const router = Router();

router.get("/rates", requireAppAuth, getRates);
router.get("/:id", getDashboardData);

export default router;
