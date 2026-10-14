import { Router } from "express";
import { requireAppAuth } from "../../../middleware/appAuthMiddleware.js";
import { getRates } from "./option.controller.js";

const router = Router();

router.get("/rates", requireAppAuth, getRates);

export default router;
