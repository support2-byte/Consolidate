import { Router } from "express";
import { register, login, me, logout } from "./auth.controller.js";

const router = Router();

router.post("/register", register);
router.post("/login", login);
router.get("/me", me);
router.post("/logout", logout);

export default router;