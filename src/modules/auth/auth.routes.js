import { Router } from "express";
import { register, login, me, logout,adminForceResetPassword } from "./auth.controller.js";
import { requireAuth } from "./auth.middleware.js";

const router = Router();
// middleware/requireAdmin.js


//   const token = req.cookies.token;
router.post("/register", register);
router.post("/login", login);
router.get("/me", me);
router.post("/logout", logout);
router.post('/admin/reset-user-password', adminForceResetPassword);
// In your routes file (protected by admin middleware)
router.post('/admin/users/:userEmailOrId/reset-password', 
    requireAuth,
  adminForceResetPassword
);
export default router;