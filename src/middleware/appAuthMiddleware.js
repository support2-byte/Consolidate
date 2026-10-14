import jwt from "jsonwebtoken";
import pool from "../db/pool.js";
import logger from "../services/logger.js";

const JWT_SECRET = process.env.JWT_SECRET;

export const requireAppAuth = async (req, res, next) => {
  try {
    const authHeader = req.headers.authorization;

    if (!authHeader?.startsWith("Bearer ")) {
      return res
        .status(401)
        .json({ success: false, message: "Authentication required." });
    }

    const token = authHeader.split(" ")[1];

    let decoded;
    try {
      decoded = jwt.verify(token, JWT_SECRET);
    } catch (err) {
      return res
        .status(401)
        .json({ success: false, message: "Invalid or expired token." });
    }

    const { rows } = await pool.query(
      `SELECT id, customer_id, "fullName", email, status, account_type FROM app_customers WHERE id = $1`,
      [decoded.id],
    );

    const dbUser = rows[0];

    if (!dbUser) {
      return res
        .status(401)
        .json({ success: false, message: "Account not found." });
    }

    if (!dbUser.status) {
      return res
        .status(403)
        .json({ success: false, message: "Account disabled." });
    }

    req.user = {
      id: dbUser.id,
      customer_id: dbUser.customer_id,
      fullName: dbUser.fullName,
      email: dbUser.email,
      status: dbUser.status,
      type: dbUser.account_type,
    };

    next();
  } catch (error) {
    logger.error("App auth middleware failed", { error });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong." });
  }
};
