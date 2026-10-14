import pool from "../../../db/pool.js";
import logger from "../../../services/logger.js";
import jwt from "jsonwebtoken";
import bcrypt from "bcryptjs";
import crypto from "crypto";
import { otpEmailTemplate } from "../../../services/otpEmail.js";
import { transporter } from "../../../middleware/nodeMailer.js";

const JWT_SECRET = process.env.JWT_SECRET;
const ACCESS_TOKEN_TTL = "15m";
const REFRESH_TOKEN_TTL_DAYS = 30;
const OTP_TTL_MINUTES = 5;

export const signup = async (req, res) => {
  try {
    const { fullName, email, phone, password, accountType } = req.body;

    if (!fullName || !email?.trim() || !phone?.trim() || !password) {
      return res
        .status(400)
        .json({ success: false, message: "All fields are required." });
    }

    const ALLOWED_ACCOUNT_TYPES = ["receiver", "shipper"];
    const normalizedAccountType = (accountType || "receiver")
      .trim()
      .toLowerCase();

    if (!ALLOWED_ACCOUNT_TYPES.includes(normalizedAccountType)) {
      return res.status(400).json({
        success: false,
        message: "Invalid account type.",
      });
    }

    const normalizedEmail = email.trim().toLowerCase();

    const existingUser = await pool.query(
      `SELECT id FROM app_customers WHERE email = $1 LIMIT 1`,
      [normalizedEmail],
    );

    if (existingUser.rows.length > 0) {
      return res.status(409).json({
        success: false,
        message: "An account with this email already exists.",
      });
    }

    const hashedPassword = await bcrypt.hash(password, 12);

    const { rows } = await pool.query(
      `INSERT INTO app_customers ("fullName", email, phone, password, status, account_type)
       VALUES ($1, $2, $3, $4, $5, $6)
       RETURNING id, "fullName", email, phone, status, account_type`,
      [
        fullName,
        normalizedEmail,
        phone.trim(),
        hashedPassword,
        true,
        normalizedAccountType,
      ],
    );

    const user = rows[0];

    const accessToken = jwt.sign(
      {
        id: user.id,
        fullName: user.fullName,
        email: user.email,
        type: user.account_type,
      },
      JWT_SECRET,
      { algorithm: "HS256", expiresIn: ACCESS_TOKEN_TTL },
    );

    const refreshToken = crypto.randomBytes(48).toString("hex");
    const expiresAt = new Date(
      Date.now() + REFRESH_TOKEN_TTL_DAYS * 24 * 60 * 60 * 1000,
    );

    await pool.query(
      `INSERT INTO app_refresh_token (user_id, token, expires_at) VALUES ($1, $2, $3)`,
      [user.id, refreshToken, expiresAt],
    );

    logger.info("New app user registered", {
      userId: user.id,
      email: user.email,
      accountType: user.account_type,
    });

    return res.status(201).json({
      success: true,
      message: "Account created successfully.",
      data: {
        token: accessToken,
        refreshToken,
        user: {
          id: user.id,
          fullName: user.fullName,
          email: user.email,
          phone: user.phone,
          status: user.status,
          type: user.account_type,
        },
      },
    });
  } catch (error) {
    logger.error("Failed to signup app-user", { error });
    return res
      .status(500)
      .json({ success: false, message: "Something went wrong." });
  }
};

export const login = async (req, res) => {
  try {
    const { email } = req.body;

    if (!email?.trim()) {
      return res.status(400).json({
        success: false,
        message: "Email is required.",
      });
    }

    const normalizedEmail = email.trim().toLowerCase();

    const { rows } = await pool.query(
      "SELECT * FROM app_customers WHERE email = $1",
      [normalizedEmail],
    );

    const user = rows[0];

    if (!user) {
      logger.warn("No User Found");
      return res.status(404).json({
        success: false,
        message: "Account not found!",
      });
    }

    if (!user.status) {
      logger.warn("Login attempt for disabled account", { userId: user.id });
      return res.status(403).json({
        success: false,
        message: "ACCOUNT DISABLED!",
      });
    }

    const otp = crypto.randomInt(100000, 999999);
    const now = new Date();
    const expiresAt = new Date(now.getTime() + OTP_TTL_MINUTES * 60 * 1000);

    await pool.query(`DELETE FROM app_otp WHERE user_email = $1`, [
      normalizedEmail,
    ]);

    await pool.query(
      `INSERT INTO app_otp (user_email, otp, created_at, expires_at)
       VALUES ($1, $2, $3, $4)`,
      [normalizedEmail, otp, now, expiresAt],
    );

    await transporter.sendMail({
      from: `"RGSL Support" <${process.env.SMTP_FROM_EMAIL || process.env.SMTP_USER}>`,
      to: user.email,
      subject: "Your RGSL Dashboard login code",
      html: otpEmailTemplate({ otp, fullName: user.fullName }),
    });

    logger.info("Password verified, OTP sent", {
      userId: user.id,
      email: user.email,
    });

    return res.status(200).json({
      success: true,
      message: "OTP sent to your email.",
    });
  } catch (error) {
    logger.error("Failed to login app-user", { error });
    return res.status(500).json({
      success: false,
      message: "Something went wrong",
    });
  }
};

export const sendOTP = async (req, res) => {
  try {
    const { email } = req.body;

    if (!email?.trim()) {
      return res
        .status(400)
        .json({ success: false, message: "Email is required." });
    }

    const normalizedEmail = email.trim().toLowerCase();

    const { rows } = await pool.query(
      `SELECT id, "fullName", email, status, account_type FROM app_customers WHERE email = $1`,
      [normalizedEmail],
    );

    if (rows.length === 0) {
      logger.warn("OTP resend requested for unknown email", {
        email: normalizedEmail,
      });
      return res.status(404).json({
        success: false,
        message: "Account not found!",
      });
    }

    const user = rows[0];

    if (!user.status) {
      return res.status(403).json({
        success: false,
        message: "ACCOUNT DISABLED!",
      });
    }

    const otp = crypto.randomInt(100000, 999999);
    const now = new Date();
    const expiresAt = new Date(now.getTime() + OTP_TTL_MINUTES * 60 * 1000);

    await pool.query(`DELETE FROM app_otp WHERE user_email = $1`, [
      normalizedEmail,
    ]);

    await pool.query(
      `INSERT INTO app_otp (user_email, otp, created_at, expires_at)
       VALUES ($1, $2, $3, $4)`,
      [normalizedEmail, otp, now, expiresAt],
    );

    await transporter.sendMail({
      from: `"RGSL Support" <${process.env.SMTP_FROM_EMAIL || process.env.SMTP_USER}>`,
      to: user.email,
      subject: "Your RGSL Dashboard login code",
      html: otpEmailTemplate({ otp, fullName: user.fullName }),
    });

    logger.info("OTP resent", { userId: user.id, email: user.email });

    return res.status(200).json({
      success: true,
      message: "OTP resent to your email.",
    });
  } catch (error) {
    logger.error("Failed to resend OTP", { error });
    return res.status(500).json({
      success: false,
      message: "Something went wrong.",
    });
  }
};

export const verifyOTP = async (req, res) => {
  try {
    const { email, otp } = req.body;

    if (!email?.trim() || !otp) {
      return res.status(400).json({
        success: false,
        message: "Email and OTP are required.",
      });
    }

    const normalizedEmail = email.trim().toLowerCase();

    const { rows: otpRows } = await pool.query(
      `SELECT id, otp, expires_at FROM app_otp
       WHERE user_email = $1
       ORDER BY created_at DESC
       LIMIT 1`,
      [normalizedEmail],
    );

    if (otpRows.length === 0) {
      logger.warn("OTP verify attempt with no OTP on file", {
        email: normalizedEmail,
      });
      return res.status(400).json({
        success: false,
        message: "OTP not found. Please request a new one.",
      });
    }

    const otpRecord = otpRows[0];

    if (new Date() > new Date(otpRecord.expires_at)) {
      await pool.query(`DELETE FROM app_otp WHERE id = $1`, [otpRecord.id]);
      logger.warn("Expired OTP verify attempt", { email: normalizedEmail });
      return res.status(400).json({
        success: false,
        message: "OTP expired. Please request a new one.",
      });
    }

    if (Number(otpRecord.otp) !== Number(otp)) {
      logger.warn("Invalid OTP verify attempt", { email: normalizedEmail });
      return res.status(400).json({
        success: false,
        message: "Invalid OTP.",
      });
    }

    await pool.query(`DELETE FROM app_otp WHERE id = $1`, [otpRecord.id]);

    const { rows } = await pool.query(
      `SELECT id, customer_id, "fullName", email, status, account_type FROM app_customers WHERE email = $1`,
      [normalizedEmail],
    );

    const user = rows[0];

    if (!user) {
      return res.status(404).json({
        success: false,
        message: "Account not found!",
      });
    }

    if (!user.status) {
      logger.warn("OTP login attempt for disabled account", {
        userId: user.id,
      });
      return res.status(403).json({
        success: false,
        message: "ACCOUNT DISABLED!",
      });
    }

    const accessToken = jwt.sign(
      {
        id: user.id,
        customer_id: user.customer_id,
        fullName: user.fullName,
        email: user.email,
        type: user.account_type,
      },
      JWT_SECRET,
      { algorithm: "HS256", expiresIn: ACCESS_TOKEN_TTL },
    );

    const refreshToken = crypto.randomBytes(48).toString("hex");
    const refreshExpiresAt = new Date(
      Date.now() + REFRESH_TOKEN_TTL_DAYS * 24 * 60 * 60 * 1000,
    );

    await pool.query(
      `INSERT INTO app_refresh_token (user_id, token, expires_at)
       VALUES ($1, $2, $3)`,
      [user.id, refreshToken, refreshExpiresAt],
    );

    logger.info("User logged in via OTP", {
      userId: user.id,
      email: user.email,
    });

    return res.status(200).json({
      success: true,
      data: {
        token: accessToken,
        refreshToken,
        user: {
          id: user.id,
          customer_id: user.customer_id,
          fullName: user.fullName,
          email: user.email,
          type: user.account_type,
        },
      },
    });
  } catch (error) {
    logger.error("Failed to verify OTP", { error });
    return res.status(500).json({
      success: false,
      message: "Something went wrong.",
    });
  }
};

export const me = async (req, res) => {
  try {
    return res.status(200).json({
      success: true,
      data: req.user,
    });
  } catch (error) {
    logger.error("Failed to fetch current app-user", { error });
    return res.status(500).json({
      success: false,
      message: "Something went wrong.",
    });
  }
};

export const refresh = async (req, res) => {
  try {
    const { refreshToken } = req.body;

    if (!refreshToken) {
      return res.status(401).json({
        success: false,
        message: "Refresh token required.",
      });
    }

    const { rows } = await pool.query(
      `SELECT rt.id AS token_id, rt.expires_at, c.id, c."fullName", c.email, c.status, c.account_type
       FROM app_refresh_token rt
       JOIN app_customers c ON c.id = rt.user_id
       WHERE rt.token = $1`,
      [refreshToken],
    );

    const record = rows[0];

    if (!record) {
      return res.status(401).json({
        success: false,
        message: "Invalid refresh token.",
      });
    }

    if (new Date(record.expires_at) < new Date()) {
      await pool.query(`DELETE FROM app_refresh_token WHERE id = $1`, [
        record.token_id,
      ]);

      return res.status(401).json({
        success: false,
        message: "Refresh token expired.",
      });
    }

    if (!record.status) {
      return res.status(403).json({
        success: false,
        message: "Account disabled.",
      });
    }

    const accessToken = jwt.sign(
      {
        id: record.id,
        fullName: record.fullName,
        email: record.email,
        type: record.account_type,
      },
      JWT_SECRET,
      { algorithm: "HS256", expiresIn: ACCESS_TOKEN_TTL },
    );

    return res.status(200).json({
      success: true,
      data: { token: accessToken },
    });
  } catch (error) {
    logger.error("Failed to refresh app-user token", { error });
    return res.status(500).json({
      success: false,
      message: "Something went wrong.",
    });
  }
};

export const logout = async (req, res) => {
  try {
    const { refreshToken } = req.body;

    if (refreshToken) {
      await pool.query(`DELETE FROM app_refresh_token WHERE token = $1`, [
        refreshToken,
      ]);
    }

    return res.status(200).json({
      success: true,
      message: "Logged out successfully.",
    });
  } catch (error) {
    logger.error("Failed to logout app-user", { error });
    return res.status(500).json({
      success: false,
      message: "Something went wrong.",
    });
  }
};
