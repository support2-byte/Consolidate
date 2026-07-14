import dotenv from "dotenv";
import nodemailer from "nodemailer";
import logger from "../services/logger.js";
dotenv.config();

logger.info("Email configuration loaded", {
  service: process.env.EMAIL_SERVICE,
  userConfigured: !!process.env.GMAIL_USER,
  fromAddress: process.env.GMAIL_FROM_ADDRESS,
});

const transporter = nodemailer.createTransport({
  service: process.env.EMAIL_SERVICE || "gmail",
  auth: {
    user: process.env.GMAIL_USER,
    pass: process.env.GMAIL_PASS,
  },
  secure: true,
  tls: {
    rejectUnauthorized: false,
  },
});

transporter.verify((error, success) => {
  if (error) {
    logger.error("Email transporter verification failed", {
      error: error.message,
    });
  } else {
    logger.info("Email transporter ready");
  }
});

export { transporter };
