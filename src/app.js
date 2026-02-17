import express from "express";
import cookieParser from "cookie-parser";
import cors from "cors";
import dotenv from "dotenv";
import path from "path";
import authRoutes from "./modules/auth/auth.routes.js";
import customerRoutes from "./modules/customers/customer.routes.js";
import vendorRoutes from "./modules/vendors/vendorRoutes.js";
import containerRoutes from "./modules/containers/container.routes.js";
import orderRoutes from './modules/orders/orderRoutes.js'
import consignmentRoutes from './modules/consignment/consignment.routes.js';
import optionsRoutes from './modules/options/options.routes.js';
import sendOrderEmail from "./middleware/nodeMailer.js";
import { getCustomersPanel } from "./modules/customers/customer.controller.js";
import webhook from "./modules/customers/webhook.js"
// After other middleware
  dotenv.config();

const app = express();
app.use(express.json());
app.use(cookieParser());

const allowedOrigins = process.env.CLIENT_ORIGINS
  ? process.env.CLIENT_ORIGINS.split(",").map(o => o.trim())
  : [
      "http://localhost:5173",
      "http://127.0.0.1:5501",
      "http://localhost:3000",
      "http://localhost:5000",
      "http://127.0.0.1:5000",
      "http://localhost:5500",
      "http://localhost:8000",          // python http.server
      "http://192.168.100.160:56445",   // ← Add your exact current origin here (temporary)
      "http://192.168.100.160:*",
      "http://192.168.100.162:*",
      "http://192.168.1.29:*",
      "http://192.168.137.1:*",
      "192.168.137.85:5000",
      "origin: '*'",       // Wildcard port (not perfect, but works for testing)
      "https://imaginative-pothos-0a1193.netlify.app",
    ];
app.use(
  cors({
    origin: function (origin, callback) {
      // Allow requests with NO origin (file://, Postman, curl, etc.)
      if (!origin || allowedOrigins.includes(origin)) {
        callback(null, true);
      } else {
        console.log(`Rejected origin: ${origin}`); // ← debug log
        callback(new Error('Not allowed by CORS'));
      }
    },
    credentials: true,
    methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"],
  })
);
app.use("/auth", authRoutes);
app.use("/api/customers", customerRoutes);
app.use("/api/vendors", vendorRoutes);
app.use("/api/containers", containerRoutes);
app.use("/api/orders", orderRoutes);
app.use("/api/consignments", consignmentRoutes);
app.use('/api/options', optionsRoutes);
app.use('/api/zohoCustomer', webhook);
app.use('/api/customerPanals', getCustomersPanel);

// Serve uploads folder statically on /uploads
app.use('/uploads', express.static(path.join(process.cwd(), 'uploads')));
app.get("/health", (_req, res) => {
  res.json({ status: "ok", time: new Date().toISOString() });
});

export default app;