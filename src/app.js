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


// After other middleware
  dotenv.config();

const app = express();
app.use(express.json());
app.use(cookieParser());

const allowedOrigins = process.env.CLIENT_ORIGINS
  ? process.env.CLIENT_ORIGINS.split(",")
  : [
      "http://localhost:5173",
      "https://imaginative-pothos-0a1193.netlify.app",
    ];

app.use(
  cors({
    origin: allowedOrigins,
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

// Serve uploads folder statically on /uploads
app.use('/uploads', express.static(path.join(process.cwd(), 'uploads')));
app.get("/health", (_req, res) => {
  res.json({ status: "ok", time: new Date().toISOString() });
});

export default app;