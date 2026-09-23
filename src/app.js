import express from "express";
import cookieParser from "cookie-parser";
import cors from "cors";
import dotenv from "dotenv";
import path from "path";
import swaggerJsdoc from "swagger-jsdoc";
import swaggerUi from "swagger-ui-express";
import authRoutes from "./modules/auth/auth.routes.js";
import customerRoutes from "./modules/customers/customer.routes.js";
import vendorRoutes from "./modules/vendors/vendorRoutes.js";
import containerRoutes from "./modules/containers/container.routes.js";
import orderRoutes from "./modules/orders/orderRoutes.js";
import consignmentRoutes from "./modules/consignment/consignment.routes.js";
import optionsRoutes from "./modules/options/options.routes.js";
import monitorRoutes from "./modules/monitoring/monitorRoutes.js";
import internalRoutes from "./modules/intenral/internal.route.js";
import notificationRoutes from "./modules/notifications/notification.route.js";
import kycRoutes from "./modules/kyc/kyc.route.js";
import bookingRoutes from "./modules/booking-confirmation/booking.routes.js";
import invoiceRoutes from "./modules/invoices/invoice.route.js";
import deliveryRoutes from "./modules/deliveries/delivery.route.js";
import storageRoutes from "./modules/storage/storage.route.js";
import dropOffRoutes from "./modules/drop-offs/drop-off.route.js";
import addressRoutes from "./modules/address/address.route.js";
import { getCustomersPanel } from "./modules/customers/customer.controller.js";
import webhook from "./modules/customers/webhook.js";
import {
  globalErrorHandler,
  notFoundHandler,
} from "./middleware/errorHandler.js";

import appAuthRoutes from "./modules/mobile-app/auth/auth.route.js";
import appDashboardRoutes from "./modules/mobile-app/dashboard/dashboard.route.js";
import appShipmentRoutes from "./modules/mobile-app/shipments/shipment.route.js";
import appInvoiceRoutes from "./modules/mobile-app/invoices/invoice.route.js";
import appOptionRoutes from "./modules/mobile-app/options/option.route.js";

dotenv.config();

const app = express();
app.use(express.json());
app.use(cookieParser());

const allowedOrigins = process.env.CLIENT_ORIGINS
  ? process.env.CLIENT_ORIGINS.split(",").map((o) => o.trim())
  : [
      "http://localhost:5173",
      "http://127.0.0.1:5500",
      "http://localhost:3000",
      "http://localhost:5000",
      "http://127.0.0.1:5000",
      "http://localhost:5500",
      "http://localhost:8000",
      "http://192.168.100.160:*",
      "http://192.168.100.162:*",
      "http://192.168.1.29:*",
      "http://192.168.137.1:*",
      "http://192.168.137.85:*",
      "https://consolidatetracking.onrender.com",
      "https://imaginative-pothos-0a1193.netlify.app",
      "https://orders.royalgulfshipping.com",
      "https://trackorder.royalgulfshipping.com",
    ];

function isOriginAllowed(origin) {
  if (!origin || origin === "null") return true;
  return allowedOrigins.some((pattern) => {
    if (pattern.endsWith(":*")) {
      const base = pattern.slice(0, -2);
      return origin.startsWith(base + ":") || origin === base;
    }
    return origin === pattern;
  });
}

app.use(
  cors({
    origin: function (origin, callback) {
      if (isOriginAllowed(origin)) {
        callback(null, true);
      } else {
        console.log(`Rejected origin: ${origin}`);
        callback(new Error("Not allowed by CORS"));
      }
    },
    credentials: true,
    methods: ["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"],
  }),
);

const swaggerSpec = swaggerJsdoc({
  definition: {
    openapi: "3.0.0",
    info: {
      title: "Consolidate Dashboard API",
      version: "1.0.0",
      description: "API documentation for the Consolidate Dashboard backend",
    },
    servers: [
      { url: "http://localhost:5000", description: "Local dev server" },
      {
        url: "https://consolidate.onrender.com",
        description: "Production server",
      },
    ],
    components: {
      securitySchemes: {
        cookieAuth: {
          type: "apiKey",
          in: "cookie",
          name: "accessToken",
        },
      },
    },
  },
  apis: [
    "./src/modules/**/*.routes.js",
    "./src/modules/**/*Routes.js",
    "./src/modules/**/*.schemas.js",
  ],
});

app.use("/api-docs", swaggerUi.serve, swaggerUi.setup(swaggerSpec));
app.get("/api-docs.json", (_req, res) => {
  res.setHeader("Content-Type", "application/json");
  res.send(swaggerSpec);
});

app.use("/auth", authRoutes);
app.use("/api/customers", customerRoutes);
app.use("/api/vendors", vendorRoutes);
app.use("/api/containers", containerRoutes);
app.use("/api/orders", orderRoutes);
app.use("/api/consignments", consignmentRoutes);
app.use("/api/options", optionsRoutes);
app.use("/api/zohoCustomer", webhook);
app.use("/api/customerPanals", getCustomersPanel);
app.use("/api/monitoring", monitorRoutes);
app.use("/api/internal", internalRoutes);
app.use("/api/notifications", notificationRoutes);
app.use("/api/kyc", kycRoutes);
app.use("/api/booking", bookingRoutes);
app.use("/api/invoices", invoiceRoutes);
app.use("/api/deliveries", deliveryRoutes);
app.use("/api/storage", storageRoutes);
app.use("/api/drop-off", dropOffRoutes);
app.use("/api/address", addressRoutes);

app.use("/api/mobile-app/auth", appAuthRoutes);
app.use("/api/mobile-app/dashboard", appDashboardRoutes);
app.use("/api/mobile-app/shipments", appShipmentRoutes);
app.use("/api/mobile-app/invoices", appInvoiceRoutes);
app.use("/api/mobile-app/options", appOptionRoutes);

app.get("/api/mobile-app/payments/ngenius/redirect", (req, res) => {
  res.send(`
    <!DOCTYPE html>
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
      body {
        margin: 0;
        height: 100vh;
        display: flex;
        align-items: center;
        justify-content: center;
        background: #0d6c6a;
        font-family: -apple-system, sans-serif;
      }
      .card {
        text-align: center;
        animation: fadeUp 0.5s ease-out;
      }
      .check {
        width: 72px;
        height: 72px;
        border-radius: 50%;
        background: #fff;
        display: flex;
        align-items: center;
        justify-content: center;
        margin: 0 auto 20px;
        animation: pop 0.4s ease-out 0.1s both;
      }
      .check svg {
        width: 36px;
        height: 36px;
        stroke: #0d6c6a;
        stroke-width: 3;
        fill: none;
        stroke-dasharray: 48;
        stroke-dashoffset: 48;
        animation: draw 0.4s ease-out 0.4s forwards;
      }
      h1 {
        color: #fff;
        font-size: 20px;
        margin: 0;
      }
      p {
        color: rgba(255,255,255,0.8);
        font-size: 14px;
        margin-top: 8px;
      }
      @keyframes fadeUp {
        from { opacity: 0; transform: translateY(12px); }
        to { opacity: 1; transform: translateY(0); }
      }
      @keyframes pop {
        from { transform: scale(0); }
        to { transform: scale(1); }
      }
      @keyframes draw {
        to { stroke-dashoffset: 0; }
      }
    </style>
    </head>
    <body>
      <div class="card">
        <div class="check">
          <svg viewBox="0 0 24 24"><polyline points="4,13 9,18 20,6"/></svg>
        </div>
        <h1>Payment Processed</h1>
        <p>You can close this window</p>
      </div>
    </body>
    </html>
  `);
});

app.use("/uploads", express.static(path.join(process.cwd(), "uploads")));
app.get("/health", (_req, res) => {
  res.json({ status: "ok", time: new Date().toISOString() });
});

app.use(notFoundHandler);
app.use(globalErrorHandler);

export default app;
