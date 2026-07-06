import app from "./src/app.js";
import logger from "./src/services/logger.js";

const PORT = process.env.PORT || 5000;

const server = app.listen(PORT, "0.0.0.0", () => {
  logger.info("Server started", {
    host: "0.0.0.0",
    port: PORT,
    environment: process.env.NODE_ENV || "development",
  });
});

server.on("error", (err) => {
  logger.error("Failed to start server", {
    error: err,
  });
  process.exit(1);
});
