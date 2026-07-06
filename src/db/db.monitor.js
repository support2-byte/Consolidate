import logger from "../services/logger.js";

const SLOW_QUERY_THRESHOLD_MS = 500;

export function applyQueryMonitoring(pool) {
  const originalQuery = pool.query.bind(pool);

  pool.query = async (...args) => {
    const start = Date.now();
    const queryText =
      typeof args[0] === "string" ? args[0] : args[0]?.text || "";
    const params = args[1] ? JSON.stringify(args[1]) : null;

    try {
      const result = await originalQuery(...args);
      const duration = Date.now() - start;

      if (duration > SLOW_QUERY_THRESHOLD_MS) {
        logger.warn("Slow database query detected", {
          durationMs: duration,
          query: queryText.slice(0, 200),
        });
        originalQuery(
          `INSERT INTO monitoring.query_logs 
                        (query_text, duration_ms, params) 
                     VALUES ($1, $2, $3)`,
          [queryText.slice(0, 500), duration, params?.slice(0, 200)],
        ).catch((err) =>
          logger.error("Failed to persist slow query log", {
            error: err,
          }),
        );
      }

      return result;
    } catch (err) {
      const duration = Date.now() - start;

      logger.error("Database query failed", {
        durationMs: duration,
        query: queryText.slice(0, 200),
        error: err,
      });

      originalQuery(
        `INSERT INTO monitoring.query_logs 
                    (query_text, duration_ms, params, error_message) 
                 VALUES ($1, $2, $3, $4)`,
        [queryText.slice(0, 500), duration, params?.slice(0, 200), err.message],
      ).catch((e) =>
        logger.error("Failed to persist database query error log", {
          error: e,
        }),
      );

      throw err;
    }
  };

  logger.info("Database query monitoring enabled", {
    slowQueryThresholdMs: SLOW_QUERY_THRESHOLD_MS,
  });
  return pool;
}
