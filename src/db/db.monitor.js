// src/config/db.monitor.js

const SLOW_QUERY_THRESHOLD_MS = 500;

export function applyQueryMonitoring(pool) {
    const originalQuery = pool.query.bind(pool);

    pool.query = async (...args) => {
        const start = Date.now();
        const queryText = typeof args[0] === 'string'
            ? args[0]
            : args[0]?.text || '';
        const params = args[1] ? JSON.stringify(args[1]) : null;

        try {
            const result = await originalQuery(...args);
            const duration = Date.now() - start;

            // Log slow queries to DB
            if (duration > SLOW_QUERY_THRESHOLD_MS) {
                console.warn('[SLOW QUERY]', {
                    duration_ms: duration,
                    query: queryText.slice(0, 200),
                    params: params?.slice(0, 100),
                });

                // Save to monitoring table (fire and forget)
                originalQuery(
                    `INSERT INTO monitoring.query_logs 
                        (query_text, duration_ms, params) 
                     VALUES ($1, $2, $3)`,
                    [queryText.slice(0, 500), duration, params?.slice(0, 200)]
                ).catch(err => console.error('[Monitor] Failed to log query:', err.message));
            }

            return result;

        } catch (err) {
            const duration = Date.now() - start;

            console.error('[QUERY ERROR]', {
                duration_ms: duration,
                query: queryText.slice(0, 200),
                error: err.message,
            });

            // Log errors to DB
            originalQuery(
                `INSERT INTO monitoring.query_logs 
                    (query_text, duration_ms, params, error_message) 
                 VALUES ($1, $2, $3, $4)`,
                [queryText.slice(0, 500), duration, params?.slice(0, 200), err.message]
            ).catch(e => console.error('[Monitor] Failed to log error:', e.message));

            throw err;
        }
    };

    console.log('[Monitor] Query monitoring applied ✓');
    return pool;
}