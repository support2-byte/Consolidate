// src/modules/monitoring/monitoring.controller.js
import pool from '../../db/pool.js';
export async function getSlowQueries(req, res) {
    const client = await pool.connect();
    try {
        const result = await client.query(
            `SELECT * FROM monitoring.slow_queries LIMIT 20`
        );
        res.json(result.rows);
    } catch (err) {
        res.status(500).json({ error: err.message });
    } finally {
        client.release();
    }
}

export async function getTableActivity(req, res) {
    const client = await pool.connect();
    try {
        const result = await client.query(
            `SELECT * FROM monitoring.table_activity`
        );
        res.json(result.rows);
    } catch (err) {
        res.status(500).json({ error: err.message });
    } finally {
        client.release();
    }
}

export async function getQueryLogs(req, res) {
    const client = await pool.connect();
    try {
        const limit = parseInt(req.query.limit) || 50;
        const result = await client.query(
            `SELECT * FROM monitoring.query_logs 
             ORDER BY logged_at DESC 
             LIMIT $1`,
            [limit]
        );
        res.json(result.rows);
    } catch (err) {
        res.status(500).json({ error: err.message });
    } finally {
        client.release();
    }
}

export async function getMissingIndexes(req, res) {
    const client = await pool.connect();
    try {
        const result = await client.query(
            `SELECT * FROM monitoring.missing_indexes`
        );
        res.json(result.rows);
    } catch (err) {
        res.status(500).json({ error: err.message });
    } finally {
        client.release();
    }
}

export async function resetStats(req, res) {
    const client = await pool.connect();
    try {
        await client.query(`SELECT pg_stat_reset()`);
        await client.query(`SELECT pg_stat_statements_reset()`);
        res.json({ message: 'Stats reset successfully' });
    } catch (err) {
        res.status(500).json({ error: err.message });
    } finally {
        client.release();
    }
}