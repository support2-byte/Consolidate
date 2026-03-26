import pkg from 'pg';
const { Pool } = pkg;
import dotenv from "dotenv";
dotenv.config();
  
import { applyQueryMonitoring } from './db.monitor.js';

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }, // required for Neon
});

// Apply monitoring
applyQueryMonitoring(pool);

export default pool;