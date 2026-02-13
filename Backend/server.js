// server.js
// Requirements:
// npm i express cors bcryptjs pg dotenv multer multer-storage-cloudinary cloudinary
// Additional (for added features):
// npm i @sentry/node helmet express-rate-limit prom-client ioredis bullmq
require('dotenv').config();

/////////////////////////////////////////////////////////////////////
// Added infra: Sentry, Helmet, Rate limiter, Prometheus, Redis & Bull
/////////////////////////////////////////////////////////////////////
const Sentry = require('@sentry/node'); // optional - set SENTRY_DSN in env
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const promClient = require('prom-client');
const IORedis = require('ioredis');
const { Queue, Worker } = require('bullmq');

/////////////////////////////////////////////////////////////////////
// Original imports
/////////////////////////////////////////////////////////////////////
const express = require('express');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');
const bcrypt = require('bcryptjs');
const { Pool } = require('pg');

/////////////////////////////////////////////////////////////////////
// Sentry init (optional)
/////////////////////////////////////////////////////////////////////
if (process.env.SENTRY_DSN) {
  Sentry.init({
    dsn: process.env.SENTRY_DSN,
    tracesSampleRate: Number(process.env.SENTRY_TRACES_SAMPLE_RATE || 0.2) // adjust as needed
  });
  console.log('Sentry initialized');
}

const app = express();
const PORT = process.env.PORT || 3000;

/////////////////////////////////////////////////////////////////////
// Attach Sentry request/tracing handlers early (if enabled)
/////////////////////////////////////////////////////////////////////
if (process.env.SENTRY_DSN) {
  app.use(Sentry.Handlers.requestHandler());
  app.use(Sentry.Handlers.tracingHandler());
}

/////////////////////////////////////////////////////////////////////
// Security & rate limiting
/////////////////////////////////////////////////////////////////////
app.use(helmet());

const limiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: Number(process.env.RATE_LIMIT_MAX || 200), // limit each IP
  standardHeaders: true,
  legacyHeaders: false
});
// apply to all requests
app.use(limiter);

/////////////////////////////////////////////////////////////////////
// CORS + body parsing (original)
/////////////////////////////////////////////////////////////////////
app.use(cors({ origin: true, credentials: true }));
app.use(express.json({ limit: '10mb' })); // allow some room for base64 if demo

/////////////////////////////////////////////////////////////////////
// Prometheus metrics
/////////////////////////////////////////////////////////////////////
promClient.collectDefaultMetrics({
  // prefix if you want: prefix: 'wireconnect_',
  timeout: 5000
});
// Example custom metric (requests counter)
const httpRequestCounter = new promClient.Counter({
  name: 'wireconnect_http_requests_total',
  help: 'Total number of HTTP requests',
  labelNames: ['method', 'route', 'status_code']
});

// middleware to increment metric
app.use((req, res, next) => {
  const start = Date.now();
  res.on('finish', () => {
    const route = req.path || req.originalUrl || 'unknown';
    httpRequestCounter.inc({ method: req.method, route, status_code: res.statusCode }, 1);
  });
  next();
});

app.get('/metrics', async (req, res) => {
  try {
    res.set('Content-Type', promClient.register.contentType);
    res.end(await promClient.register.metrics());
  } catch (err) {
    res.status(500).end(err.message);
  }
});

/////////////////////////////////////////////////////////////////////
// Redis + BullMQ setup (optional - only requires REDIS env variables)
/////////////////////////////////////////////////////////////////////
let redis = null;
let jobQueue = null;
let jobWorker = null;
const REDIS_URL = process.env.REDIS_URL || (process.env.REDIS_HOST ? `redis://${process.env.REDIS_HOST}:${process.env.REDIS_PORT||6379}` : null);

if (REDIS_URL) {
  try {
    redis = new IORedis(REDIS_URL, {
      maxRetriesPerRequest: 1,
      enableReadyCheck: true,
      // add other ioredis options if needed (password, tls, etc)
    });
    // create BullMQ queue
    jobQueue = new Queue('wireconnect-job-queue', { connection: redis, defaultJobOptions: { removeOnComplete: true, attempts: 3 } });

    // Simple example Worker - keep light and move heavy processing to separate worker process in production.
    jobWorker = new Worker('wireconnect-job-queue', async job => {
      // job.data contains whatever you push to the queue
      console.log('Bull Worker processing job id=', job.id, 'name=', job.name, 'data=', job.data);

      // Example: if job.name === 'notify-tech', call an internal function (send push, email, etc)
      if (job.name === 'notify-tech') {
        // send push or notification - placeholder
        // await notifyTechnician(job.data);
        console.log('Simulated notify-tech:', job.data);
      }

      // return value becomes job.returnvalue
      return { ok: true };
    }, { connection: redis });

    jobWorker.on('completed', (job, result) => {
      console.log(`Job ${job.id} completed`, result);
    });
    jobWorker.on('failed', (job, err) => {
      console.error(`Job ${job.id} failed:`, err && err.message ? err.message : err);
    });

    console.log('Redis & BullMQ configured using', REDIS_URL);
  } catch (err) {
    console.error('Redis/BullMQ setup failure (continuing without queue):', err);
    redis = null;
    jobQueue = null;
    jobWorker = null;
  }
} else {
  console.log('REDIS_URL not set: skipping Redis & BullMQ setup.');
}

/////////////////////////////////////////////////////////////////////
// Original DB + upload + all existing code starts here (unchanged logic)
// I have inserted the added infra above and will not remove anything below.
/////////////////////////////////////////////////////////////////////

// Postgres pool
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.PGSSLMODE === 'require' ? { rejectUnauthorized:false } : false
});

// multi upload (we'll support both disk and cloudinary; choose at runtime)
const multer = require('multer');
const path = require('path');
const fs = require('fs');

// ---------- LOCAL DISK STORAGE (kept as fallback) ----------
const UPLOAD_DIR = path.join(__dirname, 'uploads', 'kyc');
try { fs.mkdirSync(UPLOAD_DIR, { recursive: true }); } catch (e) { /* ignore */ }

const diskStorage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, UPLOAD_DIR);
  },
  filename: (req, file, cb) => {
    // keep original name with timestamp prefix to avoid collisions
    const name = Date.now() + '-' + file.originalname.replace(/\s+/g, '_');
    cb(null, name);
  }
});

const uploadDisk = multer({
  storage: diskStorage,
  limits: {
    // adjust as needed
    fileSize: 50 * 1024 * 1024 // 50MB per file
  }
});

// ------------------ DB init & migrations (idempotent) ------------------
// Create core tables if missing, and run safe ALTERs to add missing columns for older DBs.
const initSql = `
CREATE TABLE IF NOT EXISTS users (
  id TEXT PRIMARY KEY,
  role TEXT NOT NULL,
  email TEXT NOT NULL UNIQUE,
  phone TEXT NOT NULL UNIQUE,
  fullname TEXT NOT NULL,
  username TEXT NOT NULL UNIQUE,
  state TEXT NOT NULL,
  lga TEXT NOT NULL,
  city TEXT NOT NULL,
  gender TEXT,
  specializations TEXT[],
  password_hash TEXT NOT NULL,
  kyc_status TEXT DEFAULT 'Unverified',
  avatar_url TEXT,
  profile_complete boolean DEFAULT false,
  account_details JSONB,
  kyc_documents TEXT[],
  kyc_submitted_at TIMESTAMP WITH TIME ZONE,
  online boolean DEFAULT false,
  lat double precision,
  lng double precision,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE TABLE IF NOT EXISTS announcements (
  id SERIAL PRIMARY KEY,
  title TEXT NOT NULL,
  body TEXT NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE TABLE IF NOT EXISTS articles (
  id SERIAL PRIMARY KEY,
  title TEXT NOT NULL,
  excerpt TEXT,
  body TEXT,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE TABLE IF NOT EXISTS transactions (
  id TEXT PRIMARY KEY,
  job_id TEXT REFERENCES jobs(id),
  client_id TEXT REFERENCES users(id),
  tech_id TEXT REFERENCES users(id),
  amount NUMERIC,
  currency TEXT DEFAULT 'NGN',
  method TEXT,
  status TEXT,
  metadata JSONB,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE TABLE IF NOT EXISTS disputes (
  id TEXT PRIMARY KEY,
  job_id TEXT REFERENCES jobs(id),
  claimant_id TEXT REFERENCES users(id),
  defendant_id TEXT REFERENCES users(id),
  reason TEXT,
  details TEXT,
  status TEXT DEFAULT 'open',
  admin_note TEXT,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);
CREATE TABLE IF NOT EXISTS staff (
  id TEXT PRIMARY KEY,
  fullname TEXT NOT NULL,
  email TEXT NOT NULL UNIQUE,
  role TEXT NOT NULL,
  password_hash TEXT NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);
CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  client_id TEXT NOT NULL REFERENCES users(id),
  role_required TEXT NOT NULL DEFAULT 'technician',
  state TEXT NOT NULL,
  city TEXT,
  address TEXT,
  lat DOUBLE PRECISION,
  lng DOUBLE PRECISION,
  job_type TEXT,
  description TEXT,
  price NUMERIC,
  status TEXT NOT NULL DEFAULT 'created',
  assigned_tech_id TEXT REFERENCES users(id),
  assigned_at TIMESTAMP WITH TIME ZONE,
  expires_at TIMESTAMP WITH TIME ZONE,
  workers_needed INTEGER DEFAULT 1,
  estimated_days INTEGER DEFAULT 1,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE TABLE IF NOT EXISTS kyc_requests (
  id SERIAL PRIMARY KEY,
  user_id TEXT NOT NULL REFERENCES users(id),
  id_type TEXT,
  id_number TEXT,
  id_images TEXT[],
  selfie TEXT,                 -- ✅ ADDED HERE
  work_video TEXT,
  notes TEXT,
  status TEXT DEFAULT 'pending',
  admin_note TEXT,
  submitted_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
  decided_at TIMESTAMP WITH TIME ZONE
);
`;

// Additional safe ALTERs to add columns that may be missing in older DBs
const alterUsersSql = `
ALTER TABLE users ADD COLUMN IF NOT EXISTS avatar_url TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS profile_complete boolean DEFAULT false;
ALTER TABLE users ADD COLUMN IF NOT EXISTS account_details JSONB;
ALTER TABLE users ADD COLUMN IF NOT EXISTS kyc_documents TEXT[];
ALTER TABLE users ADD COLUMN IF NOT EXISTS kyc_submitted_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE users ADD COLUMN IF NOT EXISTS online boolean DEFAULT false;
ALTER TABLE users ADD COLUMN IF NOT EXISTS lat double precision;
ALTER TABLE users ADD COLUMN IF NOT EXISTS lng double precision;
ALTER TABLE users ADD COLUMN IF NOT EXISTS created_at TIMESTAMP WITH TIME ZONE DEFAULT now();
`;

// Ensure older DBs have the new columns (safe ALTER statements)
const alterJobsSql = `
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS workers_needed INTEGER DEFAULT 1;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS estimated_days INTEGER DEFAULT 1;
`;
//Ensure older DBs have the new colums
const alterStaffSql = `
ALTER TABLE staff ADD COLUMN IF NOT EXISTS fullname TEXT;
ALTER TABLE staff ADD COLUMN IF NOT EXISTS email TEXT;
ALTER TABLE staff ADD COLUMN IF NOT EXISTS role TEXT;
ALTER TABLE staff ADD COLUMN IF NOT EXISTS password_hash TEXT;
ALTER TABLE staff ADD COLUMN IF NOT EXISTS created_at TIMESTAMP WITH TIME ZONE DEFAULT now();
`;
//Ensure older DBs have new columns
const alterKycSql = `
ALTER TABLE kyc_requests ADD COLUMN IF NOT EXISTS selfie TEXT;
`;
// Ensure messages table exists (some versions referenced it)
const createMessagesTableSql = `
CREATE TABLE IF NOT EXISTS messages (
  id SERIAL PRIMARY KEY,
  job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  sender_id TEXT NOT NULL REFERENCES users(id),
  text TEXT NOT NULL,
  metadata JSONB,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);
`;

(async ()=> {
  try{
    await pool.query(initSql);
    await pool.query(alterUsersSql);
    await pool.query(alterKycSql);
    await pool.query(alterJobsSql);
    await pool.query(createMessagesTableSql);
    console.log('DB ready and migrations applied.');
  } catch(err){
    console.error('DB init/migration error', err);
    process.exit(1);
  }
})();

// Helpers
function validEmail(email){ return /\S+@\S+\.\S+/.test(email || ''); }
function validPhone(ph){ if(!ph) return false; const cleaned = ph.replace(/\s+/g,''); return /^(?:\+234|0)?\d{10}$/.test(cleaned); }
function uid(){ return Math.floor(1000000000 + Math.random()*9000000000).toString(); }
function distanceMeters(lat1, lon1, lat2, lon2){
  if(lat1 == null || lon1 == null || lat2 == null || lon2 == null) return Number.POSITIVE_INFINITY;
  const R = 6371000;
  const toRad = v => v * Math.PI / 180;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a = Math.sin(dLat/2)**2 + Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon/2)**2;
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
  return R * c;
}


// ------------------ CLOUDINARY CONFIG (safe public fallback + diagnostics) ------------------
const crypto = require('crypto'); // already used elsewhere, safe to require again
let uploadCloud = null;

try {
  if (
    process.env.CLOUDINARY_CLOUD_NAME &&
    process.env.CLOUDINARY_API_KEY &&
    process.env.CLOUDINARY_API_SECRET
  ) {
    const cloudinary = require('cloudinary').v2;
    const { CloudinaryStorage } = require('multer-storage-cloudinary');

    // ensure secure delivery
    cloudinary.config({
      cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
      api_key: process.env.CLOUDINARY_API_KEY,
      api_secret: process.env.CLOUDINARY_API_SECRET,
      secure: true
    });

    // Build CloudinaryStorage with safe options
    const cloudStorage = new CloudinaryStorage({
      cloudinary,
      params: async (req, file) => {
        // Use 'auto' so Cloudinary handles image vs video
        const isVideo = file.mimetype && file.mimetype.startsWith && file.mimetype.startsWith('video/');
        return {
          folder: isVideo ? 'wireconnect/kyc/videos' : 'wireconnect/kyc/images',
          // resource_type 'auto' avoids wrong resource_type errors
          resource_type: 'auto',
          // For now keep public (same behaviour as before)
          // If later you want private, change to: type: 'private'
          // type: 'private',
          public_id: `${Date.now()}-${file.originalname.replace(/\s+/g, '_')}`
        };
      }
    });

    // Create multer instance with Cloudinary storage
    uploadCloud = multer({
      storage: cloudStorage,
      limits: { fileSize: 100 * 1024 * 1024 } // 100MB per file (same as before)
    });

    // Light API test to validate credentials (non-blocking)
    cloudinary.api.resources({ max_results: 1 })
      .then((r) => {
        console.log('Cloudinary credentials OK — resources test succeeded.');
      })
      .catch((err) => {
        // This will show up in Render logs -> helpful to debug credential / network issues
        console.error('Cloudinary API test failed — check CLOUDINARY env vars and network access.', err && err.message ? err.message : err);
        // if the test fails we still keep uploadCloud set so multer will attempt uploads,
        // but we now have a clear log message to act on.
      });

    console.log('Cloudinary configured: using Cloudinary for uploads (public).');
  } else {
    console.log('Cloudinary not configured: using local disk uploads as fallback.');
  }
} catch (err) {
  console.error('Cloudinary setup error (continuing with disk uploads):', err && err.stack ? err.stack : err);
  uploadCloud = null;
}

//Testing password
app.get('/test-admin', async (req,res)=>{
 const ok = await bcrypt.compare(
   "PUT_YOUR_REAL_PASSWORD_HERE",
   process.env.ADMIN_PASSWORD_HASH
 );

 res.json({match: ok});
});
// Registration
app.post('/api/register', async (req, res) => {
  try {
    const {
      role, email, phone, fullname, username,
      state, lga, city, gender, specializations, password
    } = req.body || {};

    if(!email || !validEmail(email)) return res.status(400).json({ success:false, message:'Invalid email' });
    if(!phone || !validPhone(phone)) return res.status(400).json({ success:false, message:'Invalid phone' });
    if(!fullname || fullname.trim().length < 3) return res.status(400).json({ success:false, message:'Invalid full name' });
    if(!username || username.trim().length < 3) return res.status(400).json({ success:false, message:'Invalid username' });
    if(!state || !lga || !city) return res.status(400).json({ success:false, message:'State/LGA/City required' });
    if(!password || password.length < 6) return res.status(400).json({ success:false, message:'Password must be at least 6 characters' });

    const client = await pool.connect();
    try {
      const dupQuery = `SELECT email, username, phone FROM users WHERE email = $1 OR username = $2 OR phone = $3 LIMIT 1`;
      const dupRes = await client.query(dupQuery, [email, username, phone]);
      if(dupRes.rows.length){
        return res.status(409).json({ success:false, message: 'Email, username or phone already exists' });
      }

      const salt = await bcrypt.genSalt(10);
      const hash = await bcrypt.hash(password, salt);

      const newUser = {
        id: uid(),
        role: role || 'client',
        email, phone, fullname, username,
        state, lga, city,
        gender: gender || 'other',
        specializations: Array.isArray(specializations) ? specializations : [],
        password_hash: hash,
        kyc_status: (role === 'worker') ? 'not_required' : 'not_required'
      };

      const insertSql = `
        INSERT INTO users (id, role, email, phone, fullname, username, state, lga, city, gender, specializations, password_hash, kyc_status)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
      `;
      await client.query(insertSql, [
        newUser.id, newUser.role, newUser.email, newUser.phone,
        newUser.fullname, newUser.username, newUser.state, newUser.lga,
        newUser.city, newUser.gender, newUser.specializations, newUser.password_hash, newUser.kyc_status
      ]);

      return res.json({ success:true, message:'Account created successfully', userId: newUser.id });

    } finally {
      client.release();
    }

  } catch(err){
    console.error('Server error /api/register', err);
    return res.status(500).json({ success:false, message:'Server error' });
  }
});
