// server.js
// Full TradeHive-enabled backend (extends your WireConnect base).
// See top comments above for environment variables and package install instructions.

require('dotenv').config();

/////////////////////////////////////////////////////////////////////
// infra: Sentry, Helmet, Rate limiter, Prometheus, Redis & Bull
/////////////////////////////////////////////////////////////////////
const Sentry = require('@sentry/node'); // optional - set SENTRY_DSN in env
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const promClient = require('prom-client');
const IORedis = require('ioredis');
const { Queue, Worker } = require('bullmq');

/////////////////////////////////////////////////////////////////////
// Original imports + extras
/////////////////////////////////////////////////////////////////////
const express = require('express');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');
const bcrypt = require('bcryptjs');
const fetch = global.fetch || require('node-fetch'); // node 18+ has fetch, otherwise install node-fetch
const crypto = require('crypto');
const multer = require('multer');
const path = require('path');
const fs = require('fs');

let stripeLib = null;
if (process.env.STRIPE_SECRET_KEY) {
  try {
    stripeLib = require('stripe')(process.env.STRIPE_SECRET_KEY);
  } catch (e) {
    console.warn('Stripe library not available; stripe features will require `npm i stripe` and STRIPE env keys.');
    stripeLib = null;
  }
}

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
// CORS + body parsing
/////////////////////////////////////////////////////////////////////
// We'll use express.json() generally, but for Stripe webhook we will use express.raw() on that specific route.
app.use(cors({ origin: true, credentials: true }));
app.use(express.json({ limit: '12mb' })); // allow some room for base64 if needed

/////////////////////////////////////////////////////////////////////
// Prometheus metrics
/////////////////////////////////////////////////////////////////////
promClient.collectDefaultMetrics({
  timeout: 5000
});
const httpRequestCounter = new promClient.Counter({
  name: 'tradehive_http_requests_total',
  help: 'Total number of HTTP requests',
  labelNames: ['method', 'route', 'status_code']
});
app.use((req, res, next) => {
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
// Redis + BullMQ setup (optional)
/////////////////////////////////////////////////////////////////////
let redis = null;
let jobQueue = null;
let jobWorker = null;
const REDIS_URL = process.env.REDIS_URL || (process.env.REDIS_HOST ? `redis://${process.env.REDIS_HOST}:${process.env.REDIS_PORT||6379}` : null);

if (REDIS_URL) {
  try {
    redis = new IORedis(REDIS_URL, {
      maxRetriesPerRequest: 1,
      enableReadyCheck: true
    });
    jobQueue = new Queue('tradehive-jobs', { connection: redis, defaultJobOptions: { removeOnComplete: true, attempts: 3 } });

    jobWorker = new Worker('tradehive-jobs', async job => {
      console.log('Worker processing job:', job.name, job.data);
      if (job.name === 'notify-seller' || job.name === 'notify-buyer') {
        const { to, subject, message } = job.data;
        // placeholder: replace with real email / push
        console.log(`Notify (${job.name}) to ${to}: ${subject}\n${message}`);
        return { ok: true };
      }
      if (job.name === 'payout-seller') {
        const { orderId } = job.data;
        // load order/payment/user details from DB then call payoutSeller
        try {
          const client = await pool.connect();
          try {
            const ores = await client.query('SELECT * FROM orders WHERE id=$1 LIMIT 1', [orderId]);
            if (!ores.rows.length) return { ok:false, message:'order-not-found' };
            const order = ores.rows[0];
            // fetch seller user
            const sres = await client.query('SELECT * FROM users WHERE id=$1 LIMIT 1', [order.seller_id]);
            if (!sres.rows.length) return { ok:false, message:'seller-not-found' };
            const seller = sres.rows[0];
            const amount = order.amount; // assume stored in NGN (or currency)
            console.log('Attempting payout for order', orderId, 'amount', amount, 'to seller', seller.id);
            const payoutRes = await payoutSeller({
              seller,
              amount,
              currency: order.currency || 'NGN',
              metadata: { orderId }
            });
            // save payout result to payments table (we'll create record)
            if (payoutRes && payoutRes.ok) {
              await client.query('UPDATE payments SET payout_status=$1, payout_reference=$2, payout_response=$3 WHERE order_id=$4', ['paid', payoutRes.reference || null, JSON.stringify(payoutRes.raw || {}), orderId]);
              return { ok:true };
            } else {
              await client.query('UPDATE payments SET payout_status=$1, payout_response=$2 WHERE order_id=$3', ['failed', JSON.stringify(payoutRes || {}), orderId]);
              return { ok:false, message:'payout-failed', detail:payoutRes };
            }
          } finally {
            client.release();
          }
        } catch (e) {
          console.error('payout-seller job error', e);
          return { ok:false, error: e.message || e };
        }
      }
      return { ok:true };
    }, { connection: redis });

    jobWorker.on('completed', (job, result) => {
      console.log(`Job ${job.id} (${job.name}) completed`, result);
    });
    jobWorker.on('failed', (job, err) => {
      console.error(`Job ${job.id} (${job.name}) failed:`, err && err.message ? err.message : err);
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
// DB and storage setup (Postgres, migrations)
/////////////////////////////////////////////////////////////////////
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: false,
  },
});

// ---------- LOCAL DISK STORAGE fallback ----------
const UPLOAD_DIR = path.join(__dirname, 'uploads', 'kyc');
try { fs.mkdirSync(UPLOAD_DIR, { recursive: true }); } catch (e) { /* ignore */ }
const diskStorage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, UPLOAD_DIR),
  filename: (req, file, cb) => cb(null, Date.now() + '-' + file.originalname.replace(/\s+/g, '_'))
});
const uploadDisk = multer({ storage: diskStorage, limits: { fileSize: 100 * 1024 * 1024 } });

/////////////////////////////////////////////////////////////////////
// Cloudinary (optional)
/////////////////////////////////////////////////////////////////////
let uploadCloud = null;
try {
  if (process.env.CLOUDINARY_CLOUD_NAME && process.env.CLOUDINARY_API_KEY && process.env.CLOUDINARY_API_SECRET) {
    const cloudinary = require('cloudinary').v2;
    const { CloudinaryStorage } = require('multer-storage-cloudinary');

    cloudinary.config({
      cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
      api_key: process.env.CLOUDINARY_API_KEY,
      api_secret: process.env.CLOUDINARY_API_SECRET,
      secure: true
    });

    const cloudStorage = new CloudinaryStorage({
      cloudinary,
      params: async (req, file) => {
        const isVideo = file.mimetype && file.mimetype.startsWith && file.mimetype.startsWith('video/');
        return {
          folder: isVideo ? 'tradehive/kyc/videos' : 'tradehive/kyc/images',
          resource_type: 'auto',
          public_id: `${Date.now()}-${file.originalname.replace(/\s+/g, '_')}`
        };
      }
    });

    uploadCloud = multer({ storage: cloudStorage, limits: { fileSize: 100 * 1024 * 1024 } });

    // Simple test call (non-blocking)
    cloudinary.api.resources({ max_results: 1 }).then(() => {
      console.log('Cloudinary credentials OK — resources test succeeded.');
    }).catch((err) => {
      console.error('Cloudinary API test failed — check CLOUDINARY env vars and network access.', err && err.message ? err.message : err);
    });

    console.log('Cloudinary configured.');
  } else {
    console.log('Cloudinary not configured: using local disk uploads as fallback.');
  }
} catch (err) {
  console.error('Cloudinary setup error (continuing with disk uploads):', err && err.stack ? err.stack : err);
  uploadCloud = null;
}

/////////////////////////////////////////////////////////////////////
// init DB: create TradeHive tables (ads, orders, payments) and run safe ALTERs
/////////////////////////////////////////////////////////////////////
const initSql = `
-- existing core tables (some duplicates tolerated) +
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

-- TradeHive: ads (sellers post ads for goods)
CREATE TABLE IF NOT EXISTS ads (
  id TEXT PRIMARY KEY,
  seller_id TEXT NOT NULL REFERENCES users(id),
  title TEXT NOT NULL,
  description TEXT,
  images TEXT[],
  price NUMERIC NOT NULL,
  currency TEXT DEFAULT 'NGN',
  quantity INTEGER DEFAULT 1,
  location TEXT,
  category TEXT,
  subcategory TEXT,
  status TEXT DEFAULT 'draft', -- draft | pending_payment | live | removed
  kyc_required boolean DEFAULT false,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- Orders: when buyer purchases an ad
CREATE TABLE IF NOT EXISTS orders (
  id TEXT PRIMARY KEY,
  ad_id TEXT NOT NULL REFERENCES ads(id),
  buyer_id TEXT NOT NULL REFERENCES users(id),
  seller_id TEXT NOT NULL REFERENCES users(id),
  qty INTEGER DEFAULT 1,
  amount NUMERIC NOT NULL,
  currency TEXT DEFAULT 'NGN',
  status TEXT DEFAULT 'pending_payment', -- pending_payment | paid | released | completed | cancelled
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- Payments: track payments and payouts
CREATE TABLE IF NOT EXISTS payments (
  id TEXT PRIMARY KEY,
  order_id TEXT REFERENCES orders(id),
  ad_id TEXT REFERENCES ads(id),
  user_id TEXT, -- payer
  provider TEXT,
  reference TEXT, -- provider transaction reference
  amount NUMERIC,
  currency TEXT,
  status TEXT DEFAULT 'initiated', -- initiated | success | failed
  meta JSONB,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
  payout_status TEXT DEFAULT 'none', -- none | pending | paid | failed
  payout_reference TEXT,
  payout_response JSONB
);

-- ensure older / other tables exist (messages, staff, jobs)
CREATE TABLE IF NOT EXISTS staff (
  id TEXT PRIMARY KEY,
  fullname TEXT,
  email TEXT,
  role TEXT,
  password_hash TEXT,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  client_id TEXT REFERENCES users(id),
  role_required TEXT,
  state TEXT,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE TABLE IF NOT EXISTS messages (
  id SERIAL PRIMARY KEY,
  job_id TEXT REFERENCES jobs(id),
  sender_id TEXT REFERENCES users(id),
  text TEXT,
  metadata JSONB,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);
`;

// Additional safe alter statements (idempotent)
const safeAlters = `
ALTER TABLE users ADD COLUMN IF NOT EXISTS account_details JSONB;
ALTER TABLE users ADD COLUMN IF NOT EXISTS avatar_url TEXT;
ALTER TABLE ads ADD COLUMN IF NOT EXISTS created_at TIMESTAMP WITH TIME ZONE DEFAULT now();
ALTER TABLE orders ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITH TIME ZONE DEFAULT now();
`;

// Run migration
(async ()=> {
  try{
    await pool.query(initSql);
    await pool.query(safeAlters);
    console.log('DB ready and migrations applied (TradeHive).');
  } catch(err){
    console.error('DB init/migration error', err);
    process.exit(1);
  }
})();

/////////////////////////////////////////////////////////////////////
// Helpers
/////////////////////////////////////////////////////////////////////
function validEmail(email){ return /\S+@\S+\.\S+/.test(email || ''); }
function validPhone(ph){ if(!ph) return false; const cleaned = ph.replace(/\s+/g,''); return /^(?:\+234|0)?\d{10,13}$/.test(cleaned); }
function uid(){ return Math.floor(1000000000 + Math.random()*9000000000).toString(); }
function nowISO(){ return new Date().toISOString(); }

function safeJsonParse(s){
  try { return JSON.parse(s); } catch(e){ return null; }
}

/////////////////////////////////////////////////////////////////////
// Payment provider helpers: initialize payment, verify webhook, payout seller
/////////////////////////////////////////////////////////////////////
const PAYMENT_PROVIDER = (process.env.PAYMENT_PROVIDER || 'paystack').toLowerCase();

async function initializePayment({ provider = PAYMENT_PROVIDER, email, amount, currency = 'NGN', metadata = {}, callback_url }) {
  // amount should be number in major currency units (e.g., NGN)
  if (provider === 'paystack') {
    const key = process.env.PAYSTACK_SECRET_KEY;
    if (!key) throw new Error('PAYSTACK_SECRET_KEY not set');
    const res = await fetch('https://api.paystack.co/transaction/initialize', {
      method: 'POST',
      headers: { Authorization: `Bearer ${key}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        email,
        amount: Math.round(Number(amount) * 100), // in kobo
        callback_url: callback_url || (process.env.CALLBACK_BASE_URL || '') + '/pay/callback',
        metadata
      })
    });
    const body = await res.json();
    return body; // caller should check body.data.authorization_url and body.status
  }

  if (provider === 'flutterwave') {
    const key = process.env.FLUTTERWAVE_SECRET_KEY;
    if (!key) throw new Error('FLUTTERWAVE_SECRET_KEY not set');
    // Basic create payment
    const tx_ref = 'th_' + uid();
    const requestBody = {
      tx_ref,
      amount: String(amount),
      currency,
      redirect_url: callback_url || (process.env.CALLBACK_BASE_URL || '') + '/pay/callback',
      customer: { email: metadata.email || email, phonenumber: metadata.phone || '' },
      meta: metadata
    };
    const res = await fetch('https://api.flutterwave.com/v3/payments', {
      method: 'POST',
      headers: { Authorization: `Bearer ${key}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(requestBody)
    });
    const body = await res.json();
    return body;
  }

  if (provider === 'stripe') {
    if (!stripeLib) throw new Error('Stripe not configured or stripe package missing');
    // Create a PaymentIntent or Checkout Session — here we'll create a Checkout session
    const session = await stripeLib.checkout.sessions.create({
      payment_method_types: ['card'],
      mode: 'payment',
      line_items: [{ price_data: { currency, product_data: { name: metadata.title || 'TradeHive purchase' }, unit_amount: Math.round(Number(amount)*100) }, quantity: 1 }],
      success_url: (process.env.CALLBACK_BASE_URL || '') + '/pay/success?session_id={CHECKOUT_SESSION_ID}',
      cancel_url: (process.env.CALLBACK_BASE_URL || '') + '/pay/cancel'
    });
    return { ok:true, url: session.url, session };
  }

  // fallback: create a local "pending" link
  return { ok:true, url: callback_url || '/', message:'provider-not-configured' };
}

// Webhook verifying middleware for Paystack & Flutterwave
function verifyPaystackWebhook(req, res, next) {
  const sig = req.headers['x-paystack-signature'];
  const secret = process.env.PAYSTACK_SECRET_KEY;
  if (!secret) {
    console.warn('PAYSTACK_SECRET_KEY not set; skipping verification (NOT RECOMMENDED)');
    return next();
  }
  // note: Paystack expects HMAC SHA512 of raw body
  try {
    const payload = JSON.stringify(req.body);
    const hash = crypto.createHmac('sha512', secret).update(payload).digest('hex');
    if (hash === sig) return next();
    console.warn('Paystack webhook signature mismatch');
    return res.status(400).send('invalid signature');
  } catch (e) {
    console.error('Paystack verify error', e);
    return res.status(400).send('invalid signature');
  }
}

function verifyFlutterwaveWebhook(req, res, next) {
  const header = req.headers['verif-hash'] || req.headers['verif_hash'] || req.headers['x-verif-hash'];
  const secret = process.env.FLUTTERWAVE_WEBHOOK_SECRET;
  if (!secret) {
    console.warn('FLUTTERWAVE_WEBHOOK_SECRET not set; skipping verification (NOT RECOMMENDED)');
    return next();
  }
  if (!header) {
    console.warn('Flutterwave webhook missing verif-hash header');
    return res.status(400).send('missing signature');
  }
  if (header !== secret) {
    console.warn('Flutterwave webhook signature mismatch');
    return res.status(400).send('invalid signature');
  }
  return next();
}

// Stripe webhook route will use stripe.webhooks.constructEvent on raw body

// Generic payout function: tries to pay seller via configured provider
async function payoutSeller({ seller, amount, currency = 'NGN', metadata = {} }) {
  // seller is DB user row (account_details field should contain bank info)
  const provider = PAYMENT_PROVIDER;
  console.log('payoutSeller called', provider, amount, 'seller id', seller.id);

  if (!seller || !seller.account_details) {
    return { ok:false, message:'seller-account-details-missing' };
  }
  const details = seller.account_details || {}; // { account_number, bank_code, bank_name, name }

  try {
    if (provider === 'paystack') {
      const key = process.env.PAYSTACK_SECRET_KEY;
      if (!key) return { ok:false, message:'PAYSTACK_SECRET_KEY not set' };

      // 1. create transfer recipient (if not present). Ideally save recipient_code to account_details.
      let recipient_code = details.recipient_code;
      if (!recipient_code) {
        const createRes = await fetch('https://api.paystack.co/transferrecipient', {
          method: 'POST',
          headers: { Authorization: `Bearer ${key}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({
            type: 'nuban',
            name: details.name || `${seller.fullname || seller.username}`,
            account_number: details.account_number,
            bank_code: details.bank_code,
            currency: currency
          })
        });
        const cr = await createRes.json();
        if (!cr.status) return { ok:false, message:'failed-create-recipient', raw:cr };
        recipient_code = cr.data.recipient_code;
        // Ideally save recipient_code back to users.account_details in DB
        try {
          const client = await pool.connect();
          try {
            await client.query('UPDATE users SET account_details = jsonb_set(coalesce(account_details, {}::jsonb), \'{recipient_code}\', to_jsonb($1::text), true) WHERE id=$2', [recipient_code, seller.id]);
          } finally { client.release(); }
        } catch(e){ console.warn('Could not persist recipient_code', e); }
      }

      // 2. initiate transfer
      const amount_kobo = Math.round(Number(amount) * 100);
      const transferRes = await fetch('https://api.paystack.co/transfer', {
        method: 'POST',
        headers: { Authorization: `Bearer ${key}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({
          source: 'balance',
          amount: amount_kobo,
          recipient: recipient_code,
          reason: metadata && metadata.orderId ? `Payout for order ${metadata.orderId}` : 'TradeHive payout'
        })
      });
      const tr = await transferRes.json();
      if (!tr.status) return { ok:false, message:'transfer_failed', raw:tr };
      return { ok:true, reference: tr.data.transfer_code || tr.data.reference, raw: tr };
    }

    if (provider === 'flutterwave') {
      const key = process.env.FLUTTERWAVE_SECRET_KEY;
      if (!key) return { ok:false, message:'FLUTTERWAVE_SECRET_KEY not set' };

      // Create beneficiary (if not present)
      let beneficiary_id = details.beneficiary_id;
      if (!beneficiary_id) {
        const createBenef = await fetch('https://api.flutterwave.com/v3/beneficiaries', {
          method: 'POST',
          headers: { Authorization: `Bearer ${key}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({
            account_number: details.account_number,
            account_bank: details.bank_code,
            fullname: details.name || seller.fullname || seller.username
          })
        });
        const cb = await createBenef.json();
        if (!cb.status) return { ok:false, message:'failed-create-beneficiary', raw:cb };
        beneficiary_id = cb.data.id;
        try {
          const client = await pool.connect();
          try {
            await client.query('UPDATE users SET account_details = jsonb_set(coalesce(account_details, {}::jsonb), \'{beneficiary_id}\', to_jsonb($1::text), true) WHERE id=$2', [String(beneficiary_id), seller.id]);
          } finally { client.release(); }
        } catch(e) { console.warn('Could not persist beneficiary_id', e); }
      }

      // Initiate transfer
      const trRes = await fetch('https://api.flutterwave.com/v3/transfers', {
        method: 'POST',
        headers: { Authorization: `Bearer ${key}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({
          account_bank: details.bank_code,
          account_number: details.account_number,
          amount: String(amount),
          narration: metadata && metadata.orderId ? `Payout for order ${metadata.orderId}` : 'TradeHive payout',
          currency,
          reference: 'th_' + uid()
        })
      });
      const tr = await trRes.json();
      if (!(tr && (tr.status === 'success' || tr.status === true))) {
        return { ok:false, message:'transfer_failed', raw:tr };
      }
      return { ok:true, reference: tr.data && (tr.data.id || tr.data.reference), raw: tr };
    }

    if (provider === 'stripe') {
      if (!stripeLib) return { ok:false, message:'stripe library not configured' };
      // This requires connected accounts (Stripe Connect). We assume seller.account_details.connect_account_id
      const connectId = details.connect_account_id;
      if (!connectId) return { ok:false, message:'seller-missing-connect-account-id' };
      // Create Transfer to connected account. This will move funds from platform to connected account.
      const transferObj = await stripeLib.transfers.create({
        amount: Math.round(Number(amount) * 100),
        currency: currency,
        destination: connectId,
        description: `TradeHive payout`
      });
      return { ok:true, reference: transferObj.id, raw: transferObj };
    }

    // fallback (simulate)
    console.log('payoutSeller: unknown provider, simulating payout');
    return { ok:true, reference: 'sim-' + uid() };
  } catch (err) {
    console.error('payoutSeller error', err);
    return { ok:false, message:'exception', error: err && err.message, raw: err };
  }
}

/////////////////////////////////////////////////////////////////////
// Routes: registration and login (kept from your version, with small safety checks)
/////////////////////////////////////////////////////////////////////
app.post('/api/register', async (req, res) => {
  try {
    const { role, email, phone, fullname, username, state, lga, city, gender, specializations, password } = req.body || {};
    if (!email || !validEmail(email)) return res.status(400).json({ success:false, message:'Invalid email' });
    if (!phone || !validPhone(phone)) return res.status(400).json({ success:false, message:'Invalid phone' });
    if (!fullname || fullname.trim().length < 3) return res.status(400).json({ success:false, message:'Invalid full name' });
    if (!username || username.trim().length < 3) return res.status(400).json({ success:false, message:'Invalid username' });
    if (!state || !lga || !city) return res.status(400).json({ success:false, message:'State/LGA/City required' });
    if (!password || password.length < 6) return res.status(400).json({ success:false, message:'Password must be at least 6 characters' });

    const client = await pool.connect();
    try {
      const dupQuery = `SELECT email, username, phone FROM users WHERE email = $1 OR username = $2 OR phone = $3 LIMIT 1`;
      const dupRes = await client.query(dupQuery, [email, username, phone]);
      if (dupRes.rows.length) return res.status(409).json({ success:false, message: 'Email, username or phone already exists' });

      const salt = await bcrypt.genSalt(10);
      const hash = await bcrypt.hash(password, salt);
      const newUser = {
        id: uid(),
        role: role || 'client',
        email, phone, fullname, username,
        state, lga, city,
        gender: gender || 'other',
        specializations: Array.isArray(specializations) ? specializations : [],
        password_hash: hash
      };
      const insertSql = `
        INSERT INTO users (id, role, email, phone, fullname, username, state, lga, city, gender, specializations, password_hash)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
      `;
      await client.query(insertSql, [
        newUser.id, newUser.role, newUser.email, newUser.phone, newUser.fullname, newUser.username,
        newUser.state, newUser.lga, newUser.city, newUser.gender, newUser.specializations, newUser.password_hash
      ]);
      return res.json({ success:true, message:'Account created successfully', userId: newUser.id });
    } finally {
      client.release();
    }
  } catch (err) {
    console.error('Server error /api/register', err);
    return res.status(500).json({ success:false, message:'Server error' });
  }
});

app.post('/api/login', async (req, res) => {
  try {
    const { login, password, email } = req.body || {};
    if (!login || !password) return res.status(400).json({ success: false, message: 'Login and password required' });
    const loginValue = String(login).trim();
    const ADMIN_USERNAME = process.env.ADMIN_USERNAME || 'admin';
    const ADMIN_PASSWORD_HASH = process.env.ADMIN_PASSWORD_HASH || null;

    if (loginValue === ADMIN_USERNAME) {
      if (ADMIN_PASSWORD_HASH && await bcrypt.compare(password, ADMIN_PASSWORD_HASH)) {
        return res.json({ success:true, message:'Admin login successful', role:'admin', user:null });
      }
      return res.status(401).json({ success:false, message:'Invalid credentials' });
    }

    const client = await pool.connect();
    try {
      const q = `SELECT * FROM users WHERE email=$1 OR username=$1 OR phone=$1 LIMIT 1`;
      const r = await client.query(q, [loginValue]);
      if (!r.rows.length) return res.status(404).json({ success:false, message:'User not found' });
      const user = r.rows[0];
      const ok = await bcrypt.compare(password, user.password_hash);
      if (!ok) return res.status(401).json({ success:false, message:'Incorrect password' });

      const safeUser = {
        id: user.id,
        role: user.role,
        email: user.email,
        phone: user.phone,
        fullname: user.fullname,
        username: user.username,
        city: user.city,
        kyc_status: user.kyc_status,
        avatar_url: user.avatar_url,
        online: user.online,
        account_details: user.account_details
      };

      // staff redirect example
      if ((user.role || '').toLowerCase() === 'staff') {
        const BASE = process.env.ADMIN_UI_BASE || 'https://your-admin-ui.example.com';
        return res.json({ success:true, message:'Staff login', role:'staff', user:safeUser, redirect: BASE + '/staff' });
      }

      return res.json({ success:true, message:'Login successful', role:user.role||'client', user:safeUser });
    } finally {
      client.release();
    }
  } catch (e) {
    console.error('Server error /api/login', e);
    return res.status(500).json({ success:false, message:'Server error' });
  }
});

app.get('/', (req,res)=> res.send('TradeHive backend running'));

/////////////////////////////////////////////////////////////////////
// TradeHive: Ads & Orders & Payments routes
/////////////////////////////////////////////////////////////////////

// POST /api/ads - create ad (seller). If ad fee required, we'll create payment initialization and return url
app.post('/api/ads', async (req, res) => {
  // expects seller_id, title, description, images[], price, currency, quantity, location, category, subcategory
  try {
    const { seller_id, title, description, images, price, currency, quantity, location, category, subcategory } = req.body || {};
    if (!seller_id || !title || !price) return res.status(400).json({ success:false, message:'seller_id, title, price required' });

    const adId = uid();
    const client = await pool.connect();
    try {
      await client.query(`INSERT INTO ads (id, seller_id, title, description, images, price, currency, quantity, location, category, subcategory, status) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
        [adId, seller_id, title, description || '', images || [], price, currency || 'NGN', quantity || 1, location || '', category || '', subcategory || '', 'pending_payment']);
      // Create a payment record that will be updated when paid
      const paymentId = uid();
      const adFee = Number(process.env.AD_FEE_NGN || 1000); // default N1000 fee for posting an ad
      await client.query(`INSERT INTO payments (id, ad_id, user_id, provider, amount, currency, status, meta) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
        [paymentId, adId, seller_id, PAYMENT_PROVIDER, adFee, currency || 'NGN', 'initiated', JSON.stringify({ type:'ad_fee' })]);

      // initialize payment at provider and return the URL
      const email = (await client.query('SELECT email FROM users WHERE id=$1 LIMIT 1', [seller_id])).rows?.[0]?.email || `seller_${seller_id}@example.com`;
      const metadata = { adId, paymentId, type:'ad_fee', email };
      const callback = (process.env.CALLBACK_BASE_URL || '') + '/pay/ad-callback';
      const initResp = await initializePayment({ provider: PAYMENT_PROVIDER, email, amount: adFee, metadata, currency: currency || 'NGN', callback_url: callback });

      // store provider reference if present
      if (initResp && initResp.data && initResp.data.reference) {
        await client.query('UPDATE payments SET reference=$1, meta = meta || $2 WHERE id=$3', [initResp.data.reference, JSON.stringify({ provider_init: initResp.data }), paymentId]);
      } else if (initResp && initResp.session) {
        await client.query('UPDATE payments SET reference=$1, meta = meta || $2 WHERE id=$3', [initResp.session.id, JSON.stringify({ provider_init: initResp.session }), paymentId]);
      }

      return res.json({ success:true, adId, paymentId, init: initResp });
    } finally {
      client.release();
    }
  } catch (err) {
    console.error('/api/ads error', err);
    return res.status(500).json({ success:false, message:'Server error' });
  }
});

app.get('/pay/ad-callback', async (req, res) => {
  const reference = req.query.reference || req.query.trxref;

  if (!reference) {
    return res.send('No payment reference received');
  }

  res.send(`
    <h1>Payment Received</h1>
    <p>Reference: ${reference}</p>
  `);
});

  } catch (err) {
    console.error('Callback error:', err);
    res.status(500).send('Something went wrong');
  }
});

// GET /api/ads - list ads (live only by default)
app.get('/api/ads', async (req, res) => {
  try {
    const status = req.query.status || 'live';
    const client = await pool.connect();
    try {
      const r = await client.query('SELECT * FROM ads WHERE status=$1 ORDER BY created_at DESC LIMIT 200', [status]);
      return res.json({ success:true, ads: r.rows });
    } finally { client.release(); }
  } catch (e) { console.error('GET /api/ads error', e); return res.status(500).json({ success:false }); }
});

// GET /api/ads/:id
app.get('/api/ads/:id', async (req, res) => {
  try {
    const id = req.params.id;
    const client = await pool.connect();
    try {
      const r = await client.query('SELECT * FROM ads WHERE id=$1 LIMIT 1', [id]);
      if (!r.rows.length) return res.status(404).json({ success:false, message:'not-found' });
      return res.json({ success:true, ad: r.rows[0] });
    } finally { client.release(); }
  } catch (e) { console.error('GET /api/ads/:id', e); return res.status(500).json({ success:false }); }
});

// BUY: POST /api/ads/:id/buy — create order, create payment, initialize payment
app.post('/api/ads/:id/buy', async (req, res) => {
  try {
    const adId = req.params.id;
    const { buyer_id, qty = 1 } = req.body || {};
    if (!buyer_id) return res.status(400).json({ success:false, message:'buyer_id required' });
    const client = await pool.connect();
    try {
      const adR = await client.query('SELECT * FROM ads WHERE id=$1 LIMIT 1', [adId]);
      if (!adR.rows.length) return res.status(404).json({ success:false, message:'ad-not-found' });
      const ad = adR.rows[0];
      const amount = Number(ad.price) * Number(qty);
      const orderId = uid();
      await client.query('INSERT INTO orders (id, ad_id, buyer_id, seller_id, qty, amount, currency, status) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)', [orderId, adId, buyer_id, ad.seller_id, qty, amount, ad.currency || 'NGN', 'pending_payment']);
      const paymentId = uid();
      await client.query('INSERT INTO payments (id, order_id, ad_id, user_id, provider, amount, currency, status, meta) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)', [paymentId, orderId, adId, buyer_id, PAYMENT_PROVIDER, amount, ad.currency || 'NGN', 'initiated', JSON.stringify({ type:'order' })]);
      const email = (await client.query('SELECT email FROM users WHERE id=$1 LIMIT 1', [buyer_id])).rows?.[0]?.email || `buyer_${buyer_id}@example.com`;
      const metadata = { orderId, paymentId, adId, type:'order', email };
      const callback = (process.env.CALLBACK_BASE_URL || '') + '/pay/order-callback';
      const initResp = await initializePayment({ provider: PAYMENT_PROVIDER, email, amount, metadata, currency: ad.currency || 'NGN', callback_url: callback });
      // store provider reference if present
      if (initResp && initResp.data && initResp.data.reference) {
        await client.query('UPDATE payments SET reference=$1, meta = meta || $2 WHERE id=$3', [initResp.data.reference, JSON.stringify({ provider_init: initResp.data }), paymentId]);
      } else if (initResp && initResp.session) {
        await client.query('UPDATE payments SET reference=$1, meta = meta || $2 WHERE id=$3', [initResp.session.id, JSON.stringify({ provider_init: initResp.session }), paymentId]);
      }
      return res.json({ success:true, orderId, paymentId, init: initResp });
    } finally { client.release(); }
  } catch (e) {
    console.error('POST /api/ads/:id/buy error', e);
    return res.status(500).json({ success:false, message:'Server error' });
  }
});


// Seller endpoint to release goods for an order (seller triggers release after buyer paid)
// POST /api/orders/:id/release
app.post('/api/orders/:id/release', async (req, res) => {
  try {
    const orderId = req.params.id;
    const client = await pool.connect();
    try {
      const or = await client.query('SELECT * FROM orders WHERE id=$1 LIMIT 1', [orderId]);
      if (!or.rows.length) return res.status(404).json({ success:false, message:'order-not-found' });
      const order = or.rows[0];
      // check current status
      if (order.status !== 'paid') return res.status(400).json({ success:false, message:'order-not-paid' });
      await client.query('UPDATE orders SET status=$1, updated_at=now() WHERE id=$2', ['released', orderId]);
      // enqueue payout job (option: payout only after buyer confirms; but you asked for auto-call)
      // We'll enqueue payout now (or you can change logic to wait for buyer confirmation)
      if (jobQueue) {
        await jobQueue.add('payout-seller', { orderId }, { attempts: 3 });
      }
      // send notifications
      if (jobQueue) {
        await jobQueue.add('notify-buyer', { to: order.buyer_id, subject: 'Seller released order', message: `Seller released order ${orderId}. Please confirm receipt.` });
        await jobQueue.add('notify-seller', { to: order.seller_id, subject:'Order released', message: `You released order ${orderId}. Payout will be processed shortly.` });
      }
      return res.json({ success:true, message:'order released, payout queued' });
    } finally { client.release(); }
  } catch (e) { console.error('/api/orders/:id/release', e); return res.status(500).json({ success:false }); }
});

// Buyer confirms received: mark completed and if payout still pending enqueue payout
app.post('/api/orders/:id/confirm', async (req, res) => {
  try {
    const orderId = req.params.id;
    const client = await pool.connect();
    try {
      const or = await client.query('SELECT * FROM orders WHERE id=$1 LIMIT 1', [orderId]);
      if (!or.rows.length) return res.status(404).json({ success:false, message:'order-not-found' });
      const order = or.rows[0];
      if (order.status !== 'released') return res.status(400).json({ success:false, message:'order-not-released' });
      await client.query('UPDATE orders SET status=$1, updated_at=now() WHERE id=$2', ['completed', orderId]);
      // enqueue payout
      if (jobQueue) {
        await jobQueue.add('payout-seller', { orderId }, { attempts: 3 });
      }
      // enqueue notifications
      if (jobQueue) {
        await jobQueue.add('notify-seller', { to: order.seller_id, subject:'Buyer confirmed', message:`Buyer confirmed receipt of order ${orderId}. Payout queued.` });
      }
      return res.json({ success:true, message:'Order completed and payout queued' });
    } finally { client.release(); }
  } catch (e) { console.error('/api/orders/:id/confirm', e); return res.status(500).json({ success:false }); }
});

/////////////////////////////////////////////////////////////////////
// Webhook endpoints: Paystack / Flutterwave / Stripe
// NOTE: Stripe requires raw body; for that route we use express.raw middleware
/////////////////////////////////////////////////////////////////////

// Paystack webhook (POST /webhook/paystack)
app.post('/webhook/paystack', express.json({ limit:'1mb' }), verifyPaystackWebhook, async (req, res) => {
  // Paystack sends an event object in req.body; typical event: "charge.success"
  try {
    const body = req.body;
    const event = body.event || body.eventType || null;
    console.log('Paystack webhook received event:', event);
    // extract reference (some providers pass as body.data.reference)
    const data = body.data || {};
    const reference = data.reference || data.transaction || data.id || null;

    // if this was a charge success for an ad or order payment
    // We stored provider reference in payments.reference earlier during initialize
    // So find matching payment
    const client = await pool.connect();
    try {
      const pq = await client.query('SELECT * FROM payments WHERE reference=$1 LIMIT 1', [reference]);
      if (!pq.rows.length) {
        // try to find metadata (in some flows metadata contains adId/orderId)
        console.log('Paystack webhook: payment not found by reference', reference);
      } else {
        const payment = pq.rows[0];
        if (data.status === 'success' || (payment && payment.status !== 'success' && (event && event.toLowerCase().includes('charge')))) {
          // mark payment success
          await client.query('UPDATE payments SET status=$1, meta = meta || $2 WHERE id=$3', ['success', JSON.stringify({ provider_data: data }), payment.id]);

          // if this payment was for an ad -> make ad live
          if (payment.ad_id) {
            await client.query('UPDATE ads SET status=$1 WHERE id=$2', ['live', payment.ad_id]);
            // notify seller
            if (jobQueue) await jobQueue.add('notify-seller', { to: payment.user_id, subject:'Ad is live', message: `Your ad ${payment.ad_id} is now live.` });
          }
          // if this payment was for an order -> mark order paid
          if (payment.order_id) {
            await client.query('UPDATE orders SET status=$1, updated_at=now() WHERE id=$2', ['paid', payment.order_id]);
            // notify seller & buyer
            if (jobQueue) {
              await jobQueue.add('notify-seller', { to: (await client.query('SELECT seller_id FROM orders WHERE id=$1',[payment.order_id])).rows[0].seller_id, subject:'Order paid', message:`Order ${payment.order_id} has been paid. Please deliver.` });
              await jobQueue.add('notify-buyer', { to: (await client.query('SELECT buyer_id FROM orders WHERE id=$1',[payment.order_id])).rows[0].buyer_id, subject:'Payment received', message:`We received payment for order ${payment.order_id}.` });
            }
          }
        }
      }
    } finally { client.release(); }
    res.status(200).send('ok');
  } catch (err) {
    console.error('Paystack webhook error', err);
    res.status(500).send('error');
  }
});

// Flutterwave webhook
app.post('/webhook/flutterwave', express.json({ limit:'1mb' }), verifyFlutterwaveWebhook, async (req, res) => {
  try {
    const b = req.body;
    // Flutterwave shape: contains 'event' and 'data'
    console.log('Flutterwave webhook:', b.event);
    const data = b.data || {};
    // Determine reference (tx_ref or flw_ref)
    const ref = data.tx_ref || data.flw_ref || data.reference;
    // find payments by metadata or reference
    const client = await pool.connect();
    try {
      let p = null;
      if (ref) {
        const pq = await client.query('SELECT * FROM payments WHERE reference=$1 LIMIT 1', [ref]);
        if (pq.rows.length) p = pq.rows[0];
      }
      if (!p && data.meta && data.meta.orderId) {
        const pq2 = await client.query('SELECT * FROM payments WHERE meta->>\'orderId\' = $1 LIMIT 1', [String(data.meta.orderId)]);
        if (pq2.rows.length) p = pq2.rows[0];
      }
      if (p && (data.status === 'successful' || data.status === 'success')) {
        await client.query('UPDATE payments SET status=$1, meta = meta || $2 WHERE id=$3', ['success', JSON.stringify({ provider_data: data }), p.id]);
        if (p.ad_id) {
          await client.query('UPDATE ads SET status=$1 WHERE id=$2', ['live', p.ad_id]);
          if (jobQueue) await jobQueue.add('notify-seller', { to: p.user_id, subject:'Ad live', message:`Your ad ${p.ad_id} is live.` });
        }
        if (p.order_id) {
          await client.query('UPDATE orders SET status=$1, updated_at=now() WHERE id=$2', ['paid', p.order_id]);
          if (jobQueue) {
            await jobQueue.add('notify-seller', { to: (await client.query('SELECT seller_id FROM orders WHERE id=$1',[p.order_id])).rows[0].seller_id, subject:'Order paid', message:`Order ${p.order_id} has been paid.` });
            await jobQueue.add('notify-buyer', { to: (await client.query('SELECT buyer_id FROM orders WHERE id=$1',[p.order_id])).rows[0].buyer_id, subject:'Payment received', message:`Payment for order ${p.order_id} received.` });
          }
        }
      } else {
        console.log('Flutterwave webhook: unknown or unsuccessful', data.status, data);
      }
    } finally { client.release(); }
    res.status(200).send('ok');
  } catch (err) {
    console.error('Flutterwave webhook error', err);
    res.status(500).send('error');
  }
});

// Stripe webhook: requires raw body (see Stripe docs). This route uses express.raw middleware.
app.post('/webhook/stripe', express.raw({ type: 'application/json' }), async (req, res) => {
  const endpointSecret = process.env.STRIPE_WEBHOOK_SECRET;
  if (!stripeLib || !endpointSecret) {
    console.warn('Stripe not configured properly for webhooks.');
    return res.status(400).send('stripe not configured');
  }
  const sig = req.headers['stripe-signature'];
  let event;
  try {
    event = stripeLib.webhooks.constructEvent(req.body, sig, endpointSecret);
  } catch (err) {
    console.error('Stripe webhook signature verification failed.', err.message);
    return res.status(400).send(`Webhook Error: ${err.message}`);
  }

  // Handle the event
  try {
    const type = event.type;
    console.log('Stripe webhook event type', type);
    if (type === 'checkout.session.completed' || type === 'payment_intent.succeeded') {
      const session = event.data.object;
      // session.client_reference_id or metadata should contain payment/order info
      const metadata = session.metadata || {};
      const reference = session.id;
      const client = await pool.connect();
      try {
        // try to find payment by metadata.paymentId or reference
        let payment = null;
        if (metadata.paymentId) {
          const pq = await client.query('SELECT * FROM payments WHERE id=$1 LIMIT 1', [metadata.paymentId]);
          if (pq.rows.length) payment = pq.rows[0];
        }
        if (!payment && metadata.orderId) {
          const pq2 = await client.query('SELECT * FROM payments WHERE order_id=$1 LIMIT 1', [metadata.orderId]);
          if (pq2.rows.length) payment = pq2.rows[0];
        }
        if (!payment) {
          const pq3 = await client.query('SELECT * FROM payments WHERE reference=$1 LIMIT 1', [reference]);
          if (pq3.rows.length) payment = pq3.rows[0];
        }
        if (payment) {
          await client.query('UPDATE payments SET status=$1, meta = meta || $2 WHERE id=$3', ['success', JSON.stringify({ provider_data: session }), payment.id]);
          if (payment.ad_id) {
            await client.query('UPDATE ads SET status=$1 WHERE id=$2', ['live', payment.ad_id]);
            if (jobQueue) await jobQueue.add('notify-seller', { to: payment.user_id, subject:'Ad live', message:`Your ad ${payment.ad_id} is live.` });
          }
          if (payment.order_id) {
            await client.query('UPDATE orders SET status=$1, updated_at=now() WHERE id=$2', ['paid', payment.order_id]);
            if (jobQueue) {
              await jobQueue.add('notify-seller', { to: (await client.query('SELECT seller_id FROM orders WHERE id=$1',[payment.order_id])).rows[0].seller_id, subject:'Order paid', message:`Order ${payment.order_id} has been paid.` });
              await jobQueue.add('notify-buyer', { to: (await client.query('SELECT buyer_id FROM orders WHERE id=$1',[payment.order_id])).rows[0].buyer_id, subject:'Payment received', message:`Payment for order ${payment.order_id} received.` });
            }
          }
        } else {
          console.log('Stripe webhook: no matching payment found for session', session.id);
        }
      } finally { client.release(); }
    }
    res.json({ received: true });
  } catch (err) {
    console.error('Stripe webhook processing error', err);
    res.status(500).send('error');
  }
});

/////////////////////////////////////////////////////////////////////
// Small helpful endpoints: admin init / health
/////////////////////////////////////////////////////////////////////
app.get('/health', (req,res) => res.json({ ok:true, now: new Date().toISOString() }));

/////////////////////////////////////////////////////////////////////
// Attach Sentry error handler (if enabled)
/////////////////////////////////////////////////////////////////////
if (process.env.SENTRY_DSN) {
  app.use(Sentry.Handlers.errorHandler());
}

/////////////////////////////////////////////////////////////////////
// Start server & graceful shutdown for Bull & Redis
/////////////////////////////////////////////////////////////////////
const server = app.listen(PORT, ()=> console.log(`TradeHive backend listening on port ${PORT}`));

async function shutdown() {
  console.log('Shutting down...');
  try {
    if (jobWorker) {
      await jobWorker.close();
      console.log('Bull worker closed.');
    }
    if (jobQueue) {
      await jobQueue.close();
      console.log('Bull queue closed.');
    }
    if (redis) {
      redis.disconnect();
      console.log('Redis disconnected.');
    }
    server.close(() => {
      console.log('HTTP server closed.');
      process.exit(0);
    });
  } catch (err) {
    console.error('Shutdown error', err);
    process.exit(1);
  }
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);