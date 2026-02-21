// server.js
// TradeHive backend — merged + fixed version
require('dotenv').config();

/////////////////////////////////////////////////////////////////////
// Core libs
/////////////////////////////////////////////////////////////////////
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const promClient = require('prom-client');
const { Pool } = require('pg');
const IORedis = require('ioredis');
const { Queue, Worker } = require('bullmq');
const fetch = global.fetch || require('node-fetch');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const bcrypt = require('bcryptjs');
const multer = require('multer');

let stripeLib = null;
if (process.env.STRIPE_SECRET_KEY) {
  try { stripeLib = require('stripe')(process.env.STRIPE_SECRET_KEY); } catch(e){ stripeLib = null; }
}

// Optional email libs: SendGrid or Resend. We'll support SendGrid if present; otherwise stub.
// If you prefer Resend, set RESEND_API_KEY and modify sendEmail accordingly.
let sendgrid = null;
try {
  if (process.env.SENDGRID_API_KEY) {
    sendgrid = require('@sendgrid/mail');
    sendgrid.setApiKey(process.env.SENDGRID_API_KEY);
    console.log('SendGrid configured.');
  }
} catch (e) {
  console.warn('SendGrid lib not configured or missing, falling back to email stub.');
  sendgrid = null;
}

/////////////////////////////////////////////////////////////////////
// Cloudinary + multer upload setup (KYC = authenticated, Ads = public)
/////////////////////////////////////////////////////////////////////
let cloudinary = null;
let CloudinaryStorage = null;
let uploadCloud = null;
let uploadAds = null;
let diskAdsUpload = null;

const TMP_ADS_DIR = path.join(process.cwd(), 'uploads', 'tmp_ads');
try { fs.mkdirSync(TMP_ADS_DIR, { recursive: true }); } catch(e){}

function cloudinaryPublicUrl(public_id, opts = {}) {
  if (!cloudinary || !public_id) return null;
  try { return cloudinary.url(public_id, { secure: true, type: 'upload', resource_type: opts.resource_type || 'image', ...opts }); }
  catch (e) { console.error('cloudinaryPublicUrl error', e && e.message ? e.message : e); return null; }
}

function cloudinarySignedUrl(public_id, opts = {}) {
  if (!cloudinary || !public_id) return null;
  try { return cloudinary.url(public_id, { sign_url: true, secure: true, type: opts.type || 'authenticated', resource_type: opts.resource_type || 'image', ...opts }); }
  catch (e) { console.error('cloudinarySignedUrl error', e && e.message ? e.message : e); return null; }
}

try {
  if (process.env.CLOUDINARY_CLOUD_NAME && process.env.CLOUDINARY_API_KEY && process.env.CLOUDINARY_API_SECRET) {
    cloudinary = require('cloudinary').v2;
    CloudinaryStorage = require('multer-storage-cloudinary').CloudinaryStorage;
    cloudinary.config({ cloud_name: process.env.CLOUDINARY_CLOUD_NAME, api_key: process.env.CLOUDINARY_API_KEY, api_secret: process.env.CLOUDINARY_API_SECRET, secure: true });

    const kycStorage = new CloudinaryStorage({
      cloudinary,
      params: async (req, file) => {
        const isVideo = file.mimetype && file.mimetype.startsWith && file.mimetype.startsWith('video/');
        return {
          folder: isVideo ? 'tradehive/kyc/videos' : 'tradehive/kyc/images',
          resource_type: isVideo ? 'video' : 'image',
          type: 'authenticated',
          public_id: `kyc-${Date.now()}-${file.originalname.replace(/\s+/g, '_')}`,
          overwrite: false
        };
      }
    });

    const adsStorage = new CloudinaryStorage({
      cloudinary,
      params: async (req, file) => {
        const isVideo = file.mimetype && file.mimetype.startsWith && file.mimetype.startsWith('video/');
        return {
          folder: isVideo ? 'tradehive/ads/videos' : 'tradehive/ads/images',
          resource_type: isVideo ? 'video' : 'image',
          type: 'upload',
          public_id: `ad-${Date.now()}-${file.originalname.replace(/\s+/g, '_')}`,
          overwrite: false
        };
      }
    });

    uploadCloud = multer({ storage: kycStorage, limits: { fileSize: 200 * 1024 * 1024 } });
    uploadAds = multer({ storage: adsStorage, limits: { fileSize: 100 * 1024 * 1024 } });

    console.log('Cloudinary configured: kyc=authenticated, ads=public');
  } else {
    console.log('Cloudinary credentials missing — using disk fallback for uploads.');
  }
} catch (err) {
  console.error('Cloudinary setup error', err && err.message ? err.message : err);
  uploadCloud = null;
  uploadAds = null;
}

// Disk fallback for ads
if (!uploadAds) {
  try {
    const uploadsDir = path.resolve(process.cwd(), 'uploads', 'ads');
    fs.mkdirSync(uploadsDir, { recursive: true });
    const diskStorageAds = multer.diskStorage({
      destination: (req, file, cb) => cb(null, uploadsDir),
      filename: (req, file, cb) => {
        const safe = file.originalname.replace(/\s+/g,'_').replace(/[^a-zA-Z0-9_\-\.]/g, '');
        cb(null, `${Date.now()}-${safe}`);
      }
    });
    diskAdsUpload = multer({ storage: diskStorageAds, limits: { fileSize: 100 * 1024 * 1024 } });
    console.log('Disk fallback uploader ready for ads ->', uploadsDir);
  } catch (err) {
    console.error('Disk fallback setup error (ads)', err && err.message ? err.message : err);
    diskAdsUpload = null;
  }
}

// Disk fallback for KYC (optional)
if (!uploadCloud) {
  try {
    const kycDir = path.resolve(process.cwd(), 'uploads', 'kyc');
    fs.mkdirSync(kycDir, { recursive: true });
    const diskStorageKyc = multer.diskStorage({
      destination: (req, file, cb) => cb(null, kycDir),
      filename: (req, file, cb) => {
        const safe = file.originalname.replace(/\s+/g, '_').replace(/[^a-zA-Z0-9_\-\.]/g, '');
        cb(null, `kyc-${Date.now()}-${safe}`);
      }
    });
    uploadCloud = multer({ storage: diskStorageKyc, limits: { fileSize: 200 * 1024 * 1024 } });
    console.log('Disk fallback uploader ready for KYC ->', kycDir);
  } catch (err) {
    console.error('Disk fallback (KYC) setup error', err && err.message ? err.message : err);
    uploadCloud = null;
  }
}

/////////////////////////////////////////////////////////////////////
// Express setup
/////////////////////////////////////////////////////////////////////
const app = express();
const PORT = process.env.PORT || 3000;

if (process.env.SENTRY_DSN) {
  try {
    const Sentry = require('@sentry/node');
    Sentry.init({ dsn: process.env.SENTRY_DSN, tracesSampleRate: Number(process.env.SENTRY_TRACES_SAMPLE_RATE || 0.2) });
    app.use(Sentry.Handlers.requestHandler());
    app.use(Sentry.Handlers.tracingHandler());
    console.log('Sentry enabled.');
  } catch(e){ console.warn('Sentry lib missing or init failed'); }
}

app.use(helmet());
app.use(cors({ origin: true, credentials: true }));
app.use(express.json({ limit: '18mb' }));
const limiter = rateLimit({ windowMs: 15*60*1000, max: Number(process.env.RATE_LIMIT_MAX || 300) });
app.use(limiter);
promClient.collectDefaultMetrics({ timeout: 5000 });
const httpRequestCounter = new promClient.Counter({ name: 'tradehive_http_requests_total', help: 'Total number of HTTP requests', labelNames: ['method','route','status_code'] });
app.use((req,res,next) => {
  res.on('finish', ()=> {
    const route = req.route && req.route.path ? req.route.path : req.path || req.originalUrl || 'unknown';
    httpRequestCounter.inc({ method: req.method, route, status_code: res.statusCode }, 1);
  });
  next();
});
app.get('/metrics', async (req,res) => { res.set('Content-Type', promClient.register.contentType); res.end(await promClient.register.metrics()); });

/////////////////////////////////////////////////////////////////////
// Postgres & init
/////////////////////////////////////////////////////////////////////
const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });

const initSql = `
CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, role TEXT NOT NULL, email TEXT NOT NULL UNIQUE, phone TEXT NOT NULL UNIQUE, fullname TEXT NOT NULL, username TEXT NOT NULL UNIQUE, state TEXT, lga TEXT, city TEXT, gender TEXT, specializations TEXT[], password_hash TEXT, kyc_status TEXT DEFAULT 'Unverified', avatar_url TEXT, profile_complete boolean DEFAULT false, account_details JSONB, online boolean DEFAULT false, created_at TIMESTAMP WITH TIME ZONE DEFAULT now());
CREATE TABLE IF NOT EXISTS ads (id TEXT PRIMARY KEY, seller_id TEXT NOT NULL REFERENCES users(id), title TEXT NOT NULL, description TEXT, images TEXT[], price NUMERIC NOT NULL, currency TEXT DEFAULT 'NGN', quantity INTEGER DEFAULT 1, location TEXT, category TEXT, subcategory TEXT, status TEXT DEFAULT 'draft', kyc_required boolean DEFAULT false, created_at TIMESTAMP WITH TIME ZONE DEFAULT now(), updated_at TIMESTAMP WITH TIME ZONE DEFAULT now());
CREATE TABLE IF NOT EXISTS orders (id TEXT PRIMARY KEY, ad_id TEXT NOT NULL REFERENCES ads(id), buyer_id TEXT NOT NULL REFERENCES users(id), seller_id TEXT NOT NULL REFERENCES users(id), qty INTEGER DEFAULT 1, amount NUMERIC NOT NULL, currency TEXT DEFAULT 'NGN', status TEXT DEFAULT 'pending_payment', created_at TIMESTAMP WITH TIME ZONE DEFAULT now(), updated_at TIMESTAMP WITH TIME ZONE DEFAULT now(), released_at TIMESTAMP WITH TIME ZONE, buyer_confirmed_at TIMESTAMP WITH TIME ZONE, release_followup_stage INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS payments (id TEXT PRIMARY KEY, order_id TEXT REFERENCES orders(id), ad_id TEXT REFERENCES ads(id), user_id TEXT, provider TEXT, reference TEXT, amount NUMERIC, currency TEXT, status TEXT DEFAULT 'initiated', meta JSONB, created_at TIMESTAMP WITH TIME ZONE DEFAULT now(), payout_status TEXT DEFAULT 'none', payout_reference TEXT, payout_response JSONB);
CREATE TABLE IF NOT EXISTS kyc_requests (id TEXT PRIMARY KEY, user_id TEXT REFERENCES users(id), id_type TEXT, id_name TEXT, id_number TEXT, id_images TEXT[], work_videos TEXT[], selfie TEXT, status TEXT DEFAULT 'pending', admin_note TEXT, submitted_at TIMESTAMP WITH TIME ZONE DEFAULT now());
CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, name TEXT, data JSONB, state TEXT, created_at TIMESTAMP WITH TIME ZONE DEFAULT now());
`;

(async ()=>{
  try { await pool.query(initSql); console.log('DB initialized'); } catch(e){ console.error('DB init error', e); process.exit(1); }
})();

/////////////////////////////////////////////////////////////////////
// Redis / Bull queue (optional)
/////////////////////////////////////////////////////////////////////
let redis = null, jobQueue = null, jobWorker = null;
if (process.env.REDIS_URL) {
  try {
    redis = new IORedis(process.env.REDIS_URL, { maxRetriesPerRequest: 1, enableReadyCheck: true });
    jobQueue = new Queue('tradehive-jobs', { connection: redis, defaultJobOptions: { removeOnComplete: true, attempts: 3 } });
    console.log('Redis/Bull configured.');
  } catch(e){ console.error('Redis init failed', e); redis = null; jobQueue = null; }
} else {
  console.log('REDIS_URL not set: jobQueue disabled (in-process fallback used for scheduling).');
}

/////////////////////////////////////////////////////////////////////
// Disk fallback storage for KYC (already created above) & a generic uploadDisk
/////////////////////////////////////////////////////////////////////
const UPLOAD_DIR = path.join(__dirname, 'uploads', 'kyc');
try { fs.mkdirSync(UPLOAD_DIR, { recursive: true }); } catch(e){ }
const diskStorage = multer.diskStorage({
  destination: (req,file,cb)=> cb(null, UPLOAD_DIR),
  filename: (req,file,cb)=> cb(null, Date.now() + '-' + file.originalname.replace(/\s+/g,'_'))
});
const uploadDisk = multer({ storage: diskStorage, limits: { fileSize: 200 * 1024 * 1024 } });

/////////////////////////////////////////////////////////////////////
// Helpers
/////////////////////////////////////////////////////////////////////
function uid(){ return Math.floor(1000000000 + Math.random()*9000000000).toString(); }
function nowISO(){ return new Date().toISOString(); }
function safeJsonParse(s){ try { return JSON.parse(s); } catch(e) { return null; } }
function adminAuth(req,res,next){
  const key = process.env.ADMIN_API_KEY || null;
  if (!key) return res.status(403).json({ success:false, message:'admin auth not configured' });
  const got = req.headers['x-admin-key'] || req.query.admin_key || null;
  if (got !== key) return res.status(403).json({ success:false, message:'forbidden' });
  next();
}
function toLowerTrim(s){ return s ? String(s).trim().toLowerCase() : ''; }
function validEmail(e){ return typeof e === 'string' && e.includes('@'); }
function validPhone(p){ return typeof p === 'string' && p.replace(/\D/g,'').length >= 9; }

/////////////////////////////////////////////////////////////////////
// Send email helper (SendGrid or stub) — safe: errors do not stop flow
/////////////////////////////////////////////////////////////////////
async function sendEmail({ to, subject, text, html }) {
  if (sendgrid && process.env.SENDGRID_FROM) {
    try {
      await sendgrid.send({ to, from: process.env.SENDGRID_FROM, subject, text, html });
      console.log('Email sent to', to, subject);
      return { ok:true };
    } catch (e) {
      console.error('SendGrid send error', e && e.message ? e.message : e);
      return { ok:false, error:e };
    }
  } else {
    // No provider configured: stub and continue.
    console.log(`[email stub] to=${to} subject=${subject}\n${text || html}`);
    return { ok:true, stub:true };
  }
}

/////////////////////////////////////////////////////////////////////
// Working-days helper (skip weekend)
/////////////////////////////////////////////////////////////////////
function isWeekend(d) { const day = d.getDay(); return day === 0 || day === 6; }
function addWorkingDays(date, days) {
  let d = (date instanceof Date) ? new Date(date) : new Date(date);
  let added = 0;
  while (added < days) {
    d.setDate(d.getDate() + 1);
    if (!isWeekend(d)) added++;
  }
  return d;
}
function msUntil(date) {
  const now = Date.now();
  const t = (date instanceof Date) ? date.getTime() : new Date(date).getTime();
  return Math.max(0, t - now);
}

/////////////////////////////////////////////////////////////////////
// scheduling + job handler
/////////////////////////////////////////////////////////////////////
async function scheduleJobAt(name, data, targetDate, opts = {}) {
  if (!targetDate) targetDate = new Date();
  const delay = msUntil(targetDate);
  if (jobQueue) {
    return await jobQueue.add(name, data, { delay, attempts: opts.attempts || 3, removeOnComplete: true });
  } else {
    console.warn('Using in-process scheduling (no Redis). This is ephemeral and will not survive restart.');
    setTimeout(async () => {
      try { await jobHandler(name, data); } catch(e){ console.error('in-process job handler error', e); }
    }, delay);
    return { ok:true, fallback:true, scheduledAt: targetDate };
  }
}

async function jobHandler(name, data) {
  console.log('Job handler running', name, data);
  if (name === 'notify') {
    const { to, subject, message } = data;
    await sendEmail({ to, subject, text: message, html: `<p>${message}</p>` });
    return { ok:true };
  }
  if (name === 'payout-seller') {
    const { orderId } = data;
    const client = await pool.connect();
    try {
      const order = (await client.query('SELECT * FROM orders WHERE id=$1 LIMIT 1', [orderId])).rows[0];
      if (!order) return { ok:false, message:'order-not-found' };
      const payment = (await client.query('SELECT * FROM payments WHERE order_id=$1 ORDER BY created_at DESC LIMIT 1', [orderId])).rows[0];
      if (!payment) return { ok:false, message:'payment-not-found' };
      const seller = (await client.query('SELECT * FROM users WHERE id=$1 LIMIT 1', [order.seller_id])).rows[0];
      if (!seller) return { ok:false, message:'seller-not-found' };
      const payoutRes = await payoutSeller({ seller, amount: order.amount, currency: order.currency || 'NGN', metadata: { orderId } });
      if (payoutRes && payoutRes.ok) {
        await client.query('UPDATE payments SET payout_status=$1, payout_reference=$2, payout_response=$3 WHERE order_id=$4', ['paid', payoutRes.reference || null, JSON.stringify(payoutRes.raw || {}), orderId]);
        await sendEmail({ to: seller.email || seller.username, subject: `Payout processed for order ${orderId}`, text: `Payout processed. Reference: ${payoutRes.reference}` }).catch(()=>{});
        return { ok:true };
      } else {
        await client.query('UPDATE payments SET payout_status=$1, payout_response=$2 WHERE order_id=$3', ['failed', JSON.stringify(payoutRes || {}), orderId]);
        return { ok:false, message:'payout-failed', detail:payoutRes };
      }
    } finally { client.release(); }
  }
  if (name === 'order-followup') {
    const { orderId, stage } = data;
    const client = await pool.connect();
    try {
      const order = (await client.query('SELECT * FROM orders WHERE id=$1 LIMIT 1', [orderId])).rows[0];
      if (!order) return { ok:false, message:'order-not-found' };
      if (order.status !== 'released') return { ok:true, message:'no-action' };
      if (stage === 1 || stage === 2) {
        await sendEmail({ to: order.buyer_id, subject: `Reminder: confirm receipt for ${orderId}`, text: `Please confirm receipt for order ${orderId}.` }).catch(()=>{});
        await sendEmail({ to: order.seller_id, subject: `Reminder sent to buyer for ${orderId}`, text: `A reminder was sent to the buyer.` }).catch(()=>{});
        await client.query('UPDATE orders SET release_followup_stage = $1 WHERE id=$2', [stage, orderId]);
        return { ok:true };
      }
      if (stage === 3) {
        await sendEmail({ to: order.buyer_id, subject: `Final reminder: confirm receipt for ${orderId}`, text: `Final reminder before auto-release.` }).catch(()=>{});
        await client.query('UPDATE orders SET release_followup_stage = $1 WHERE id=$2', [stage, orderId]);
        return { ok:true };
      }
      if (stage === 4) {
        await client.query('UPDATE orders SET status=$1, updated_at=now() WHERE id=$2', ['completed', orderId]);
        await scheduleJobAt('payout-seller', { orderId }, new Date());
        await sendEmail({ to: order.seller_id, subject: `Auto-release: funds released for ${orderId}`, text: `Funds are being released (auto-resolve).` }).catch(()=>{});
        await sendEmail({ to: order.buyer_id, subject: `Order auto-resolved for ${orderId}`, text: `The order was auto-resolved and funds released to seller.` }).catch(()=>{});
        return { ok:true };
      }
      return { ok:true };
    } finally { client.release(); }
  }
  return { ok:true };
}

if (jobQueue) {
  jobWorker = new Worker('tradehive-jobs', async job => await jobHandler(job.name, job.data || job.data), { connection: redis });
  jobWorker.on('completed', job => console.log('Job completed', job.name, job.id));
  jobWorker.on('failed', (job, err) => console.error('Job failed', job.name, err && err.message));
}

/////////////////////////////////////////////////////////////////////
// Payment helpers
/////////////////////////////////////////////////////////////////////
const PAYMENT_PROVIDER = (process.env.PAYMENT_PROVIDER || 'paystack').toLowerCase();

/**
 * sanitizeMetadata - returns flat primitive-only metadata to avoid provider errors
 */
function sanitizeMetadata(obj = {}) {
  const out = {};
  if (!obj || typeof obj !== 'object') return out;
  const maxLen = 500;
  for (const k of Object.keys(obj)) {
    try {
      const v = obj[k];
      if (v === null || typeof v === 'undefined') continue;
      const t = typeof v;
      if (t === 'string' || t === 'number' || t === 'boolean') {
        const sval = String(v);
        out[String(k)] = sval.length > maxLen ? sval.slice(0, maxLen) : sval;
      } else {
        continue;
      }
    } catch (e) { continue; }
  }
  return out;
}

async function initializePayment({ provider = PAYMENT_PROVIDER, email, amount, currency = 'NGN', metadata = {}, callback_url }) {
  provider = (provider || PAYMENT_PROVIDER || '').toString().toLowerCase();
  const safeMetadata = sanitizeMetadata(metadata || {});
  if (metadata && metadata.tx_ref && !safeMetadata.tx_ref) safeMetadata.tx_ref = String(metadata.tx_ref);
  if (email && !safeMetadata.email) safeMetadata.email = String(email);

  if (provider === 'paystack') {
    const key = process.env.PAYSTACK_SECRET_KEY;
    if (!key) throw new Error('PAYSTACK_SECRET_KEY not set');
    const body = { email, amount: Math.round(Number(amount) * 100), callback_url: callback_url || (process.env.CALLBACK_BASE_URL || '') + '/pay/callback', metadata: safeMetadata };
    const res = await fetch('https://api.paystack.co/transaction/initialize', { method:'POST', headers:{ Authorization:`Bearer ${key}`, 'Content-Type':'application/json' }, body: JSON.stringify(body) });
    return await res.json();
  }

  if (provider === 'flutterwave') {
    const key = process.env.FLUTTERWAVE_SECRET_KEY;
    if (!key) throw new Error('FLUTTERWAVE_SECRET_KEY not set');
    const tx_ref = safeMetadata.tx_ref || metadata.tx_ref || ('th_' + uid());
    const requestBody = { tx_ref, amount: String(amount), currency, redirect_url: callback_url || (process.env.CALLBACK_BASE_URL || '') + '/pay/callback', customer: { email: safeMetadata.email || email || '', phonenumber: safeMetadata.phone || '' }, meta: safeMetadata };
    const res = await fetch('https://api.flutterwave.com/v3/payments', { method:'POST', headers:{ Authorization:`Bearer ${key}`, 'Content-Type':'application/json' }, body: JSON.stringify(requestBody) });
    return await res.json();
  }

  if (provider === 'stripe') {
    if (!stripeLib) throw new Error('Stripe not configured');
    const session = await stripeLib.checkout.sessions.create({
      payment_method_types: ['card'],
      mode: 'payment',
      line_items: [{ price_data: { currency, product_data: { name: safeMetadata.title || 'TradeHive' }, unit_amount: Math.round(Number(amount) * 100) }, quantity: 1 }],
      success_url: (process.env.CALLBACK_BASE_URL || '') + '/pay/success?session_id={CHECKOUT_SESSION_ID}',
      cancel_url: (process.env.CALLBACK_BASE_URL || '') + '/pay/cancel',
      metadata: safeMetadata
    });
    return { ok:true, url: session.url, session };
  }

  return { ok:true, url: callback_url || '/', message:'provider-not-configured' };
}

function verifyPaystackSignature(req) {
  const secret = process.env.PAYSTACK_SECRET_KEY;
  if (!secret) return true;
  const sig = req.headers['x-paystack-signature'];
  const payload = JSON.stringify(req.body || {});
  const hash = crypto.createHmac('sha512', secret).update(payload).digest('hex');
  return hash === sig;
}

async function payoutSeller({ seller, amount, currency='NGN', metadata={} }) {
  if (!seller || !seller.account_details) return { ok:false, message:'seller-account-details-missing' };
  const details = seller.account_details;
  const provider = PAYMENT_PROVIDER;
  try {
    if (provider === 'paystack') {
      const key = process.env.PAYSTACK_SECRET_KEY; if (!key) return { ok:false, message:'PAYSTACK_SECRET_KEY not set' };
      let recipient_code = details.recipient_code;
      if (!recipient_code) {
        const createRes = await fetch('https://api.paystack.co/transferrecipient', { method:'POST', headers:{ Authorization:`Bearer ${key}`, 'Content-Type':'application/json' }, body: JSON.stringify({ type:'nuban', name: details.name || seller.fullname || seller.username, account_number: details.account_number, bank_code: details.bank_code, currency }) });
        const cr = await createRes.json();
        if (!cr.status) return { ok:false, message:'failed-create-recipient', raw:cr };
        recipient_code = cr.data.recipient_code;
        try { const c = await pool.connect(); await c.query('UPDATE users SET account_details = jsonb_set(coalesce(account_details, {}::jsonb), \'{recipient_code}\', to_jsonb($1::text), true) WHERE id=$2', [recipient_code, seller.id]); c.release(); } catch(e){ console.warn('persist recipient failed', e); }
      }
      const amount_kobo = Math.round(Number(amount) * 100);
      const transferRes = await fetch('https://api.paystack.co/transfer', { method:'POST', headers: { Authorization:`Bearer ${key}`, 'Content-Type':'application/json' }, body: JSON.stringify({ source:'balance', amount: amount_kobo, recipient: recipient_code, reason: metadata && metadata.orderId ? `Payout for order ${metadata.orderId}` : 'TradeHive payout' }) });
      const tr = await transferRes.json();
      if (!tr.status) return { ok:false, message:'transfer_failed', raw:tr };
      return { ok:true, reference: tr.data.transfer_code || tr.data.reference, raw: tr };
    }

    if (provider === 'flutterwave') {
      const key = process.env.FLUTTERWAVE_SECRET_KEY; if (!key) return { ok:false, message:'FLUTTERWAVE_SECRET_KEY not set' };
      let beneficiary_id = details.beneficiary_id;
      if (!beneficiary_id) {
        const createBenef = await fetch('https://api.flutterwave.com/v3/beneficiaries', { method:'POST', headers:{ Authorization:`Bearer ${key}`, 'Content-Type':'application/json' }, body: JSON.stringify({ account_number: details.account_number, account_bank: details.bank_code, fullname: details.name || seller.fullname || seller.username }) });
        const cb = await createBenef.json();
        if (!cb.status) return { ok:false, message:'failed-create-beneficiary', raw:cb };
        beneficiary_id = cb.data.id;
        try { const c = await pool.connect(); await c.query('UPDATE users SET account_details = jsonb_set(coalesce(account_details, {}::jsonb), \'{beneficiary_id}\', to_jsonb($1::text), true) WHERE id=$2', [String(beneficiary_id), seller.id]); c.release(); } catch(e){ console.warn('persist beneficiary failed', e); }
      }
      const trRes = await fetch('https://api.flutterwave.com/v3/transfers', { method:'POST', headers:{ Authorization:`Bearer ${key}`, 'Content-Type':'application/json' }, body: JSON.stringify({ account_bank: details.bank_code, account_number: details.account_number, amount: String(amount), narration: metadata && metadata.orderId ? `Payout for order ${metadata.orderId}` : 'TradeHive payout', currency, reference: 'th_' + uid() }) });
      const tr = await trRes.json();
      if (!(tr && (tr.status === 'success' || tr.status === true))) return { ok:false, message:'transfer_failed', raw:tr };
      return { ok:true, reference: tr.data && (tr.data.id || tr.data.reference), raw: tr };
    }

    if (provider === 'stripe' && stripeLib) {
      const connectId = details.connect_account_id;
      if (!connectId) return { ok:false, message:'stripe-connect-id-missing' };
      const transferObj = await stripeLib.transfers.create({ amount: Math.round(Number(amount) * 100), currency, destination: connectId, description: 'TradeHive payout' });
      return { ok:true, reference: transferObj.id, raw: transferObj };
    }

    return { ok:true, reference: 'sim-' + uid() };
  } catch (err) {
    console.error('payoutSeller error', err);
    return { ok:false, message:'exception', error: err && err.message, raw: err };
  }
}

/////////////////////////////////////////////////////////////////////
// Helper: fileUrlFromMulterFile
/////////////////////////////////////////////////////////////////////
function fileUrlFromMulterFile(file) {
  if (!file) return null;
  return file.path || file.location || file.secure_url || file.url || (file.filename ? `/uploads/ads/${file.filename}` : null) || null;
}

function mapAdImagesForResponse(imagesArray) {
  if (!imagesArray) return [];
  return imagesArray.map(item => {
    if (!item) return null;
    if (typeof item === 'string') {
      if (item.startsWith('http://') || item.startsWith('https://')) return item;
      if (cloudinary) return cloudinaryPublicUrl(item) || item;
      return item;
    }
    if (typeof item === 'object') {
      return item.url || item.secure_url || item.path || item.location || null;
    }
    return null;
  }).filter(Boolean);
}

/////////////////////////////////////////////////////////////////////
// Upload middleware selection for Ads
/////////////////////////////////////////////////////////////////////
const adsUploadMiddleware = uploadAds ? uploadAds.array('images', 8) : (diskAdsUpload ? diskAdsUpload.array('images', 8) : (req, res, next) => next());
const uploadHandler = uploadCloud || uploadDisk;

//////////////////////////////////////////////////////////
// Routes: KYC submit + status
/////////////////////////////////////////////////////////////////////
app.post('/api/kyc/submit', uploadHandler.fields([{ name:'id_images', maxCount: 6 }, { name:'work_videos', maxCount: 2 }, { name:'selfie', maxCount: 1 }]), async (req, res) => {
  try {
    const body = req.body || {};
    const userId = body.userId || body.user_id;
    if (!userId) return res.status(400).json({ success:false, message:'userId required' });

    const files = req.files || {};
    const idImages = [];
    const workVideos = [];
    let selfiePath = null;

    function fileInfoToUrl(f) {
      if (!f) return null;
      if (f.path && !cloudinary) return f.path;
      if (f.public_id) return { public_id: f.public_id, resource_type: f.resource_type || 'image' };
      if (f.location) return f.location;
      if (f.url) return f.url;
      return f.path || null;
    }

    (files.id_images || []).forEach(f => { const info = fileInfoToUrl(f); if (info) idImages.push(info); });
    (files.work_videos || []).forEach(f => { const info = fileInfoToUrl(f); if (info) workVideos.push(info); });
    if ((files.selfie || [])[0]) selfiePath = fileInfoToUrl((files.selfie || [])[0]);

    const reqId = 'kyc_' + uid();
    const insert = `INSERT INTO kyc_requests (id, user_id, id_type, id_name, id_number, id_images, work_videos, selfie, status, submitted_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,now())`;
    const client = await pool.connect();
    try {
      const serialize = arr => (arr || []).map(i => typeof i === 'object' && i.public_id ? i.public_id : (typeof i === 'string' ? i : null)).filter(Boolean);
      const selfieStored = (typeof selfiePath === 'object' && selfiePath.public_id) ? selfiePath.public_id : (typeof selfiePath === 'string' ? selfiePath : null);

      await client.query(insert, [reqId, userId, body.id_type || null, body.id_name || null, body.id_number || null, serialize(idImages), serialize(workVideos), selfieStored, 'pending']);
      await client.query('UPDATE users SET kyc_status=$1 WHERE id=$2', ['pending', userId]);
      if (process.env.ADMIN_EMAIL) {
        try { await sendEmail({ to: process.env.ADMIN_EMAIL, subject: `KYC submitted by ${userId}`, text: `User ${userId} submitted KYC. Review at admin UI.` }); } catch(e){ console.warn('sendEmail failed', e && e.message); }
      }
      return res.json({ success:true, requestId: reqId });
    } finally { client.release(); }
  } catch (err) {
    console.error('/api/kyc/submit error', err && err.stack ? err.stack : err);
    return res.status(500).json({ success:false, message:'Server error' });
  }
});

app.get('/api/kyc/status/:id', async (req, res) => {
  try {
    const userId = req.params.id;
    const client = await pool.connect();
    try {
      const ures = await client.query('SELECT id, email, fullname, username, kyc_status, account_details, online FROM users WHERE id=$1 LIMIT 1', [userId]);
      if (!ures.rows.length) return res.status(404).json({ success:false, message:'user-not-found' });
      const user = ures.rows[0];
      const kres = await client.query('SELECT * FROM kyc_requests WHERE user_id=$1 ORDER BY submitted_at DESC LIMIT 1', [userId]);
      const latest = kres.rows[0] || null;

      function mapToUrls(arr, resource_type='image') {
        if (!arr) return [];
        return arr.map(item => {
          if (!item) return null;
          if (typeof item === 'string' && cloudinary) return cloudinarySignedUrl(item, { resource_type });
          if (typeof item === 'string') return item;
          return null;
        }).filter(Boolean);
      }

      let latestOut = null;
      if (latest) {
        const idImgs = Array.isArray(latest.id_images) ? latest.id_images : [];
        const vids = Array.isArray(latest.work_videos) ? latest.work_videos : [];
        latestOut = {
          id: latest.id,
          user_id: latest.user_id,
          id_type: latest.id_type,
          id_name: latest.id_name,
          id_number: latest.id_number,
          id_images: mapToUrls(idImgs, 'image'),
          work_videos: mapToUrls(vids, 'video'),
          selfie: (latest.selfie && cloudinary) ? cloudinarySignedUrl(latest.selfie, { resource_type: 'image' }) : latest.selfie,
          status: latest.status,
          admin_note: latest.admin_note,
          submitted_at: latest.submitted_at
        };
      }

      return res.json({ success:true, user, latest_request: latestOut });
    } finally { client.release(); }
  } catch (err) {
    console.error('/api/kyc/status/:id error', err);
    return res.status(500).json({ success:false, message:'Server error' });
  }
});

/////////////////////////////////////////////////////////////////////
// Auth: register & login
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
      const newUser = { id: uid(), role: role || 'client', email, phone, fullname, username, state, lga, city, gender: gender || 'other', specializations: Array.isArray(specializations) ? specializations : [], password_hash: hash };
      const insertSql = `INSERT INTO users (id, role, email, phone, fullname, username, state, lga, city, gender, specializations, password_hash) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`;
      await client.query(insertSql, [newUser.id, newUser.role, newUser.email, newUser.phone, newUser.fullname, newUser.username, newUser.state, newUser.lga, newUser.city, newUser.gender, newUser.specializations, newUser.password_hash]);
      return res.json({ success:true, message:'Account created successfully', userId: newUser.id });
    } finally { client.release(); }
  } catch (err) {
    console.error('Server error /api/register', err);
    return res.status(500).json({ success:false, message:'Server error' });
  }
});

app.post('/api/login', async (req, res) => {
  try {
    const { login, password } = req.body || {};
    if (!login || !password) return res.status(400).json({ success: false, message: 'Login and password required' });
    const loginValue = String(login).trim();
    const ADMIN_USERNAME = process.env.ADMIN_USERNAME || 'admin';
    const ADMIN_PASSWORD_HASH = process.env.ADMIN_PASSWORD_HASH || null;

    if (loginValue === ADMIN_USERNAME) {
      if (ADMIN_PASSWORD_HASH && await bcrypt.compare(password, ADMIN_PASSWORD_HASH)) return res.json({ success:true, message:'Admin login successful', role:'admin', user:null });
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

      const safeUser = { id: user.id, role: user.role, email: user.email, phone: user.phone, fullname: user.fullname, username: user.username, city: user.city, kyc_status: user.kyc_status, avatar_url: user.avatar_url, online: user.online, account_details: user.account_details };

      if ((user.role || '').toLowerCase() === 'staff') {
        const BASE = process.env.ADMIN_UI_BASE || 'https://your-admin-ui.example.com';
        return res.json({ success:true, message:'Staff login', role:'staff', user:safeUser, redirect: BASE + '/staff' });
      }

      return res.json({ success:true, message:'Login successful', role:user.role||'client', user:safeUser });
    } finally { client.release(); }
  } catch (e) {
    console.error('Server error /api/login', e);
    return res.status(500).json({ success:false, message:'Server error' });
  }
});

app.get('/', (req,res)=> res.send('TradeHive backend running'));

/////////////////////////////////////////////////////////////////////
// ADS routes: create ad (store ad WITHOUT images until payment verified)
/////////////////////////////////////////////////////////////////////
app.post('/api/ads', async (req, res) => {
  try {
    const { seller_id, title, description, temp_images, images, price, currency, quantity, location, category, subcategory, idempotency_key } = req.body || {};

    if (!seller_id || !title || !price) return res.status(400).json({ success:false, message:'seller_id, title, price required' });

    const client = await pool.connect();
    try {
      if (idempotency_key) {
        const prev = (await client.query("SELECT p.id as payment_id, a.id as ad_id FROM payments p JOIN ads a ON p.ad_id=a.id WHERE p.meta->>'idempotency_key' = $1 LIMIT 1", [idempotency_key])).rows[0];
        if (prev) return res.json({ success:true, adId: prev.ad_id, paymentId: prev.payment_id, note:'idempotent-return' });
      }

      const adId = uid();
      await client.query('INSERT INTO ads (id, seller_id, title, description, images, price, currency, quantity, location, category, subcategory, status) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)', [adId, seller_id, title, description || '', [], price, currency || 'NGN', quantity || 1, location || '', category || '', subcategory || '', 'pending_payment']);

      const paymentId = uid();
      const adFee = Number(process.env.AD_FEE_NGN || 1000);
      const tx_ref = `th_ad_${uid()}_${Date.now()}`;
      const tempImgs = Array.isArray(temp_images) ? temp_images : (Array.isArray(images) ? images : []);
      const meta = { type:'ad_fee', adId, paymentId, idempotency_key: idempotency_key || null, tx_ref, temp_images: tempImgs };

      await client.query('INSERT INTO payments (id, ad_id, user_id, provider, amount, currency, status, reference, meta) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)', [paymentId, adId, seller_id, PAYMENT_PROVIDER, adFee, currency || 'NGN', 'initiated', tx_ref, JSON.stringify(meta)]);

      const sellerEmailRow = (await client.query('SELECT email FROM users WHERE id=$1 LIMIT 1', [seller_id])).rows[0];
      const email = sellerEmailRow ? sellerEmailRow.email : `seller_${seller_id}@example.com`;
      const callback = (process.env.CALLBACK_BASE_URL || '') + '/pay/ad-callback';
      const initResp = await initializePayment({ provider: PAYMENT_PROVIDER, email, amount: adFee, metadata: { ...meta, email }, currency: currency || 'NGN', callback_url: callback });

      if (initResp && (initResp.data || initResp.session)) {
        const prov = initResp.data || initResp.session;
        await client.query('UPDATE payments SET meta = coalesce(meta, \'{}\'::jsonb) || $1 WHERE id=$2', [JSON.stringify({ provider_init: prov }), paymentId]);
        const provRef = prov.reference || prov.id || prov.access_code || prov.authorization_url || prov.link || null;
        if (provRef) await client.query('UPDATE payments SET reference=$1 WHERE id=$2', [String(provRef), paymentId]);
      }

      return res.json({ success:true, adId, paymentId, init: initResp });
    } finally { client.release(); }
  } catch (err) {
    console.error('/api/ads error', err && err.stack ? err.stack : err);
    return res.status(500).json({ success:false, message:'Server error' });
  }
});
