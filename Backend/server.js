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
// ADS routes: create ad (idempotent style) - registers ad + payment
/////////////////////////////////////////////////////////////////////
// 
// --- POST /api/ads ---
// Registers a payment for an ad fee and STASHES the ad payload in payments.meta.temp_ad
app.post('/api/ads', async (req, res) => {
  try {
    const { seller_id, title, description, images, price, currency, quantity, location, category, subcategory, idempotency_key } = req.body || {};

    if (!seller_id || !title || !price) {
      return res.status(400).json({ success:false, message:'seller_id, title, price required' });
    }

    const client = await pool.connect();
    try {
      // ensure seller exists (avoid FK violation)
      const sellerQ = await client.query('SELECT id,email FROM users WHERE id=$1 LIMIT 1', [seller_id]);
      if (!sellerQ.rows.length) {
        return res.status(401).json({ success:false, message:'Invalid seller_id — please sign in' });
      }
      const sellerRow = sellerQ.rows[0];

      // idempotency: if idempotency_key exists, look for existing payments with that key
      if (idempotency_key) {
        const prev = (await client.query(
          "SELECT p.id as payment_id, p.ad_id as ad_id FROM payments p WHERE p.meta->>'idempotency_key' = $1 LIMIT 1",
          [idempotency_key]
        )).rows[0];
        if (prev) return res.json({ success:true, adId: prev.ad_id, paymentId: prev.payment_id, note:'idempotent-return' });
      }

      // build temp_ad payload (store what we need later when payment verifies)
      const tempAd = {
        seller_id,
        title,
        description: description || '',
        images: Array.isArray(images) ? images : [],
        price,
        currency: currency || 'NGN',
        quantity: quantity || 1,
        location: location || '',
        category: category || '',
        subcategory: subcategory || '',
        created_at: (new Date()).toISOString()
      };

      const paymentId = uid();
      const adFee = Number(process.env.AD_FEE_NGN || 1000);
      const tx_ref = `th_ad_${uid()}_${Date.now()}`;

      const meta = {
        type: 'ad_fee',
        // do NOT set adId yet (we'll create ad after payment verification)
        adId: null,
        paymentId,
        idempotency_key: idempotency_key || null,
        tx_ref,
        temp_ad: tempAd,
        temp_images: Array.isArray(images) ? images : []
      };

      // insert a payments row with ad_id = NULL (we will set ad_id on verification)
      await client.query(
        'INSERT INTO payments (id, ad_id, user_id, provider, amount, currency, status, reference, meta) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)',
        [paymentId, null, seller_id, PAYMENT_PROVIDER, adFee, currency || 'NGN', 'initiated', tx_ref, JSON.stringify(meta)]
      );

      // initialize provider payment (flutterwave / paystack etc)
      const email = sellerRow.email || `seller_${seller_id}@example.com`;
      const callback = (process.env.CALLBACK_BASE_URL || '') + '/pay/ad-callback';
      const initResp = await initializePayment({ provider: PAYMENT_PROVIDER, email, amount: adFee, metadata: { ...meta, email }, currency: currency || 'NGN', callback_url: callback });

      // store provider init in payments.meta, and update reference if provider gave one
      if (initResp && (initResp.data || initResp.session)) {
        const prov = initResp.data || initResp.session;
        await client.query("UPDATE payments SET meta = coalesce(meta, '{}'::jsonb) || $1 WHERE id=$2", [JSON.stringify({ provider_init: prov }), paymentId]);
        const provRef = prov.reference || prov.id || prov.access_code || prov.authorization_url || prov.link || null;
        if (provRef) await client.query('UPDATE payments SET reference=$1 WHERE id=$2', [String(provRef), paymentId]);
      }

      return res.json({ success:true, paymentId, init: initResp, note:'payment-created-temp_ad-stashed' });
    } finally { client.release(); }
  } catch (err) {
    console.error('/api/ads error', err && err.stack ? err.stack : err);
    return res.status(500).json({ success:false, message:'Server error' });
  }
});

/////////////////////////////////////////////////////////////////////
// GET /api/ads (by status) and GET /api/ads/:id
/////////////////////////////////////////////////////////////////////

// helper to normalize images field into an array
function normalizeImagesField(imagesField) {
  if (!imagesField) return [];
  if (Array.isArray(imagesField)) return imagesField;
  // if stored as JSON string in DB, try parse
  if (typeof imagesField === 'string') {
    try {
      const parsed = JSON.parse(imagesField);
      if (Array.isArray(parsed)) return parsed;
    } catch (e) {
      // not JSON, treat as single string URL
      return [imagesField];
    }
  }
  // fallback to empty array
  return [];
}
app.get('/api/ads/:id', async (req, res) => {
  try {
    const id = req.params.id;
    const client = await pool.connect();
    try {
      const r = await client.query('SELECT a.*, u.email as seller_email, u.fullname as seller_name FROM ads a LEFT JOIN users u ON u.id=a.seller_id WHERE a.id=$1 LIMIT 1', [id]);
      if (!r.rows.length) return res.status(404).json({ success:false, message:'not-found' });
      const ad = r.rows[0];
      ad.images = normalizeImagesField(ad.images);
      return res.json({ success:true, ad });
    } finally { client.release(); }
  } catch (e) {
    console.error('GET /api/ads/:id', e);
    return res.status(500).json({ success:false });
  }
});



/////////////////////////////////////////////////////////////////////
// POST /api/ads/:id/buy (create order + payment)
/////////////////////////////////////////////////////////////////////
app.post('/api/ads/:id/buy', async (req, res) => {
  const adId = req.params.id;
  const { buyer_id, qty = 1, idempotency_key } = req.body || {};
  if (!buyer_id) return res.status(400).json({ success:false, message:'buyer_id required' });

  const client = await pool.connect();
  try {
    if (idempotency_key) {
      const existing = (await client.query("SELECT p.* FROM payments p WHERE p.meta->>'idempotency_key' = $1 LIMIT 1", [idempotency_key])).rows[0];
      if (existing) return res.json({ success:true, orderId: existing.order_id, paymentId: existing.id, note:'idempotent-return' });
    }

    const adRes = (await client.query('SELECT * FROM ads WHERE id=$1 LIMIT 1', [adId])).rows[0];
    if (!adRes) return res.status(404).json({ success:false, message:'ad-not-found' });

    const amount = Number(adRes.price) * Number(qty);
    const orderId = uid();

    await client.query('BEGIN');
    await client.query('INSERT INTO orders (id, ad_id, buyer_id, seller_id, qty, amount, currency, status) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)', [orderId, adId, buyer_id, adRes.seller_id, qty, amount, adRes.currency || 'NGN', 'pending_payment']);

    const paymentId = uid();
    const tx_ref = 'th_order_' + uid() + '_' + Date.now();
    const metaObj = { type:'order', orderId, paymentId, idempotency_key: idempotency_key || null, tx_ref };
    await client.query('INSERT INTO payments (id, order_id, ad_id, user_id, provider, amount, currency, status, reference, meta) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)', [paymentId, orderId, adId, buyer_id, PAYMENT_PROVIDER, amount, adRes.currency || 'NGN', 'initiated', tx_ref, JSON.stringify(metaObj)]);

    const buyerEmailRow = (await client.query('SELECT email FROM users WHERE id=$1 LIMIT 1', [buyer_id])).rows[0];
    const email = buyerEmailRow ? buyerEmailRow.email : `buyer_${buyer_id}@example.com`;
    const callback = (process.env.CALLBACK_BASE_URL || '') + '/pay/order-callback';
    const initResp = await initializePayment({ provider: PAYMENT_PROVIDER, email, amount, metadata: { ...metaObj, email }, currency: adRes.currency || 'NGN', callback_url: callback });

    if (initResp && (initResp.data || initResp.session)) {
      const prov = initResp.data || initResp.session;
      await client.query('UPDATE payments SET meta = coalesce(meta, \'{}\'::jsonb) || $1 WHERE id=$2', [JSON.stringify({ provider_init: prov }), paymentId]);
      const provRef = prov.reference || prov.id || prov.access_code || prov.authorization_url || prov.link || null;
      if (provRef) await client.query('UPDATE payments SET reference=$1 WHERE id=$2', [String(provRef), paymentId]);
    }

    await client.query('COMMIT');
    return res.json({ success:true, orderId, paymentId, init: initResp });
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch(_) {}
    console.error('POST /api/ads/:id/buy error', err);
    return res.status(500).json({ success:false, message:'Server error' });
  } finally { client.release(); }
});

/////////////////////////////////////////////////////////////////////
// VERIFY & CALLBACK HANDLER (robust + safe)
// Replaces earlier fragile version. Accepts req,res directly.
// paste this replacement function into server.js (overwrite existing verifyAndProcessProviderPayment)
async function verifyAndProcessProviderPayment(req, res) {
  try {
    const rawQuery = req.query || {};
    const provider = (rawQuery.provider || process.env.PAYMENT_PROVIDER || 'paystack').toString().toLowerCase();

    const tx_ref = rawQuery.tx_ref || rawQuery.txref || rawQuery.txReference || rawQuery.reference || rawQuery.tx || null;
    let transaction_id = rawQuery.transaction_id || rawQuery.transactionId || rawQuery.transaction || rawQuery.id || rawQuery.flw_ref || null;

    console.log('--- verify start ---', { provider, tx_ref, transaction_id, rawQuery });

    let verifyResp = null;
    let isVerified = false; // becomes true when provider confirms success

    // ---------- FLUTTERWAVE ----------
    if (provider === 'flutterwave') {
      if (!process.env.FLUTTERWAVE_SECRET_KEY) {
        console.warn('Flutterwave not configured on server.');
        return res.status(500).send('Flutterwave not configured on server.');
      }

      // Many flutterwave integrations return ?status=completed on callback — treat as success if status indicates completed/success
      if (rawQuery.status) {
        const s = String(rawQuery.status).toLowerCase();
        if (s === 'completed' || s === 'successful' || s === 'success') {
          isVerified = Boolean(transaction_id || tx_ref);
          verifyResp = { status: 'success', data: { id: transaction_id || null, tx_ref: tx_ref || null, status: s } };
        } else if (s === 'failed' || s === 'cancelled') {
          isVerified = false;
          verifyResp = { status: 'error', data: { status: s } };
        }
      }

      // If no transaction_id provided, attempt to discover it from payments.meta.provider_init
      if (!transaction_id && tx_ref) {
        try {
          const c = await pool.connect();
          try {
            const q = await c.query("SELECT meta FROM payments WHERE meta->>'tx_ref' = $1 OR reference = $1 LIMIT 1", [String(tx_ref)]);
            if (q.rows.length && q.rows[0].meta) {
              let m = q.rows[0].meta;
              if (typeof m === 'string') m = safeJsonParse(m) || {};
              if (m && m.provider_init) {
                const p = m.provider_init;
                if (p.data && (p.data.id || p.data.flw_ref)) transaction_id = transaction_id || p.data.id || p.data.flw_ref;
                if (!transaction_id && (p.reference || p.id)) transaction_id = p.reference || p.id;
              }
            }
          } finally { c.release(); }
        } catch (e) {
          console.warn('While attempting to find fallback transaction_id by tx_ref:', e && e.message ? e.message : e);
        }
      }

      // If we haven't already decided based on status, verify using Flutterwave verify endpoint (if we have transaction_id)
      if (!verifyResp || verifyResp.status === undefined) {
        if (!transaction_id) {
          console.warn('Flutterwave callback: transaction_id missing (and not discoverable). Query:', rawQuery);
          verifyResp = { status: 'error', message: 'transaction_id missing' };
          isVerified = false;
        } else {
          console.log('Calling Flutterwave verify endpoint for transaction_id:', transaction_id);
          const r = await fetch(`https://api.flutterwave.com/v3/transactions/${encodeURIComponent(transaction_id)}/verify`, {
            headers: { Authorization: `Bearer ${process.env.FLUTTERWAVE_SECRET_KEY}`, 'Content-Type':'application/json' }
          });
          try { verifyResp = await r.json(); } catch(e){ const text = await r.text().catch(()=>null); console.error('Flutterwave verify parse error', e, text); return res.status(500).send('Failed to parse provider response.'); }
          console.log('Flutterwave verify response:', JSON.stringify(verifyResp));

          // Accept 'successful', 'success' OR 'completed' as success
          const statusVal = (verifyResp && verifyResp.data && (verifyResp.data.status || verifyResp.status)) || null;
          if (statusVal) {
            const s = String(statusVal).toLowerCase();
            isVerified = (s === 'successful' || s === 'success' || s === 'completed');
          } else {
            isVerified = false;
          }
          if (!isVerified) console.warn('Flutterwave verify did NOT confirm success', verifyResp);
        }
      }
    }

    // ---------- PAYSTACK ----------
    else if (provider === 'paystack') {
      if (!process.env.PAYSTACK_SECRET_KEY) {
        console.warn('Paystack not configured on server.');
        return res.status(500).send('Paystack not configured on server.');
      }
      const paystackBase = 'https://api.paystack.co';
      const attemptVerify = async (ref) => {
        try {
          const r = await fetch(`${paystackBase}/transaction/verify/${encodeURIComponent(ref)}`, { headers: { Authorization: `Bearer ${process.env.PAYSTACK_SECRET_KEY}`, 'Content-Type':'application/json' }});
          return await r.json().catch(()=>null);
        } catch(e){ return null; }
      };
      let j = null;
      if (transaction_id) j = await attemptVerify(transaction_id);
      if (!j && tx_ref) j = await attemptVerify(tx_ref);
      console.log('Paystack verify response:', j);
      if (!j) {
        verifyResp = { status: 'error', message: 'no-response' };
        isVerified = false;
        console.warn('Paystack verification returned no usable response', j);
      } else {
        verifyResp = j;
        // paystack uses boolean `status` and j.data.status string (success)
        isVerified = !!(j.status && j.data && (String(j.data.status).toLowerCase() === 'success' || String(j.data.status).toLowerCase() === 'successful'));
        if (!isVerified) console.warn('Paystack verify did NOT confirm success', j);
      }
    }

    // ---------- STRIPE (minimal) ----------
    else if (provider === 'stripe') {
      verifyResp = { data: rawQuery || {} };
      isVerified = !!(rawQuery && (rawQuery.id || rawQuery.session_id || rawQuery.payment_intent));
    } else {
      return res.status(400).send('Unsupported provider.');
    }

    // ---------- now find matching payments row ----------
    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      const candidates = new Set();
      if (verifyResp && verifyResp.data) {
        const d = verifyResp.data;
        if (d.reference) candidates.add(String(d.reference));
        if (d.tx_ref) candidates.add(String(d.tx_ref));
        if (d.flw_ref) candidates.add(String(d.flw_ref));
        if (d.id) candidates.add(String(d.id));
        if (d.payment_id) candidates.add(String(d.payment_id));
      }
      if (tx_ref) candidates.add(String(tx_ref));
      if (transaction_id) candidates.add(String(transaction_id));
      const uniq = Array.from(candidates).filter(Boolean);

      console.log('Candidates for payments lookup:', uniq);

      let paymentRow = null;
      for (const c of uniq) {
        const q1 = await client.query('SELECT * FROM payments WHERE reference = $1 LIMIT 1', [c]);
        if (q1.rows.length) { paymentRow = q1.rows[0]; break; }
        const q2 = await client.query('SELECT * FROM payments WHERE id = $1 LIMIT 1', [c]);
        if (q2.rows.length) { paymentRow = q2.rows[0]; break; }
        const q3 = await client.query("SELECT * FROM payments WHERE (meta->>'paymentId' = $1 OR meta->>'orderId' = $1 OR meta->>'adId' = $1 OR meta->>'tx_ref' = $1) LIMIT 1", [c]);
        if (q3.rows.length) { paymentRow = q3.rows[0]; break; }
      }

      // fallback: provider_init.reference
      if (!paymentRow && tx_ref) {
        const qf = await client.query("SELECT * FROM payments WHERE meta->'provider_init'->>'reference' = $1 LIMIT 1", [tx_ref]);
        if (qf.rows.length) paymentRow = qf.rows[0];
      }
      if (!paymentRow && tx_ref) {
        const pattern = `%${tx_ref}%`;
        const qf2 = await client.query("SELECT * FROM payments WHERE meta::text ILIKE $1 LIMIT 1", [pattern]);
        if (qf2.rows.length) paymentRow = qf2.rows[0];
      }

      if (!paymentRow) {
        await client.query('ROLLBACK');
        console.warn('No matching payment found for verified txn. Candidates:', uniq);
        return res.status(404).send('Payment verified with provider but matching payment record not found on server. Check payments table for stored tx_ref or reference.');
      }

      console.log('Found paymentRow:', { id: paymentRow.id, order_id: paymentRow.order_id, ad_id: paymentRow.ad_id, status: paymentRow.status });

      // already processed?
      if ((paymentRow.status || '').toLowerCase() === 'success' || (paymentRow.status || '').toLowerCase() === 'paid') {
        await client.query('COMMIT');
        return res.send(`<h2>Payment already processed</h2><p>Payment id ${paymentRow.id} was already processed.</p>`);
      }

      // parse meta
      let metaObj = paymentRow.meta;
      if (typeof metaObj === 'string') metaObj = safeJsonParse(metaObj) || {};
      if (!metaObj || typeof metaObj !== 'object') metaObj = {};

      // ---------- failure handling: mark payment failed and delete ad (per your request) ----------
      if (!isVerified) {
        await client.query("UPDATE payments SET status=$1, meta = coalesce(meta, '{}'::jsonb) || $2 WHERE id=$3", ['failed', JSON.stringify({ provider_verify: verifyResp }), paymentRow.id]);

        if (paymentRow.ad_id) {
          try {
            await client.query('DELETE FROM ads WHERE id=$1', [paymentRow.ad_id]);
            console.log('Deleted unpaid ad:', paymentRow.ad_id);
          } catch (e) {
            console.warn('Failed to delete unpaid ad', paymentRow.ad_id, e && e.message ? e.message : e);
            try { await client.query('UPDATE ads SET status=$1 WHERE id=$2', ['payment_failed', paymentRow.ad_id]); } catch(_) {}
          }
        }

        // cleanup tmp images if any
        const tmpImgs = (metaObj && metaObj.temp_images && Array.isArray(metaObj.temp_images)) ? metaObj.temp_images.slice() : [];
        for (const t of tmpImgs) {
          try {
            if (typeof t === 'string' && t.startsWith('tmp-')) {
              const tmpPath = path.join(TMP_ADS_DIR, t);
              if (fs.existsSync(tmpPath)) {
                try { fs.unlinkSync(tmpPath); } catch(e) { /* ignore */ }
              }
            }
          } catch (e) {
            console.warn('tmp cleanup error', e && e.message ? e.message : e);
          }
        }

        await client.query('COMMIT');

        const FRONTEND_BASE = process.env.FRONTEND_BASE || '';
        const redirectTo = FRONTEND_BASE ? FRONTEND_BASE.replace(/\/$/, '') + '/ads.html' : '/ads.html';
        const params = new URLSearchParams();
        params.set('payment', 'failed'); params.set('pid', String(paymentRow.id));
        if (paymentRow.ad_id) params.set('ad', String(paymentRow.ad_id));
        const finalUrl = redirectTo + '?' + params.toString();
        try { return res.redirect(302, finalUrl); } catch (e) { return res.send(`<h2>Payment failed</h2><p>payment id: ${paymentRow.id}</p><p><a href="${finalUrl}">Continue</a></p>`); }
      }

      // ---------- success handling ----------
      await client.query("UPDATE payments SET status=$1, meta = coalesce(meta, '{}'::jsonb) || $2 WHERE id=$3", ['success', JSON.stringify({ provider_verify: verifyResp }), paymentRow.id]);

      // create ad if ad_id is not set (we stashed temp_ad in payment.meta earlier)
      let adIdToUse = paymentRow.ad_id;
      if (!adIdToUse) {
        const temp = (metaObj && metaObj.temp_ad && typeof metaObj.temp_ad === 'object') ? metaObj.temp_ad : null;
        if (temp) {
          adIdToUse = uid();
          await client.query(
            `INSERT INTO ads (id, seller_id, title, description, images, price, currency, quantity, location, category, subcategory, status, created_at)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
            [
              adIdToUse,
              temp.seller_id,
              temp.title || '',
              temp.description || '',
              Array.isArray(temp.images) ? temp.images : [],
              temp.price || 0,
              temp.currency || 'NGN',
              temp.quantity || 1,
              temp.location || '',
              temp.category || '',
              temp.subcategory || '',
              'pending_verification',
              temp.created_at || new Date().toISOString()
            ]
          );
          await client.query('UPDATE payments SET ad_id=$1 WHERE id=$2', [adIdToUse, paymentRow.id]);
          console.log('Created ad after successful payment', { adId: adIdToUse, paymentId: paymentRow.id });
        } else {
          console.warn('No temp_ad found in payment.meta — cannot create ad. PaymentRow:', paymentRow.id);
        }
      } else {
        await client.query('UPDATE ads SET status=$1 WHERE id=$2', ['pending_verification', adIdToUse]);
      }

      // transfer images (cloudinary/local tmp)
      const tempImgs = (metaObj && metaObj.temp_images && Array.isArray(metaObj.temp_images)) ? metaObj.temp_images.slice() : [];
      const finalImages = [];
      for (const t of tempImgs) {
        if (!t) continue;
        if (typeof t === 'string' && (t.startsWith('http://') || t.startsWith('https://'))) { finalImages.push(t); continue; }
        if (cloudinary && typeof t === 'string' && !t.includes('/') && !t.startsWith('tmp-')) { finalImages.push(cloudinaryPublicUrl(t) || t); continue; }
        if (typeof t === 'string' && t.startsWith('tmp-')) {
          const tmpPath = path.join(TMP_ADS_DIR, t);
          if (fs.existsSync(tmpPath)) {
            if (cloudinary) {
              try {
                const ext = path.extname(tmpPath).toLowerCase();
                const resource_type = ['.mp4','.mov','.webm','.ogg'].includes(ext) ? 'video' : 'image';
                const uploadRes = await cloudinary.uploader.upload(tmpPath, { folder: resource_type === 'video' ? 'tradehive/ads/videos' : 'tradehive/ads/images', resource_type, type: 'upload', public_id: `ad-${Date.now()}-${Math.floor(Math.random()*90000)}` });
                if (uploadRes && uploadRes.secure_url) finalImages.push(uploadRes.secure_url);
                else if (uploadRes && uploadRes.public_id) finalImages.push(cloudinaryPublicUrl(uploadRes.public_id) || uploadRes.public_id);
              } catch (ue) {
                console.warn('cloudinary upload failed for', tmpPath, ue && ue.message ? ue.message : ue);
                finalImages.push(`${req.protocol}://${req.get('host')}/uploads/tmp_ads/${t}`);
              }
            } else {
              finalImages.push(`${req.protocol}://${req.get('host')}/uploads/tmp_ads/${t}`);
            }
            try { fs.unlinkSync(tmpPath); } catch(e){ /* ignore */ }
          } else {
            console.warn('tmp image file not found', tmpPath);
          }
          continue;
        }
        finalImages.push(t);
      }

      if (finalImages.length && adIdToUse) {
        await client.query('UPDATE ads SET images=$1 WHERE id=$2', [finalImages, adIdToUse]);
      }

      // notifications (best-effort)
      try {
        const sellerRow = (await client.query('SELECT * FROM users WHERE id=$1 LIMIT 1', [paymentRow.user_id])).rows[0];
        if (sellerRow) await sendEmail({ to: sellerRow.email || sellerRow.username, subject: 'Ad fee received — pending admin verification', text: `Your ad ${adIdToUse || ''} fee was received; admin will review.` });
      } catch (e) { console.warn('sendEmail (ad) failed', e && e.message ? e.message : e); }

      if (process.env.ADMIN_EMAIL) {
        try { await sendEmail({ to: process.env.ADMIN_EMAIL, subject: 'New ad pending verification', text: `Ad ${adIdToUse || ''} paid and requires review.` }); } catch(e){ console.warn('sendEmail admin failed', e && e.message ? e.message : e); }
      }

      // order finalization (unchanged)
      if (paymentRow.order_id) {
        await client.query('UPDATE orders SET status=$1 WHERE id=$2', ['paid', paymentRow.order_id]);
        const order = (await client.query('SELECT * FROM orders WHERE id=$1 LIMIT 1', [paymentRow.order_id])).rows[0];
        if (order) {
          try { const seller = (await client.query('SELECT * FROM users WHERE id=$1 LIMIT 1', [order.seller_id])).rows[0]; if (seller) await sendEmail({ to: seller.email || seller.username, subject: `Order ${paymentRow.order_id} has been paid`, text: `Order ${paymentRow.order_id} was paid. Qty: ${order.qty}. Amount: ${order.amount}.` }); } catch(e){ console.warn('sendEmail seller (order) failed', e && e.message ? e.message : e); }
          try { const buyer = (await client.query('SELECT * FROM users WHERE id=$1 LIMIT 1', [order.buyer_id])).rows[0]; if (buyer) await sendEmail({ to: buyer.email || buyer.username, subject: `Payment received for order ${paymentRow.order_id}`, text: `We received your payment for order ${paymentRow.order_id}.` }); } catch(e){ console.warn('sendEmail buyer (order) failed', e && e.message ? e.message : e); }
        }
      }

      await client.query('COMMIT');

      // redirect back to frontend ads page (success)
      const FRONTEND_BASE = process.env.FRONTEND_BASE || '';
      let redirectTo = '';
      if (paymentRow.order_id) redirectTo = FRONTEND_BASE ? FRONTEND_BASE.replace(/\/$/, '') + '/myorders.html' : '/myorders.html';
      else if (adIdToUse) redirectTo = FRONTEND_BASE ? FRONTEND_BASE.replace(/\/$/, '') + '/ads.html' : '/ads.html';
      else redirectTo = FRONTEND_BASE ? FRONTEND_BASE.replace(/\/$/, '') + '/' : '/';

      const params = new URLSearchParams();
      params.set('payment', 'success');
      params.set('pid', String(paymentRow.id));
      if (adIdToUse) params.set('ad', String(adIdToUse));
      if (paymentRow.order_id) params.set('order', String(paymentRow.order_id));
      const finalUrl = redirectTo + '?' + params.toString();

      try { return res.redirect(302, finalUrl); }
      catch (e) { console.warn('Redirect failed, returning HTML link', e && e.message ? e.message : e); return res.send(`<h2>Payment verified and processed</h2><p>payment id: ${paymentRow.id}</p><p><a href="${finalUrl}">Continue</a></p>`); }
    } finally {
      client.release();
    }
  } catch (err) {
    console.error('verifyAndProcessProviderPayment error', err && err.stack ? err.stack : err);
    return res.status(500).send('Error processing verification — check server logs for details.');
  }
}

app.get('/pay/ad-callback', (req, res) => verifyAndProcessProviderPayment(req, res));
app.get('/pay/order-callback', (req, res) => verifyAndProcessProviderPayment(req, res));
//////////////////////////////////////////////////////////
//Webhooks (paystack, flutterwave, stripe)//
/////////////////////////////////////////////////////////////////////
app.post('/webhook/paystack', express.json({ limit:'1mb' }), async (req, res) => {
  try {
    if (process.env.PAYSTACK_SECRET_KEY && !verifyPaystackSignature(req)) { console.warn('Paystack signature mismatch'); return res.status(400).send('invalid signature'); }
    const b = req.body || {};
    const data = b.data || {};
    const reference = data.reference || data.id || null;
    const client = await pool.connect();
    try {
      if (!reference) return res.status(200).send('ok');
      const pq = await client.query('SELECT * FROM payments WHERE reference=$1 LIMIT 1', [reference]);
      if (!pq.rows.length) return res.status(200).send('ok');
      const payment = pq.rows[0];
      if ((payment.status || '').toLowerCase() === 'success') return res.status(200).send('ok');
      if (data.status === 'success' || String(data.gateway_response || '').toLowerCase().includes('approved')) {
        await client.query('UPDATE payments SET status=$1, meta = coalesce(meta, \'{}\'::jsonb) || $2 WHERE id=$3', ['success', JSON.stringify({ provider_data: data }), payment.id]);
        if (payment.ad_id) await client.query('UPDATE ads SET status=$1 WHERE id=$2', ['pending_verification', payment.ad_id]);
        if (payment.order_id) await client.query('UPDATE orders SET status=$1, updated_at=now() WHERE id=$2', ['paid', payment.order_id]);
      }
      return res.status(200).send('ok');
    } finally { client.release(); }
  } catch (err) { console.error('Paystack webhook error', err); res.status(500).send('error'); }
});

app.post('/webhook/flutterwave', express.json({ limit:'1mb' }), async (req, res) => {
  try {
    const header = req.headers['verif-hash'] || req.headers['verif_hash'] || req.headers['x-verif-hash'];
    if (process.env.FLUTTERWAVE_WEBHOOK_SECRET && header !== process.env.FLUTTERWAVE_WEBHOOK_SECRET) { console.warn('Flutterwave webhook signature mismatch'); return res.status(400).send('invalid signature'); }
    const b = req.body || {};
    const data = b.data || {};
    const ref = data.tx_ref || data.flw_ref || data.reference;
    const client = await pool.connect();
    try {
      let p = null;
      if (ref) {
        const pq = await client.query('SELECT * FROM payments WHERE reference=$1 LIMIT 1', [ref]);
        if (pq.rows.length) p = pq.rows[0];
      }
      if (!p && data.meta && data.meta.orderId) {
        const pq2 = await client.query("SELECT * FROM payments WHERE meta->>'orderId' = $1 LIMIT 1", [String(data.meta.orderId)]);
        if (pq2.rows.length) p = pq2.rows[0];
      }
      if (p && (String(data.status).toLowerCase() === 'successful' || String(data.status).toLowerCase() === 'success')) {
        await client.query('UPDATE payments SET status=$1, meta = coalesce(meta, \'{}\'::jsonb) || $2 WHERE id=$3', ['success', JSON.stringify({ provider_data: data }), p.id]);
        if (p.ad_id) await client.query('UPDATE ads SET status=$1 WHERE id=$2', ['pending_verification', p.ad_id]);
        if (p.order_id) await client.query('UPDATE orders SET status=$1, updated_at=now() WHERE id=$2', ['paid', p.order_id]);
      } else {
        console.log('flutterwave webhook: not success or not matched', data.status);
      }
      return res.status(200).send('ok');
    } finally { client.release(); }
  } catch (e) { console.error('Flutterwave webhook error', e); res.status(500).send('error'); }
});

app.post('/webhook/stripe', express.raw({ type: 'application/json' }), async (req, res) => {
  if (!stripeLib || !process.env.STRIPE_WEBHOOK_SECRET) return res.status(400).send('stripe not configured');
  try {
    const sig = req.headers['stripe-signature'];
    const event = stripeLib.webhooks.constructEvent(req.body, sig, process.env.STRIPE_WEBHOOK_SECRET);
    if (!event) return res.status(400).send('invalid event');
    const type = event.type;
    if (type === 'checkout.session.completed' || type === 'payment_intent.succeeded') {
      const session = event.data.object;
      const client = await pool.connect();
      try {
        let payment = null;
        const metadata = session.metadata || {};
        if (metadata.paymentId) {
          const pq = await client.query('SELECT * FROM payments WHERE id=$1 LIMIT 1', [metadata.paymentId]);
          if (pq.rows.length) payment = pq.rows[0];
        }
        if (!payment && metadata.orderId) {
          const pq2 = await client.query('SELECT * FROM payments WHERE order_id=$1 LIMIT 1', [metadata.orderId]);
          if (pq2.rows.length) payment = pq2.rows[0];
        }
        if (!payment) {
          const pq3 = await client.query('SELECT * FROM payments WHERE reference=$1 LIMIT 1', [session.id]);
          if (pq3.rows.length) payment = pq3.rows[0];
        }
        if (payment) {
          await client.query('UPDATE payments SET status=$1, meta = coalesce(meta, \'{}\'::jsonb) || $2 WHERE id=$3', ['success', JSON.stringify({ provider_data: session }), payment.id]);
          if (payment.ad_id) await client.query('UPDATE ads SET status=$1 WHERE id=$2', ['pending_verification', payment.ad_id]);
          if (payment.order_id) await client.query('UPDATE orders SET status=$1, updated_at=now() WHERE id=$2', ['paid', payment.order_id]);
        }
      } finally { client.release(); }
    }
    res.json({ received: true });
  } catch (err) { console.error('Stripe webhook error', err); res.status(400).send(`Webhook Error: ${err.message}`); }
});

/////////////////////////////////////////////////////////////////////
// Admin endpoints for ads verification (approve/decline)
/////////////////////////////////////////////////////////////////////
app.get('/admin/ads/pending', adminAuth, async (req,res) => {
  try {
    const client = await pool.connect();
    try {
      const r = await client.query('SELECT a.*, u.email as seller_email, u.fullname as seller_name FROM ads a LEFT JOIN users u ON u.id=a.seller_id WHERE a.status=$1 ORDER BY created_at DESC', ['pending_verification']);
      return res.json({ success:true, ads: r.rows });
    } finally { client.release(); }
  } catch(e){ console.error('admin list pending', e); res.status(500).json({ success:false }); }
});

app.post('/admin/ads/:id/approve', adminAuth, async (req,res) => {
  try {
    const id = req.params.id;
    const client = await pool.connect();
    try {
      const r = await client.query('SELECT * FROM ads WHERE id=$1 LIMIT 1', [id]);
      if (!r.rows.length) return res.status(404).json({ success:false, message:'not-found' });
      await client.query('UPDATE ads SET status=$1 WHERE id=$2', ['live', id]);
      const seller = (await client.query('SELECT * FROM users WHERE id=$1 LIMIT 1', [r.rows[0].seller_id])).rows[0];
      if (seller) { try { await sendEmail({ to: seller.email || seller.username, subject: 'Ad approved', text: `Your ad ${id} was approved and is now live.` }); } catch(e){ console.warn('sendEmail approve failed', e && e.message ? e.message : e); } }
      return res.json({ success:true, message:'approved' });
    } finally { client.release(); }
  } catch (e){ console.error('admin approve', e); res.status(500).json({ success:false }); }
});

app.post('/admin/ads/:id/decline', adminAuth, async (req,res) => {
  try {
    const id = req.params.id;
    const { reason } = req.body || {};
    const client = await pool.connect();
    try {
      const r = await client.query('SELECT * FROM ads WHERE id=$1 LIMIT 1', [id]);
      if (!r.rows.length) return res.status(404).json({ success:false, message:'not-found' });
      await client.query('UPDATE ads SET status=$1 WHERE id=$2', ['removed', id]);
      const seller = (await client.query('SELECT * FROM users WHERE id=$1 LIMIT 1', [r.rows[0].seller_id])).rows[0];
      if (seller) { try { await sendEmail({ to: seller.email || seller.username, subject: 'Ad declined', text: `Your ad ${id} was declined. Reason: ${reason || 'No reason provided'}` }); } catch(e){ console.warn('sendEmail decline failed', e && e.message ? e.message : e); } }
      return res.json({ success:true, message:'declined' });
    } finally { client.release(); }
  } catch (e){ console.error('admin decline', e); res.status(500).json({ success:false }); }
});

/////////////////////////////////////////////////////////////////////
// User endpoints (profile, online toggle with geolocation, bank details, my ads, patch ad status)
/////////////////////////////////////////////////////////////////////
app.get('/api/users/:id', async (req, res) => {
  try {
    const id = req.params.id;
    const client = await pool.connect();
    try {
      const q = await client.query('SELECT id, role, email, phone, fullname, username, city, kyc_status, avatar_url, account_details, online, created_at FROM users WHERE id=$1 LIMIT 1', [id]);
      if (!q.rows.length) return res.status(404).json({ success:false, message:'user-not-found' });
      return res.json({ success:true, user: q.rows[0] });
    } finally { client.release(); }
  } catch (err) { console.error('/api/users/:id', err); return res.status(500).json({ success:false, message:'server-error' }); }
});

app.post('/api/users/:id/online', async (req, res) => {
  try {
    const id = req.params.id;
    const { online } = req.body || {};
    const lat = req.body.lat || null;
    const lng = req.body.lng || null;
    if (typeof online === 'undefined') return res.status(400).json({ success:false, message:'online boolean required' });

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      if (lat !== null && lng !== null) {
        await client.query(`UPDATE users SET account_details = coalesce(account_details, '{}'::jsonb) || jsonb_build_object('last_lat', $1, 'last_lng', $2), online = $3 WHERE id=$4`, [String(lat), String(lng), online, id]);
      } else {
        await client.query(`UPDATE users SET online = $1 WHERE id=$2`, [online, id]);
      }
      if (!online) {
        await client.query(`UPDATE ads SET status='offline', updated_at=now() WHERE seller_id=$1 AND status='live'`, [id]);
      } else {
        await client.query(`UPDATE ads SET status='live', updated_at=now() WHERE seller_id=$1 AND status='offline'`, [id]);
      }
      await client.query('COMMIT');
      return res.json({ success:true, message: `online set to ${online}` });
    } finally { client.release(); }
  } catch (err) { console.error('/api/users/:id/online', err); return res.status(500).json({ success:false, message:'server-error' }); }
});

app.post('/api/users/:id/bank', async (req, res) => {
  try {
    const id = req.params.id;
    const { account_number, bank_code, name, recipient_code, beneficiary_id, connect_account_id } = req.body || {};
    if (!account_number || !bank_code || !name) return res.status(400).json({ success:false, message:'account_number, bank_code and name required' });

    const client = await pool.connect();
    try {
      const details = { account_number: String(account_number), bank_code: String(bank_code), name: String(name) };
      if (recipient_code) details.recipient_code = String(recipient_code);
      if (beneficiary_id) details.beneficiary_id = String(beneficiary_id);
      if (connect_account_id) details.connect_account_id = String(connect_account_id);

      await client.query('UPDATE users SET account_details = coalesce(account_details, \'{}\'::jsonb) || $1 WHERE id=$2', [JSON.stringify(details), id]);
      return res.json({ success:true, message:'bank details saved' });
    } finally { client.release(); }
  } catch (err) { console.error('/api/users/:id/bank', err); return res.status(500).json({ success:false, message:'server-error' }); }
});

app.get('/api/my/ads', async (req, res) => {
  try {
    const userId = req.query.user_id || req.query.user || null;
    if (!userId) return res.status(400).json({ success:false, message:'user_id required' });
    const client = await pool.connect();
    try {
      const r = await client.query('SELECT * FROM ads WHERE seller_id=$1 ORDER BY created_at DESC', [userId]);
      return res.json({ success:true, ads: r.rows });
    } finally { client.release(); }
  } catch (err) { console.error('/api/my/ads', err); return res.status(500).json({ success:false, message:'server-error' }); }
});

app.patch('/api/ads/:id/status', async (req, res) => {
  try {
    const id = req.params.id;
    const { status } = req.body || {};
    if (!status) return res.status(400).json({ success:false, message:'status required' });

    const allowed = ['live','offline','removed','pending_verification','draft','expired','pending_payment'];
    if (!allowed.includes(status)) return res.status(400).json({ success:false, message:'invalid-status' });

    const client = await pool.connect();
    try {
      const q = await client.query('SELECT * FROM ads WHERE id=$1 LIMIT 1', [id]);
      if (!q.rows.length) return res.status(404).json({ success:false, message:'not-found' });
      await client.query('UPDATE ads SET status=$1, updated_at=now() WHERE id=$2', [status, id]);
      return res.json({ success:true, message:'status-updated' });
    } finally { client.release(); }
  } catch (err) { console.error('PATCH /api/ads/:id/status', err); return res.status(500).json({ success:false, message:'server-error' }); }
});
/////////////////////////////////////////////////////////////////////
// Orders: release & confirm (buyer confirm -> schedule payout)
/////////////////////////////////////////////////////////////////////
app.post('/api/orders/:id/release', async (req, res) => {
  try {
    const orderId = req.params.id;
    const client = await pool.connect();
    try {
      const or = (await client.query('SELECT * FROM orders WHERE id=$1 LIMIT 1', [orderId])).rows[0];
      if (!or) return res.status(404).json({ success:false, message:'order-not-found' });
      if (or.status !== 'paid') return res.status(400).json({ success:false, message:'order-not-paid' });

      await client.query('UPDATE orders SET status=$1, released_at=now(), release_followup_stage=0, updated_at=now() WHERE id=$2', ['released', orderId]);

      const now = new Date();
      await scheduleJobAt('order-followup', { orderId, stage:1 }, addWorkingDays(now,1));
      await scheduleJobAt('order-followup', { orderId, stage:2 }, addWorkingDays(now,2));
      await scheduleJobAt('order-followup', { orderId, stage:3 }, addWorkingDays(now,3));
      await scheduleJobAt('order-followup', { orderId, stage:4 }, addWorkingDays(now,4));

      const buyer = (await client.query('SELECT * FROM users WHERE id=$1 LIMIT 1', [or.buyer_id])).rows[0];
      const seller = (await client.query('SELECT * FROM users WHERE id=$1 LIMIT 1', [or.seller_id])).rows[0];
      if (buyer) try { await sendEmail({ to: buyer.email || buyer.username, subject: `Seller released order ${orderId}`, text: `Seller released order ${orderId}. Please confirm receipt.` }); } catch(e){ console.warn('sendEmail buyer release failed', e && e.message); }
      if (seller) try { await sendEmail({ to: seller.email || seller.username, subject: `You released order ${orderId}`, text: `You released order ${orderId}. Await buyer confirmation.` }); } catch(e){ console.warn('sendEmail seller release failed', e && e.message); }

      return res.json({ success:true, message:'order released, followups scheduled (working days)' });
    } finally { client.release(); }
  } catch (e) { console.error('/api/orders/:id/release', e); return res.status(500).json({ success:false, message:'server-error' }); }
});

app.post('/api/orders/:id/confirm', async (req, res) => {
  try {
    const orderId = req.params.id;
    const client = await pool.connect();
    try {
      const or = (await client.query('SELECT * FROM orders WHERE id=$1 LIMIT 1', [orderId])).rows[0];
      if (!or) return res.status(404).json({ success:false, message:'order-not-found' });
      if (or.status !== 'released') return res.status(400).json({ success:false, message:'order-not-released' });

      await client.query('UPDATE orders SET status=$1, buyer_confirmed_at=now(), updated_at=now() WHERE id=$2', ['completed', orderId]);

      const pay = (await client.query('SELECT * FROM payments WHERE order_id=$1 ORDER BY created_at DESC LIMIT 1', [orderId])).rows[0];

      const settlementDays = (function getSettlementDays(meta){
        if (!meta) return 2;
        try {
          const m = typeof meta === 'string' ? JSON.parse(meta) : meta;
          if (m.provider_verify && m.provider_verify.data) {
            const p = m.provider_verify.data;
            const pt = (p.payment_type || p.payment_options || p.channel || '').toString().toLowerCase();
            if (pt.includes('card')) return 6;
          }
          if (m.provider_init && m.provider_init.data) {
            const p = m.provider_init.data;
            const pt = (p.payment_type || p.payment_options || p.channel || '').toString().toLowerCase();
            if (pt.includes('card')) return 6;
          }
          return 2;
        } catch(e){ return 2; }
      })(pay ? pay.meta : null);

      const payoutDate = addWorkingDays(new Date(), settlementDays);
      await scheduleJobAt('payout-seller', { orderId }, payoutDate);

      const seller = (await client.query('SELECT * FROM users WHERE id=$1 LIMIT 1', [or.seller_id])).rows[0];
      if (seller) try { await sendEmail({ to: seller.email || seller.username, subject: `Buyer confirmed receipt for ${orderId}`, text: `Buyer confirmed. Payout scheduled in ${settlementDays} working day(s) (on ${payoutDate.toDateString()}).` }); } catch(e){ console.warn('sendEmail payout scheduled failed', e && e.message ? e.message : e); }

      return res.json({ success:true, message:`Order completed; payout scheduled in ${settlementDays} working day(s).` });
    } finally { client.release(); }
  } catch (e) { console.error('/api/orders/:id/confirm', e); return res.status(500).json({ success:false, message:'server-error' }); }
});

/////////////////////////////////////////////////////////////////////
// Small helpful endpoints & graceful shutdown
/////////////////////////////////////////////////////////////////////
app.get('/', (req,res) => res.send('TradeHive backend running'));
app.get('/health', (req,res) => res.json({ ok:true, now: new Date().toISOString() }));

const server = app.listen(PORT, ()=> console.log(`TradeHive backend listening on ${PORT}`));

async function shutdown() {
  console.log('Shutting down...');
  try {
    if (jobWorker) { await jobWorker.close(); console.log('job worker closed'); }
    if (jobQueue) { await jobQueue.close(); console.log('job queue closed'); }
    if (redis) { redis.disconnect(); console.log('redis disconnected'); }
    server.close(()=> { console.log('HTTP server closed'); process.exit(0); });
  } catch(e){ console.error('shutdown error', e); process.exit(1); }
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);