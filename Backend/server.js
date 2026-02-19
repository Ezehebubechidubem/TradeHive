// server.js
// TradeHive backend with working-days scheduling, SendGrid, Cloudinary-authenticated uploads.
// Full updated file (includes new endpoints: user online toggle, bank save, my ads, ad status patch)
// Keep environment variables: DATABASE_URL, REDIS_URL, PAYSTACK_SECRET_KEY, FLUTTERWAVE_SECRET_KEY, SENDGRID_API_KEY, SENDGRID_FROM, ADMIN_EMAIL, ADMIN_API_KEY, FRONTEND_BASE, etc.

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
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const bcrypt = require('bcryptjs');

let stripeLib = null;
if (process.env.STRIPE_SECRET_KEY) {
  try { stripeLib = require('stripe')(process.env.STRIPE_SECRET_KEY); } catch(e){ stripeLib = null; }
}

// SendGrid
let sendgrid = null;
if (process.env.SENDGRID_API_KEY) {
  try {
    sendgrid = require('@sendgrid/mail');
    sendgrid.setApiKey(process.env.SENDGRID_API_KEY);
    console.log('SendGrid configured.');
  } catch (e) {
    console.warn('SendGrid package error', e);
    sendgrid = null;
  }
}

// Cloudinary (optional)
let cloudinary = null;
let CloudinaryStorage = null;
let uploadCloud = null;
try {
  if (process.env.CLOUDINARY_CLOUD_NAME && process.env.CLOUDINARY_API_KEY && process.env.CLOUDINARY_API_SECRET) {
    cloudinary = require('cloudinary').v2;
    CloudinaryStorage = require('multer-storage-cloudinary').CloudinaryStorage;
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
        // Make uploads authenticated/private
        return {
          folder: isVideo ? 'tradehive/kyc/videos' : 'tradehive/kyc/images',
          resource_type: isVideo ? 'video' : 'image',
          type: 'authenticated',               // <-- private/authenticated
          public_id: `${Date.now()}-${file.originalname.replace(/\s+/g, '_')}`,
          overwrite: false,
        };
      }
    });
    uploadCloud = multer({ storage: cloudStorage, limits: { fileSize: 200 * 1024 * 1024 } });
    console.log('Cloudinary configured (authenticated uploads).');
  } else {
    console.log('Cloudinary not configured: using disk fallback.');
  }
} catch (err) {
  console.error('Cloudinary setup error:', err && err.message ? err.message : err);
  uploadCloud = null;
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
app.use(express.json({ limit: '18mb' })); // allow base64 payloads

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
// Postgres pool & simple migrations
/////////////////////////////////////////////////////////////////////
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

const initSql = `
CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, role TEXT NOT NULL, email TEXT NOT NULL UNIQUE, phone TEXT NOT NULL UNIQUE, fullname TEXT NOT NULL, username TEXT NOT NULL UNIQUE, state TEXT, lga TEXT, city TEXT, gender TEXT, specializations TEXT[], password_hash TEXT, kyc_status TEXT DEFAULT 'Unverified', avatar_url TEXT, profile_complete boolean DEFAULT false, account_details JSONB, online boolean DEFAULT false, created_at TIMESTAMP WITH TIME ZONE DEFAULT now());
CREATE TABLE IF NOT EXISTS ads (id TEXT PRIMARY KEY, seller_id TEXT NOT NULL REFERENCES users(id), title TEXT NOT NULL, description TEXT, images TEXT[], price NUMERIC NOT NULL, currency TEXT DEFAULT 'NGN', quantity INTEGER DEFAULT 1, location TEXT, category TEXT, subcategory TEXT, status TEXT DEFAULT 'draft', kyc_required boolean DEFAULT false, created_at TIMESTAMP WITH TIME ZONE DEFAULT now(), updated_at TIMESTAMP WITH TIME ZONE DEFAULT now());
CREATE TABLE IF NOT EXISTS orders (id TEXT PRIMARY KEY, ad_id TEXT NOT NULL REFERENCES ads(id), buyer_id TEXT NOT NULL REFERENCES users(id), seller_id TEXT NOT NULL REFERENCES users(id), qty INTEGER DEFAULT 1, amount NUMERIC NOT NULL, currency TEXT DEFAULT 'NGN', status TEXT DEFAULT 'pending_payment', created_at TIMESTAMP WITH TIME ZONE DEFAULT now(), updated_at TIMESTAMP WITH TIME ZONE DEFAULT now(), released_at TIMESTAMP WITH TIME ZONE, buyer_confirmed_at TIMESTAMP WITH TIME ZONE, release_followup_stage INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS payments (id TEXT PRIMARY KEY, order_id TEXT REFERENCES orders(id), ad_id TEXT REFERENCES ads(id), user_id TEXT, provider TEXT, reference TEXT, amount NUMERIC, currency TEXT, status TEXT DEFAULT 'initiated', meta JSONB, created_at TIMESTAMP WITH TIME ZONE DEFAULT now(), payout_status TEXT DEFAULT 'none', payout_reference TEXT, payout_response JSONB);
CREATE TABLE IF NOT EXISTS kyc_requests (id TEXT PRIMARY KEY, user_id TEXT REFERENCES users(id), id_type TEXT, id_name TEXT, id_number TEXT, id_images TEXT[], work_videos TEXT[], selfie TEXT, status TEXT DEFAULT 'pending', admin_note TEXT, submitted_at TIMESTAMP WITH TIME ZONE DEFAULT now());
CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, name TEXT, data JSONB, state TEXT, created_at TIMESTAMP WITH TIME ZONE DEFAULT now());
`;
(async ()=>{
  try { await pool.query(initSql); console.log('DB initialized'); }
  catch(e){ console.error('DB init error', e); process.exit(1); }
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
// Disk fallback storage
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
// Send email helper (SendGrid)
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
    console.log(`[email stub] to=${to} subject=${subject}\n${text || html}`);
    return { ok:true, stub:true };
  }
}

/////////////////////////////////////////////////////////////////////
// Working-days helper (skip Saturday/Sunday)
/////////////////////////////////////////////////////////////////////
function isWeekend(d) {
  const day = d.getDay();
  return day === 0 || day === 6; // Sunday=0, Saturday=6
}
function addWorkingDays(date, days) {
  // date: Date or timestamp
  let d = (date instanceof Date) ? new Date(date) : new Date(date);
  let added = 0;
  while (added < days) {
    d.setDate(d.getDate() + 1);
    if (!isWeekend(d)) added++;
  }
  // result: same timezone
  return d;
}
function msUntil(date) {
  const now = Date.now();
  const t = (date instanceof Date) ? date.getTime() : new Date(date).getTime();
  return Math.max(0, t - now);
}

/////////////////////////////////////////////////////////////////////
// Schedule job helper: uses jobQueue if present, otherwise setTimeout fallback
/////////////////////////////////////////////////////////////////////
async function scheduleJobAt(name, data, targetDate, opts = {}) {
  if (!targetDate) targetDate = new Date();
  const delay = msUntil(targetDate);
  if (jobQueue) {
    return await jobQueue.add(name, data, { delay, attempts: opts.attempts || 3, removeOnComplete: true });
  } else {
    // Fallback: in-process timer (not durable)
    console.warn('Using in-process scheduling (no Redis). This is ephemeral and will not survive restart.');
    setTimeout(async () => {
      try {
        await jobHandler(name, data);
      } catch(e) { console.error('in-process job handler error', e); }
    }, delay);
    return { ok:true, fallback:true, scheduledAt: targetDate };
  }
}

/////////////////////////////////////////////////////////////////////
// Job handler (used by Worker or in-process fallback)
/////////////////////////////////////////////////////////////////////
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
      // call payoutSeller implementation (reuses provider-specific logic)
      const payoutRes = await payoutSeller({ seller, amount: order.amount, currency: order.currency || 'NGN', metadata: { orderId } });
      if (payoutRes && payoutRes.ok) {
        await client.query('UPDATE payments SET payout_status=$1, payout_reference=$2, payout_response=$3 WHERE order_id=$4', ['paid', payoutRes.reference || null, JSON.stringify(payoutRes.raw || {}), orderId]);
        await sendEmail({ to: seller.email || seller.username, subject: `Payout processed for order ${orderId}`, text: `Payout processed. Reference: ${payoutRes.reference}` });
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
      // stages:
      // 1 => reminder day 1
      // 2 => reminder day 2
      // 3 => final warning
      // 4 => auto-resolve: mark completed & schedule payout immediately
      if (stage === 1 || stage === 2) {
        await sendEmail({ to: order.buyer_id, subject: `Reminder: confirm receipt for ${orderId}`, text: `Please confirm receipt for order ${orderId}.` });
        await sendEmail({ to: order.seller_id, subject: `Reminder sent to buyer for ${orderId}`, text: `A reminder was sent to the buyer.` });
        await client.query('UPDATE orders SET release_followup_stage = $1 WHERE id=$2', [stage, orderId]);
        return { ok:true };
      }
      if (stage === 3) {
        await sendEmail({ to: order.buyer_id, subject: `Final reminder: confirm receipt for ${orderId}`, text: `Final reminder before auto-release.` });
        await client.query('UPDATE orders SET release_followup_stage = $1 WHERE id=$2', [stage, orderId]);
        return { ok:true };
      }
      if (stage === 4) {
        // auto-resolve: mark completed, schedule payout now
        await client.query('UPDATE orders SET status=$1, updated_at=now() WHERE id=$2', ['completed', orderId]);
        // schedule payout immediately
        await scheduleJobAt('payout-seller', { orderId }, new Date());
        await sendEmail({ to: order.seller_id, subject: `Auto-release: funds released for ${orderId}`, text: `Funds are being released (auto-resolve).` });
        await sendEmail({ to: order.buyer_id, subject: `Order auto-resolved for ${orderId}`, text: `The order was auto-resolved and funds released to seller.` });
        return { ok:true };
      }
      return { ok:true };
    } finally { client.release(); }
  }
  return { ok:true };
}

/////////////////////////////////////////////////////////////////////
// Worker initialization if jobQueue available
/////////////////////////////////////////////////////////////////////
if (jobQueue) {
  jobWorker = new Worker('tradehive-jobs', async job => {
    return await jobHandler(job.name, job.data || job.data);
  }, { connection: redis });

  jobWorker.on('completed', job => console.log('Job completed', job.name, job.id));
  jobWorker.on('failed', (job, err) => console.error('Job failed', job.name, err && err.message));
}

/////////////////////////////////////////////////////////////////////
// Payment provider helpers (initializePayment & payoutSeller)
/////////////////////////////////////////////////////////////////////
const PAYMENT_PROVIDER = (process.env.PAYMENT_PROVIDER || 'paystack').toLowerCase();

async function initializePayment({ provider = PAYMENT_PROVIDER, email, amount, currency = 'NGN', metadata = {}, callback_url }) {
  if (provider === 'paystack') {
    const key = process.env.PAYSTACK_SECRET_KEY;
    if (!key) throw new Error('PAYSTACK_SECRET_KEY not set');
    const res = await fetch('https://api.paystack.co/transaction/initialize', {
      method: 'POST',
      headers: { Authorization: `Bearer ${key}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ email, amount: Math.round(Number(amount)*100), callback_url: callback_url || (process.env.CALLBACK_BASE_URL || '') + '/pay/callback', metadata })
    });
    return await res.json();
  }
  if (provider === 'flutterwave') {
    const key = process.env.FLUTTERWAVE_SECRET_KEY;
    if (!key) throw new Error('FLUTTERWAVE_SECRET_KEY not set');
    const tx_ref = metadata.tx_ref || ('th_' + uid());
    const requestBody = { tx_ref, amount: String(amount), currency, redirect_url: callback_url || (process.env.CALLBACK_BASE_URL || '') + '/pay/callback', customer: { email: metadata.email || email, phonenumber: metadata.phone || '' }, meta: metadata };
    const res = await fetch('https://api.flutterwave.com/v3/payments', { method: 'POST', headers: { Authorization: `Bearer ${key}`, 'Content-Type':'application/json' }, body: JSON.stringify(requestBody) });
    return await res.json();
  }
  if (provider === 'stripe') {
    if (!stripeLib) throw new Error('Stripe not configured');
    const session = await stripeLib.checkout.sessions.create({
      payment_method_types: ['card'],
      mode: 'payment',
      line_items: [{ price_data: { currency, product_data: { name: metadata.title || 'TradeHive' }, unit_amount: Math.round(Number(amount)*100) }, quantity: 1 }],
      success_url: (process.env.CALLBACK_BASE_URL || '') + '/pay/success?session_id={CHECKOUT_SESSION_ID}',
      cancel_url: (process.env.CALLBACK_BASE_URL || '') + '/pay/cancel'
    });
    return { ok:true, url: session.url, session };
  }
  return { ok:true, url: callback_url || '/', message:'provider-not-configured' };
}

// verify paystack webhook signature (for incoming webhooks)
function verifyPaystackSignature(req) {
  const secret = process.env.PAYSTACK_SECRET_KEY;
  if (!secret) return true;
  const sig = req.headers['x-paystack-signature'];
  const payload = JSON.stringify(req.body || {});
  const hash = crypto.createHmac('sha512', secret).update(payload).digest('hex');
  return hash === sig;
}

// payoutSeller: uses Paystack / Flutterwave / Stripe similar to your previous code
async function payoutSeller({ seller, amount, currency='NGN', metadata={} }) {
  // seller.account_details expected { account_number, bank_code, name, recipient_code, beneficiary_id, connect_account_id }
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
// Utility: cloudinary signed URL for authenticated uploads
/////////////////////////////////////////////////////////////////////
function cloudinarySignedUrl(public_id, opts = {}) {
  if (!cloudinary || !public_id) return null;
  // use cloudinary.url with sign_url=true and type:'authenticated'
  try {
    return cloudinary.url(public_id, { sign_url: true, secure: true, type: 'authenticated', resource_type: opts.resource_type || 'image', ...opts });
  } catch(e) {
    console.error('cloudinarySignedUrl error', e && e.message ? e.message : e);
    return null;
  }
}

/////////////////////////////////////////////////////////////////////
// KYC endpoints: submit & status
/////////////////////////////////////////////////////////////////////
const uploadHandler = uploadCloud || uploadDisk;

app.post('/api/kyc/submit', uploadHandler.fields([
  { name: 'id_images', maxCount: 6 },
  { name: 'work_videos', maxCount: 2 },
  { name: 'selfie', maxCount: 1 }
]), async (req, res) => {
  try {
    const body = req.body || {};
    const userId = body.userId || body.user_id;
    if (!userId) return res.status(400).json({ success:false, message:'userId required' });
