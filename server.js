'use strict';
/*
 * EVER integration layer prototype.
 *
 * One process plays four roles so the case study can be demonstrated end to end:
 *   1. Gateway          /oauth/token and /v1/*   (sign-in, rate limit, validation)
 *   2. Queue + worker   in-memory queue, retry with backoff, circuit breaker, dead-letter queue
 *   3. Transformation   SKU mapping, weighed-item barcode parsing, business-day cutoff
 *   4. Mock ERP         /sap/b1s/v1/*  (stands in for the SAP Business One Service Layer)
 * State is held in memory and resets when the process restarts.
 */
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const PORT = Number(process.env.PORT || 3000);
const SCALE = Number(process.env.RETRY_SCALE || 0.1); // 1 = real schedule, 0.1 = ten times faster for demos
const BASE_DELAYS = [5, 30, 120, 600, 1800]; // seconds: 5s, 30s, 2m, 10m, 30m
const MAX_ATTEMPTS = BASE_DELAYS.length;
const BREAKER_FAILS = 5;
const BREAKER_WINDOW_MS = 60 * 1000 * SCALE;
const BREAKER_HALF_OPEN_MS = 120 * 1000 * SCALE;
const RATE_LIMIT_PER_MIN = Number(process.env.RATE_LIMIT || 600);
const TZ_OFFSET_MS = 8 * 3600 * 1000; // Philippine time, used for the 12:00 AM business-day cutoff
const SAP_SESSION = 'sap-session-demo';
const MAX_KEPT = 6000;

const CLIENTS = {
  'agent-sucat': { secret: process.env.SECRET_SUCAT || 'sucat-demo-secret', branch: 'SUCAT' },
  'agent-zapote': { secret: process.env.SECRET_ZAPOTE || 'zapote-demo-secret', branch: 'ZAPOTE' },
  'agent-4thestate': { secret: process.env.SECRET_4TH || '4thestate-demo-secret', branch: '4TH_ESTATE' },
  'erp-admin': { secret: process.env.SECRET_ADMIN || 'erp-demo-secret', role: 'admin' },
};
const BRANCHES = ['SUCAT', 'ZAPOTE', '4TH_ESTATE'];

/* ---------- reference data ---------- */
const BASE_SKU_MAP = { 'PRK-0042': 'ITM-1042', 'MLK-0100': 'ITM-2100', 'RCE-0005': 'ITM-3005', 'SRD-0010': 'ITM-4010', 'WTR-0001': 'ITM-5001', 'OIL-0001': 'ITM-6001' };
const BASE_WEIGHED_MAP = { '000042': 'ITM-1042' };
const BASE_SKUS = ['MLK-0100', 'RCE-0005', 'SRD-0010', 'WTR-0001']; // used for random sales, so accepted mappings never leak into them
let SKU_MAP = { ...BASE_SKU_MAP };
let WEIGHED_MAP = { ...BASE_WEIGHED_MAP };

function freshErp() {
  const items = {
    'ITM-1042': { name: 'Pork liempo', uom: 'KG', pricePerKg: 450 },
    'ITM-2100': { name: 'Fresh milk 1L', uom: 'PC', price: 95 },
    'ITM-3005': { name: 'Rice 5kg', uom: 'PC', price: 310 },
    'ITM-4010': { name: 'Canned sardines', uom: 'PC', price: 28 },
    'ITM-5001': { name: 'Bottled water 500ml', uom: 'PC', price: 20 },
    'ITM-6001': { name: 'Cooking oil 1L', uom: 'PC', price: 98 },
  };
  const stock = {};
  for (const b of BRANCHES) {
    stock[b] = {};
    for (const code of Object.keys(items)) {
      stock[b][code] = code === 'ITM-1042'
        ? [{ batch: 'B-0101', expiry: '2026-11-20', qty: 40 }, { batch: 'B-0102', expiry: '2026-11-27', qty: 60 }]
        : [{ batch: 'B-STD', expiry: '2027-06-30', qty: 1000 }];
    }
  }
  return { items, stock, invoices: [], postings: [], nextDoc: 1000, mode: 'up', sessionExpired: false };
}

let erp = freshErp();
let messages = new Map(); // id -> message
let order = []; // ids, oldest first
let keyIndex = new Map(); // idempotency key -> id
let tokens = new Map();
let rate = new Map();
let recons = [];
let counters = { duplicates: 0, rejected: 0, rateLimited: 0 };
let breaker = { state: 'closed', fails: [], until: 0, probing: false };
let seq = 1000;
let priceChanges = [];

/* ---------- helpers ---------- */
const now = () => Date.now();
const iso = (t = now()) => new Date(t).toISOString();
const round2 = (n) => Math.round((n + Number.EPSILON) * 100) / 100;
const round3 = (n) => Math.round((n + Number.EPSILON) * 1000) / 1000;
const uid = () => String(++seq);
const dayAt = (ms) => new Date(ms + TZ_OFFSET_MS).toISOString().slice(0, 10);
const businessDay = (ts) => dayAt(Date.parse(ts));
const verr = (msg, review) => Object.assign(new Error(msg), { validation: true, review: review || { kind: 'invalid' } });
const MIDNIGHT_GUARD_MS = 60 * 1000; // a sale this close to 12:00 AM is held until a person picks the business day
const nearMidnight = (ms) => dayAt(ms - MIDNIGHT_GUARD_MS) !== dayAt(ms + MIDNIGHT_GUARD_MS);
const itemPrice = (it) => (it.pricePerKg !== undefined ? it.pricePerKg : it.price);
const itemLabel = (it) => it.name + (it.uom === 'KG' ? ', per kg' : '');
const dayOf = (m) => m.dayChoice || businessDay(m.payload.timestamp);

function ean13(d12) {
  let s = 0;
  for (let i = 0; i < 12; i++) s += Number(d12[i]) * (i % 2 ? 3 : 1);
  return d12 + ((10 - (s % 10)) % 10);
}
function weighedBarcode(code6, amount) {
  return ean13('2' + code6 + String(Math.round(amount * 100)).padStart(5, '0'));
}

function send(res, code, body, headers = {}) {
  const data = typeof body === 'string' ? body : JSON.stringify(body);
  res.writeHead(code, { 'Content-Type': typeof body === 'string' ? 'text/plain; charset=utf-8' : 'application/json', ...headers });
  res.end(data);
}
function readBody(req) {
  return new Promise((resolve, reject) => {
    let raw = '';
    req.on('data', (c) => { raw += c; if (raw.length > 5e6) { reject(new Error('body too large')); req.destroy(); } });
    req.on('end', () => resolve(raw));
    req.on('error', reject);
  });
}
async function readJson(req) {
  const raw = await readBody(req);
  if (!raw) return {};
  const ct = req.headers['content-type'] || '';
  if (ct.includes('application/x-www-form-urlencoded')) return Object.fromEntries(new URLSearchParams(raw));
  try { return JSON.parse(raw); } catch { throw Object.assign(new Error('Body is not valid JSON'), { status: 400 }); }
}
function log(m, event) { m.history.push({ t: iso(), event }); }

/* ---------- gateway: sign-in and rate limit ---------- */
function authenticate(req) {
  const h = req.headers.authorization || '';
  const tok = h.startsWith('Bearer ') ? h.slice(7) : '';
  const t = tokens.get(tok);
  if (!t || t.exp < now()) return null;
  return { id: t.client, ...CLIENTS[t.client] };
}
function limited(clientId) {
  const minute = Math.floor(now() / 60000);
  const r = rate.get(clientId);
  if (!r || r.minute !== minute) { rate.set(clientId, { minute, n: 1 }); return false; }
  r.n += 1;
  return r.n > RATE_LIMIT_PER_MIN;
}

/* ---------- messages ---------- */
function addMessage(type, branch, key, payload) {
  const m = { id: uid(), type, branch, key, status: 'queued', attempts: 0, nextAt: 0, error: null, createdAt: now(), postedAt: null, lastAttemptAt: null, review: null, dayChoice: null, payload, result: null, history: [] };
  log(m, 'accepted by gateway');
  messages.set(m.id, m);
  order.push(m.id);
  if (key) keyIndex.set(key, m.id);
  if (order.length > MAX_KEPT) {
    for (let i = 0; i < order.length && order.length > MAX_KEPT; i++) {
      const old = messages.get(order[i]);
      if (old && ['posted', 'duplicate', 'rejected', 'discarded'].includes(old.status)) {
        messages.delete(old.id);
        if (old.key && keyIndex.get(old.key) === old.id) keyIndex.delete(old.key);
        order.splice(i, 1); i--;
      }
    }
  }
  return m;
}
function addDuplicate(branch, key, payload) {
  const original = messages.get(keyIndex.get(key));
  const m = { id: uid(), type: 'duplicate', branch, key, status: 'duplicate', attempts: 0, nextAt: 0, error: null, createdAt: now(), postedAt: now(), lastAttemptAt: null, review: null, dayChoice: null, payload, result: { duplicateOf: original ? original.id : null }, history: [] };
  log(m, 'same idempotency key already seen, acknowledged and discarded');
  messages.set(m.id, m); order.push(m.id);
  counters.duplicates += 1;
  return m;
}

/* ---------- transformation ---------- */
function saleDay(m) {
  if (m.dayChoice) return m.dayChoice;
  const t = Date.parse(m.payload.timestamp);
  const before = dayAt(t - MIDNIGHT_GUARD_MS); const after = dayAt(t + MIDNIGHT_GUARD_MS);
  if (before !== after) throw verr('Sale time falls near midnight', { kind: 'midnight', days: [before, after] });
  return dayAt(t);
}
function suggestItem(text) {
  const words = new Set(String(text || '').toLowerCase().match(/[a-z]{3,}/g) || []);
  let best = null; let bestScore = 0;
  for (const [code, it] of Object.entries(erp.items)) {
    const score = (it.name.toLowerCase().match(/[a-z]{3,}/g) || []).filter((w) => words.has(w)).length;
    if (score > bestScore) { best = code; bestScore = score; }
  }
  if (!best) return null;
  const sku = Object.keys(SKU_MAP).find((k) => SKU_MAP[k] === best) || null;
  return { itemCode: best, itemName: erp.items[best].name, sku };
}
function saleAmount(s) {
  if (s.total !== undefined && !Number.isNaN(Number(s.total))) return round2(Number(s.total));
  let t = 0;
  for (const l of s.lines || []) {
    if (l.barcode) t += (Number(String(l.barcode).slice(7, 12)) || 0) / 100;
    else if (l.amount !== undefined) t += Number(l.amount) || 0;
    else { const it = erp.items[SKU_MAP[l.sku]]; if (it) t += round2((Number(l.qty) || 0) * itemPrice(it)); }
  }
  return round2(t);
}
function transformSale(m) {
  const s = m.payload;
  const lines = [];
  let total = 0;
  const docDate = saleDay(m);
  for (const l of s.lines) {
    if (l.barcode) {
      const b = String(l.barcode);
      if (!/^2\d{12}$/.test(b)) throw verr(`Barcode ${b} is not a weighed-item barcode`);
      if (ean13(b.slice(0, 12)) !== b) throw verr(`Barcode ${b} fails its check digit`);
      const code = WEIGHED_MAP[b.slice(1, 7)];
      if (!code) throw verr(`Unmapped weighed item code ${b.slice(1, 7)}`, { kind: 'unmapped', sku: b.slice(1, 7), weighed: true, suggestion: suggestItem(l.description) });
      const price = Number(b.slice(7, 12)) / 100;
      const perKg = erp.items[code].pricePerKg;
      const qty = round3(price / perKg);
      if (!(qty > 0)) throw verr('Weighed quantity is zero');
      lines.push({ ItemCode: code, Quantity: qty, UoM: 'KG', LineTotal: price, Source: 'weighed-barcode' });
      total += price;
    } else {
      const code = SKU_MAP[l.sku];
      if (!code) throw verr(`POS code ${l.sku} has no SKU match`, { kind: 'unmapped', sku: String(l.sku), weighed: false, suggestion: suggestItem(l.description) });
      const qty = Number(l.qty);
      if (!(qty > 0)) throw verr(`Quantity must be above zero for ${l.sku}`);
      const unit = itemPrice(erp.items[code]);
      const amount = l.amount !== undefined ? Number(l.amount) : round2(qty * unit);
      lines.push({ ItemCode: code, Quantity: qty, UoM: erp.items[code].uom, LineTotal: amount, Source: 'sku-map' });
      total += amount;
    }
  }
  total = round2(total);
  if (s.total !== undefined && Math.abs(Number(s.total) - total) > 0.01) throw verr(`POS total ${s.total} does not match line total ${total}`);
  return {
    U_TransactionKey: m.key, U_InvoiceNo: s.invoice_no, U_PosTxnNo: s.pos_transaction_no || null, U_Terminal: s.terminal,
    U_TenderType: s.tender_type, U_ApprovalCode: s.approval_code || null, U_Member: s.member_number || null,
    DocDate: docDate, BranchCode: m.branch, DocTotal: total, DocumentLines: lines,
  };
}
function transformMovement(m) {
  const s = m.payload;
  const code = SKU_MAP[s.sku];
  if (!code) throw verr(`POS code ${s.sku} has no SKU match`, { kind: 'unmapped', sku: String(s.sku), weighed: false, suggestion: suggestItem(s.description) });
  const qty = Number(s.qty);
  if (!(qty !== 0) || Number.isNaN(qty)) throw verr('Quantity must not be zero');
  return { U_TransactionKey: m.key, BranchCode: m.branch, Type: s.type, ItemCode: code, Quantity: qty, BatchNumber: s.batch || 'B-STD', ExpiryDate: s.expiry || null };
}

/* ---------- worker, retry, breaker ---------- */
async function erpCall(pathPart, method, body) {
  const res = await fetch(`http://127.0.0.1:${PORT}/sap/b1s/v1/${pathPart}`, {
    method, headers: { Authorization: 'Bearer ' + SAP_SESSION, 'Content-Type': 'application/json' },
    body: body ? JSON.stringify(body) : undefined, signal: AbortSignal.timeout(5000),
  });
  let json = null;
  try { json = await res.json(); } catch { /* empty body */ }
  return { status: res.status, json };
}
function breakerFail() {
  const t = now();
  breaker.fails = breaker.fails.filter((x) => t - x < BREAKER_WINDOW_MS);
  breaker.fails.push(t);
  if (breaker.state === 'half' || breaker.fails.length >= BREAKER_FAILS) {
    breaker.state = 'open'; breaker.until = t + BREAKER_HALF_OPEN_MS; breaker.fails = [];
  }
}
function breakerOk() { breaker.state = 'closed'; breaker.fails = []; }

function toReview(m, why, info) {
  m.status = 'review'; m.error = why; m.review = info || { kind: 'invalid' }; log(m, `needs a decision, sent to review queue: ${why}`);
}
function requeue(m, note) {
  m.status = 'queued'; m.attempts = 0; m.nextAt = 0; m.error = null; m.review = null; log(m, note);
}
function toRetryOrDlq(m, why) {
  m.attempts += 1; m.error = why; m.lastAttemptAt = now();
  if (m.attempts >= MAX_ATTEMPTS) { m.status = 'dlq'; log(m, `attempt ${m.attempts} failed (${why}), moved to dead-letter queue`); return; }
  const wait = BASE_DELAYS[m.attempts - 1] * 1000 * SCALE;
  m.status = 'retrying'; m.nextAt = now() + wait;
  log(m, `attempt ${m.attempts} failed (${why}), retry in ${round2(wait / 1000)}s`);
}

async function processMessage(m) {
  m.status = 'processing';
  let doc;
  let target;
  try {
    if (m.type === 'sale') { doc = transformSale(m); target = 'Invoices'; }
    else if (m.type === 'stock') { doc = transformMovement(m); target = 'InventoryPostings'; }
    else { m.status = 'posted'; return; }
  } catch (e) {
    if (e.validation) return toReview(m, e.message, e.review);
    return toRetryOrDlq(m, 'transformation error: ' + e.message);
  }
  log(m, 'transformed for the ERP');
  try {
    let r = await erpCall(target, 'POST', doc);
    if (r.status === 401) {
      log(m, 'ERP session expired, signing in again once');
      await erpCall('Login', 'POST', { CompanyDB: 'EVER_DEMO' });
      r = await erpCall(target, 'POST', doc);
    }
    if (r.status >= 200 && r.status < 300) {
      breakerOk();
      m.status = 'posted'; m.postedAt = now(); m.error = null; m.result = r.json;
      log(m, `posted to ERP, DocEntry ${r.json && r.json.DocEntry}`);
    } else if (r.status >= 400 && r.status < 500 && r.status !== 401) {
      toReview(m, `ERP rejected the document: ${(r.json && r.json.error) || r.status}`);
    } else {
      breakerFail();
      toRetryOrDlq(m, `ERP returned ${r.status}`);
    }
  } catch (e) {
    breakerFail();
    toRetryOrDlq(m, 'ERP unreachable: ' + (e.name === 'TimeoutError' ? 'timeout' : e.message));
  }
}

let inflight = 0;
function tick() {
  const t = now();
  if (breaker.state === 'open' && t >= breaker.until) { breaker.state = 'half'; breaker.probing = false; }
  if (breaker.state === 'open') return;
  for (const id of order) {
    if (inflight >= 20) break;
    const m = messages.get(id);
    if (!m || (m.status !== 'queued' && m.status !== 'retrying') || m.nextAt > t) continue;
    if (breaker.state === 'half') { if (breaker.probing) break; breaker.probing = true; }
    inflight += 1;
    processMessage(m).catch((e) => toRetryOrDlq(m, 'worker error: ' + e.message)).finally(() => { inflight -= 1; if (breaker.state === 'half') breaker.probing = false; });
  }
}
setInterval(tick, 100).unref();

/* ---------- mock ERP (SAP Business One Service Layer) ---------- */
function sapGuard(req, res, isLogin) {
  if (erp.mode === 'down') { send(res, 503, { error: 'Service unavailable' }); return false; }
  if (!isLogin) {
    if (req.headers.authorization !== 'Bearer ' + SAP_SESSION || erp.sessionExpired) { send(res, 401, { error: 'Session expired' }); return false; }
    if (erp.mode === 'flaky' && Math.random() < 0.5) { send(res, 503, { error: 'Temporary failure' }); return false; }
  }
  return true;
}
function deduct(branch, code, qty) {
  let left = qty;
  const batches = erp.stock[branch][code].slice().sort((a, b) => a.expiry.localeCompare(b.expiry)); // first expired, first out
  const used = [];
  for (const b of batches) {
    if (left <= 0) break;
    const take = Math.min(b.qty, left);
    if (take > 0) { b.qty = round3(b.qty - take); left = round3(left - take); used.push({ BatchNumber: b.batch, Quantity: round3(take) }); }
  }
  if (left > 0) { erp.stock[branch][code][0].qty = round3(erp.stock[branch][code][0].qty - left); used.push({ BatchNumber: 'SHORT', Quantity: left }); }
  return used;
}
async function sapRoute(req, res, url) {
  const p = url.pathname.replace('/sap/b1s/v1/', '');
  const delay = 15 + Math.random() * 30;
  await new Promise((r) => setTimeout(r, delay));
  if (p === 'Login' && req.method === 'POST') {
    if (!sapGuard(req, res, true)) return;
    erp.sessionExpired = false;
    return send(res, 200, { SessionId: SAP_SESSION });
  }
  if (!sapGuard(req, res, false)) return;
  if (p === 'Invoices' && req.method === 'POST') {
    const d = await readJson(req);
    if (!Array.isArray(d.DocumentLines) || !d.DocumentLines.length) return send(res, 400, { error: 'Document has no lines' });
    for (const l of d.DocumentLines) if (!erp.items[l.ItemCode]) return send(res, 400, { error: `Item ${l.ItemCode} not found` });
    if (!erp.stock[d.BranchCode]) return send(res, 400, { error: `Branch ${d.BranchCode} not found` });
    const doc = { ...d, DocEntry: ++erp.nextDoc, CreateTime: iso() };
    doc.DocumentLines = d.DocumentLines.map((l) => ({ ...l, BatchNumbers: deduct(d.BranchCode, l.ItemCode, l.Quantity) }));
    erp.invoices.push(doc);
    return send(res, 201, { DocEntry: doc.DocEntry, DocNum: doc.DocEntry });
  }
  if (p === 'InventoryPostings' && req.method === 'POST') {
    const d = await readJson(req);
    if (!erp.items[d.ItemCode] || !erp.stock[d.BranchCode]) return send(res, 400, { error: 'Item or branch not found' });
    const list = erp.stock[d.BranchCode][d.ItemCode];
    let b = list.find((x) => x.batch === d.BatchNumber);
    if (!b) { b = { batch: d.BatchNumber, expiry: d.ExpiryDate || '2027-06-30', qty: 0 }; list.push(b); }
    b.qty = round3(b.qty + d.Quantity);
    const doc = { ...d, DocEntry: ++erp.nextDoc };
    erp.postings.push(doc);
    return send(res, 201, { DocEntry: doc.DocEntry });
  }
  if (p === 'Invoices' && req.method === 'GET') {
    const branch = url.searchParams.get('branch'); const date = url.searchParams.get('date');
    return send(res, 200, { value: erp.invoices.filter((i) => (!branch || i.BranchCode === branch) && (!date || i.DocDate === date)) });
  }
  if (p === 'Items' && req.method === 'GET') {
    const out = Object.entries(erp.items).map(([code, it]) => ({
      ItemCode: code, ItemName: it.name, UoM: it.uom, PricePerKg: it.pricePerKg, Price: it.price,
      Stock: Object.fromEntries(BRANCHES.map((b) => [b, round3(erp.stock[b][code].reduce((s, x) => s + x.qty, 0))])),
      Batches: Object.fromEntries(BRANCHES.map((b) => [b, erp.stock[b][code].map((x) => ({ ...x }))])),
    }));
    return send(res, 200, { value: out });
  }
  send(res, 404, { error: 'Unknown Service Layer resource' });
}

/* ---------- price changes ---------- */
function applyPrice(rec) {
  const it = erp.items[rec.itemCode];
  rec.old = itemPrice(it);
  if (it.pricePerKg !== undefined) it.pricePerKg = rec.new; else it.price = rec.new;
  const m = addMessage('price', 'ALL', null, { item_code: rec.itemCode, price: rec.new, change_id: rec.id });
  m.status = 'posted'; m.postedAt = now();
  m.deliveries = Object.fromEntries(BRANCHES.map((br) => [br, { status: 'pending', ackedAt: null }]));
  log(m, 'price change approved in the ERP and queued for all branch agents');
  rec.status = 'applied'; rec.appliedAt = now(); rec.messageId = m.id;
  simulateAgentAcks(m.id);
}
setInterval(() => { for (const r of priceChanges) if (r.status === 'pending' && r.startsOn <= dayAt(now())) applyPrice(r); }, 1000).unref();

/* ---------- gateway routes ---------- */
function validSale(s) {
  if (!s || typeof s !== 'object') return 'sale must be an object';
  if (!s.terminal) return 'terminal is required';
  if (!s.invoice_no) return 'invoice_no is required';
  if (!s.timestamp || Number.isNaN(Date.parse(s.timestamp))) return 'timestamp must be an ISO date time';
  if (!s.tender_type) return 'tender_type is required';
  if (!Array.isArray(s.lines) || !s.lines.length) return 'lines must be a non-empty array';
  for (const l of s.lines) if (!l || (!l.sku && !l.barcode)) return 'every line needs a sku or a barcode';
  return null;
}
function parseCsv(text) {
  const rows = text.trim().split(/\r?\n/).map((r) => r.split(',').map((x) => x.trim()));
  const head = rows.shift().map((h) => h.toLowerCase());
  return rows.filter((r) => r.length > 1).map((r) => Object.fromEntries(head.map((h, i) => [h, r[i]])));
}

async function gateway(req, res, url) {
  if (url.pathname === '/oauth/token' && req.method === 'POST') {
    const b = await readJson(req);
    const c = CLIENTS[b.client_id];
    if (b.grant_type !== 'client_credentials' || !c || c.secret !== b.client_secret) return send(res, 401, { error: 'invalid_client' });
    const token = crypto.randomBytes(24).toString('hex');
    tokens.set(token, { client: b.client_id, exp: now() + 3600 * 1000 });
    return send(res, 200, { access_token: token, token_type: 'Bearer', expires_in: 3600 });
  }
  const client = authenticate(req);
  if (!client) return send(res, 401, { error: 'Missing or expired bearer token' });
  if (limited(client.id)) { counters.rateLimited += 1; return send(res, 429, { error: 'Rate limit exceeded' }, { 'Retry-After': '60' }); }

  if (url.pathname === '/v1/sales' && req.method === 'POST') {
    if (!client.branch) return send(res, 403, { error: 'Only branch agents can post sales' });
    const body = await readJson(req);
    const list = Array.isArray(body.sales) ? body.sales : [body];
    const results = [];
    for (const s of list) {
      const bad = validSale(s);
      if (bad) { counters.rejected += 1; results.push({ invoice_no: s && s.invoice_no, status: 'rejected', reason: bad }); continue; }
      const key = `${client.branch}|${s.terminal}|${s.invoice_no}`;
      if (keyIndex.has(key)) { const d = addDuplicate(client.branch, key, s); results.push({ invoice_no: s.invoice_no, status: 'duplicate', id: d.id }); continue; }
      const m = addMessage('sale', client.branch, key, s);
      results.push({ invoice_no: s.invoice_no, status: 'accepted', id: m.id });
    }
    return send(res, 202, { results });
  }
  if (url.pathname === '/v1/stock-movements' && req.method === 'POST') {
    if (!client.branch) return send(res, 403, { error: 'Only branch agents can post stock movements' });
    const body = await readJson(req);
    const results = [];
    for (const s of Array.isArray(body.movements) ? body.movements : []) {
      if (!s.doc_no || !s.sku || !s.type) { counters.rejected += 1; results.push({ doc_no: s.doc_no, status: 'rejected', reason: 'doc_no, sku and type are required' }); continue; }
      const key = `${client.branch}|${s.type}|${s.doc_no}`;
      if (keyIndex.has(key)) { const d = addDuplicate(client.branch, key, s); results.push({ doc_no: s.doc_no, status: 'duplicate', id: d.id }); continue; }
      results.push({ doc_no: s.doc_no, status: 'accepted', id: addMessage('stock', client.branch, key, s).id });
    }
    return send(res, 202, { results });
  }
  if (url.pathname === '/v1/prices' && req.method === 'POST') {
    if (client.role !== 'admin') return send(res, 403, { error: 'Only the ERP can publish prices' });
    const b = await readJson(req);
    const it = erp.items[b.item_code];
    if (!it || !(Number(b.price) > 0)) return send(res, 400, { error: 'item_code and a positive price are required' });
    if (b.starts_on !== undefined && !/^\d{4}-\d{2}-\d{2}$/.test(String(b.starts_on))) return send(res, 400, { error: 'starts_on must be a date like 2026-11-30 (Philippine date)' });
    const rec = { id: uid(), itemCode: b.item_code, label: itemLabel(it), old: itemPrice(it), new: Number(b.price), startsOn: b.starts_on || dayAt(now()), status: 'pending', appliedAt: null, messageId: null };
    priceChanges.push(rec);
    if (rec.startsOn <= dayAt(now())) applyPrice(rec);
    return send(res, 202, { id: rec.id, status: rec.status, message_id: rec.messageId });
  }
  if (url.pathname === '/v1/price-updates' && req.method === 'GET') {
    if (!client.branch) return send(res, 403, { error: 'Only branch agents can fetch price updates' });
    const out = [];
    for (const id of order) { const m = messages.get(id); if (m && m.type === 'price' && m.deliveries[client.branch].status === 'pending') out.push({ id: m.id, ...m.payload }); }
    return send(res, 200, { updates: out });
  }
  const ack = url.pathname.match(/^\/v1\/price-updates\/([^/]+)\/ack$/);
  if (ack && req.method === 'POST') {
    const m = messages.get(ack[1]);
    if (!client.branch || !m || m.type !== 'price') return send(res, 404, { error: 'Unknown price update' });
    m.deliveries[client.branch] = { status: 'acked', ackedAt: now() };
    log(m, `${client.branch} terminals acknowledged`);
    return send(res, 200, { ok: true });
  }
  if (url.pathname === '/v1/reconciliation' && req.method === 'POST') {
    if (!client.branch) return send(res, 403, { error: 'Only branch agents can send reconciliation files' });
    const b = await readJson(req);
    const rows = Array.isArray(b.rows) ? b.rows : (typeof b.csv === 'string' ? parseCsv(b.csv) : []);
    const date = b.business_date;
    if (!date || !rows.length) return send(res, 400, { error: 'business_date and rows (or csv) are required' });
    const erpDocs = new Map(erp.invoices.filter((i) => i.BranchCode === client.branch && i.DocDate === date).map((i) => [i.U_InvoiceNo, i]));
    let pos = 0; let erpTotal = 0; const missing = []; const mismatched = [];
    for (const r of rows) {
      const t = round2(Number(r.total)); pos += t;
      const d = erpDocs.get(r.invoice_no);
      if (!d) { missing.push(r.invoice_no); continue; }
      erpTotal += d.DocTotal;
      if (r.approval_code && d.U_ApprovalCode !== r.approval_code) mismatched.push(r.invoice_no);
    }
    pos = round2(pos); erpTotal = round2(erpTotal);
    const variance = pos === 0 ? 0 : round3(Math.abs(pos - erpTotal) / pos * 100);
    const result = { branch: client.branch, business_date: date, pos_total: pos, erp_total: erpTotal, variance_pct: variance, status: variance < 0.1 && !mismatched.length ? 'OK' : 'FLAG', missing_in_erp: missing, approval_mismatch: mismatched, at: iso() };
    recons.unshift(result); recons = recons.slice(0, 30);
    return send(res, 200, result);
  }
  send(res, 404, { error: 'Unknown gateway route' });
}

/* ---------- monitoring API ---------- */
function stats() {
  const all = order.map((id) => messages.get(id)).filter(Boolean);
  const by = {};
  for (const m of all) by[m.status] = (by[m.status] || 0) + 1;
  const posted = all.filter((m) => m.status === 'posted' && m.type !== 'price');
  const lat = posted.map((m) => m.postedAt - m.createdAt).sort((a, b) => a - b);
  const t = now();
  return {
    byStatus: by, total: all.length,
    queueDepth: (by.queued || 0) + (by.retrying || 0) + (by.processing || 0),
    postedLastMinute: posted.filter((m) => t - m.postedAt < 60000).length,
    within5MinPct: lat.length ? round2(lat.filter((x) => x <= 300000).length / lat.length * 100) : null,
    p95LatencyMs: lat.length ? lat[Math.min(lat.length - 1, Math.floor(lat.length * 0.95))] : null,
    duplicates: counters.duplicates, rejected: counters.rejected, rateLimited: counters.rateLimited,
    erpInvoices: erp.invoices.length,
  };
}
function labelOf(m) {
  const p = m.payload || {};
  const t = m.type === 'duplicate' ? (p.lines ? 'sale' : 'stock') : m.type;
  if (t === 'sale') return (p.lines || []).some((l) => l.barcode) ? 'Weighed' : 'Sale';
  return t === 'stock' ? 'Stock' : 'Price';
}
function describeLine(l) {
  if (l.barcode) {
    const b = String(l.barcode); const code = WEIGHED_MAP[b.slice(1, 7)]; const it = code && erp.items[code];
    if (!it) return l.description || `Weighed code ${b.slice(1, 7)}`;
    return `${it.name}, ${(Number(b.slice(7, 12)) / 100 / it.pricePerKg).toFixed(2)} kg`;
  }
  const it = erp.items[SKU_MAP[l.sku]];
  if (!it) return l.description || `POS code ${l.sku}`;
  return `${it.name}, ${l.qty}${it.uom === 'KG' ? ' kg' : ' pc'}`;
}
function describe(m) {
  const p = m.payload || {};
  if (m.type === 'sale' || (m.type === 'duplicate' && p.lines)) {
    const first = describeLine(p.lines[0]);
    return p.lines.length > 1 ? `${first} and ${p.lines.length - 1} more` : first;
  }
  if (m.type === 'stock' || m.type === 'duplicate') return `${p.type} ${p.doc_no}, ${p.sku} x ${p.qty}`;
  if (m.type === 'price') { const it = erp.items[p.item_code]; return `${it ? it.name : p.item_code} to ${peso(p.price)}`; }
  return '';
}
const peso = (n) => '₱' + Number(n).toLocaleString('en-US', { maximumFractionDigits: 2 });
const view = (m) => ({
  id: m.id, type: m.type, label: labelOf(m), detail: describe(m), branch: m.branch, key: m.key, status: m.status, attempts: m.attempts, error: m.error,
  createdAt: m.createdAt, postedAt: m.postedAt, nextAt: m.nextAt, lastAttemptAt: m.lastAttemptAt, review: m.review, dayChoice: m.dayChoice,
  amount: m.type === 'sale' ? saleAmount(m.payload) : null, duplicateOf: m.type === 'duplicate' && m.result ? m.result.duplicateOf : null,
});
const MSG_STATES = { queued: 'Waiting in queue', processing: 'Waiting in queue', retrying: 'Retrying', dlq: 'Dead-letter', review: 'In review queue' };
function reconciliation(branch, date) {
  const sales = order.map((id) => messages.get(id)).filter((m) => m && m.type === 'sale' && m.branch === branch && !['rejected', 'discarded'].includes(m.status));
  const invoices = erp.invoices.filter((i) => i.BranchCode === branch);
  const days = [...new Set([...sales.map(dayOf), ...invoices.map((i) => i.DocDate)])].sort().reverse();
  const day = date && days.includes(date) ? date : days[0] || dayAt(now());
  const mine = sales.filter((m) => dayOf(m) === day);
  const branchFile = round2(mine.reduce((t, m) => t + saleAmount(m.payload), 0));
  const erpPosted = round2(invoices.filter((i) => i.DocDate === day).reduce((t, i) => t + i.DocTotal, 0));
  const rows = mine.filter((m) => MSG_STATES[m.status]).map((m) => ({ id: m.id, amount: saleAmount(m.payload), reason: MSG_STATES[m.status] }));
  return { branch, day, days, branchFile, erpPosted, difference: round2(branchFile - erpPosted), rows, salesCount: mine.length };
}
function stats() {
  const all = order.map((id) => messages.get(id)).filter(Boolean);
  const by = {};
  for (const m of all) by[m.status] = (by[m.status] || 0) + 1;
  const posted = all.filter((m) => m.status === 'posted' && m.type !== 'price');
  const lat = posted.map((m) => m.postedAt - m.createdAt).sort((a, b) => a - b);
  const t = now();
  return {
    byStatus: by, total: all.length, posted: posted.length,
    queueDepth: (by.queued || 0) + (by.retrying || 0) + (by.processing || 0),
    postedLastMinute: posted.filter((m) => t - m.postedAt < 60000).length,
    within5MinPct: lat.length ? round2(lat.filter((x) => x <= 300000).length / lat.length * 100) : null,
    p95LatencyMs: lat.length ? lat[Math.min(lat.length - 1, Math.floor(lat.length * 0.95))] : null,
    duplicates: counters.duplicates, rejected: counters.rejected, rateLimited: counters.rateLimited,
    erpInvoices: erp.invoices.length,
  };
}
function erpLabel() {
  if (erp.mode === 'down') return { key: 'down', text: 'ERP down' };
  if (erp.sessionExpired) return { key: 'expired', text: 'ERP session expired' };
  if (breaker.state === 'open') return { key: 'down', text: 'Circuit open' };
  if (erp.mode === 'flaky') return { key: 'flaky', text: 'ERP flaky' };
  return { key: 'up', text: 'ERP online' };
}
function state() {
  const all = order.map((id) => messages.get(id)).filter(Boolean);
  const list = (st) => all.filter((m) => m.status === st).slice(-100).reverse().map(view);
  const prices = priceChanges.slice(-12).reverse().map((r) => {
    const m = r.messageId ? messages.get(r.messageId) : null;
    return { ...r, deliveries: m ? m.deliveries : null };
  });
  return {
    time: iso(), config: { retryScale: SCALE, retrySchedule: BASE_DELAYS, maxAttempts: MAX_ATTEMPTS, rateLimitPerMin: RATE_LIMIT_PER_MIN },
    stats: stats(), erp: { mode: erp.mode, sessionExpired: erp.sessionExpired, ...erpLabel() },
    breaker: { state: breaker.state, reopenInMs: breaker.state === 'open' ? Math.max(0, breaker.until - now()) : 0 },
    counts: { messages: all.length, dlq: all.filter((m) => m.status === 'dlq').length, review: all.filter((m) => m.status === 'review').length, prices: priceChanges.filter((r) => r.status === 'pending').length },
    items: Object.entries(erp.items).map(([code, it]) => ({ code, label: itemLabel(it), price: itemPrice(it) })), today: dayAt(now()),
    messages: all.slice(-300).reverse().map(view), dlq: list('dlq'), review: list('review'), recons: recons.slice(0, 6), prices,
    stock: Object.fromEntries(BRANCHES.map((b) => [b, round3(erp.stock[b]['ITM-1042'].reduce((s, x) => s + x.qty, 0))])),
  };
}

/* ---------- simulator (acts as the branch agents and the ERP team) ---------- */
const tokenCache = {};
async function gw(clientId, route, body, method = 'POST') {
  if (!tokenCache[clientId] || tokenCache[clientId].exp < now()) {
    const r = await fetch(`http://127.0.0.1:${PORT}/oauth/token`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ grant_type: 'client_credentials', client_id: clientId, client_secret: CLIENTS[clientId].secret }) });
    const j = await r.json();
    tokenCache[clientId] = { token: j.access_token, exp: now() + 3000 * 1000 };
  }
  const r = await fetch(`http://127.0.0.1:${PORT}${route}`, { method, headers: { Authorization: 'Bearer ' + tokenCache[clientId].token, 'Content-Type': 'application/json' }, body: body ? JSON.stringify(body) : undefined });
  return { status: r.status, json: await r.json() };
}
const AGENT = { SUCAT: 'agent-sucat', ZAPOTE: 'agent-zapote', '4TH_ESTATE': 'agent-4thestate' };
const TERMINAL = { SUCAT: 'POS18-S311', ZAPOTE: 'POS03-Z101', '4TH_ESTATE': 'POS02-F201' };
let invoiceCounter = 100000 + Math.floor(Math.random() * 800000);
const nextInv = () => '00-' + String(++invoiceCounter).padStart(6, '0');
function safeTime(ms) { return nearMidnight(ms) ? ms + 3 * MIDNIGHT_GUARD_MS : ms; } // keep ordinary demo sales away from the midnight hold
function makeSale(branch, over = {}) {
  const sku = BASE_SKUS[Math.floor(Math.random() * BASE_SKUS.length)];
  const qty = 1 + Math.floor(Math.random() * 3);
  const amount = round2(qty * itemPrice(erp.items[BASE_SKU_MAP[sku]]));
  return { terminal: TERMINAL[branch], invoice_no: nextInv(), pos_transaction_no: nextInv(), timestamp: iso(safeTime(now())), tender_type: 'Cash', lines: [{ sku, qty, amount }], ...over };
}
async function sendSales(branch, sales) { return (await gw(AGENT[branch], '/v1/sales', { sales })).json.results; }
function simulateAgentAcks(messageId) {
  // the branch agents poll for price updates and acknowledge them, as the real agents would
  for (const b of BRANCHES) setTimeout(async () => {
    try {
      const u = await gw(AGENT[b], '/v1/price-updates', null, 'GET');
      for (const x of u.json.updates) if (x.id === messageId) await gw(AGENT[b], `/v1/price-updates/${x.id}/ack`, {});
    } catch { /* demo agent */ }
  }, 800 + Math.random() * 2200);
}
const pad = (n) => String(n).padStart(2, '0');

const SCENARIOS = {
  normal: { label: 'Ordinary sales', sub: 'Five normal sales from one branch', tc: 'F1', run: async () => { const b = BRANCHES[Math.floor(Math.random() * 3)]; return { branch: b, results: await sendSales(b, Array.from({ length: 5 }, () => makeSale(b))) }; } },
  weighed: { label: 'Weighed barcode', sub: 'Pork sold by the kilo, 0.30 kg', tc: 'TC1', run: async () => {
    const barcode = weighedBarcode('000042', 135);
    return { barcode, results: await sendSales('SUCAT', [makeSale('SUCAT', { lines: [{ barcode, description: 'Pork liempo' }] })]) };
  } },
  duplicate: { label: 'Duplicate sale', sub: 'Same receipt sent three times', tc: 'TC2', run: async () => { const s = makeSale('SUCAT'); const out = []; for (let i = 0; i < 3; i++) out.push(...await sendSales('SUCAT', [s])); return { results: out }; } },
  replay: { label: 'Outage replay', sub: '500 sales sent after a 4 hour outage', tc: 'TC3', run: async (o) => {
    const n = Number(o.count) || 500; const branch = 'SUCAT'; const start = now() - 4 * 3600 * 1000; const buf = [];
    for (let i = 0; i < n; i++) buf.push(makeSale(branch, { timestamp: iso(safeTime(start + Math.floor(i * (4 * 3600 * 1000) / n))) }));
    (async () => { for (let i = 0; i < buf.length; i += 50) { await sendSales(branch, buf.slice(i, i + 50)); await new Promise((r) => setTimeout(r, 150)); } })();
    return { buffered: n, note: 'agent is replaying in batches of 50, oldest first' };
  } },
  promo: { label: 'Price change', sub: 'Pork price changes now, cooking oil changes tomorrow', tc: 'TC4', run: async () => {
    const price = 450 + Math.floor(Math.random() * 40);
    const r = await gw('erp-admin', '/v1/prices', { item_code: 'ITM-1042', price });
    const tomorrow = dayAt(now() + 24 * 3600 * 1000);
    const later = await gw('erp-admin', '/v1/prices', { item_code: 'ITM-6001', price: 98 + 4 + Math.floor(Math.random() * 6), starts_on: tomorrow });
    return { id: r.json.id, price, pending: later.json.id, startsOn: tomorrow };
  } },
  unmapped: { label: 'Unknown product code', sub: 'POS code 480012 has no SKU match', tc: 'TC5', run: async () => ({ results: await sendSales('ZAPOTE', [makeSale('ZAPOTE', { lines: [{ sku: '480012', description: 'Pork liempo slab', qty: 1 }] })]) }) },
  midnight: { label: 'Sale at midnight', sub: 'Two sales either side of 12:00 AM in Zapote', tc: 'TC6', run: async () => {
    const boundary = Date.parse(dayAt(now()) + 'T00:00:00Z') - TZ_OFFSET_MS; // 12:00 AM Philippine time today
    return { results: await sendSales('ZAPOTE', [makeSale('ZAPOTE', { timestamp: iso(boundary - 1000) }), makeSale('ZAPOTE', { timestamp: iso(boundary + 1000) })]) };
  } },
  card: { label: 'Debit labelled Credit Card', sub: 'Approval code must survive the label', tc: 'TC7', run: async () => ({ results: await sendSales('SUCAT', [makeSale('SUCAT', { tender_type: 'Credit Card', approval_code: String(100000 + Math.floor(Math.random() * 899999)) })]) }) },
  outage: { label: 'ERP outage', sub: 'Retry, then dead-letter', tc: null, run: async () => {
    erp.mode = 'down';
    const barcode = weighedBarcode('000042', 607.5); // 1.35 kg
    return { erp: 'down', results: await sendSales('SUCAT', [makeSale('SUCAT', { lines: [{ barcode, description: 'Pork belly' }] })]), note: 'Set the ERP back to Up, then retry the dead-lettered message' };
  } },
};

async function sim(req, res, url) {
  const body = await readJson(req);
  if (url.pathname === '/sim/erp') {
    if (['up', 'down', 'flaky'].includes(body.mode)) erp.mode = body.mode;
    else if (body.mode === 'expire') erp.sessionExpired = true;
    else return send(res, 400, { error: 'mode must be up, down, flaky or expire' });
    return send(res, 200, { mode: erp.mode, sessionExpired: erp.sessionExpired });
  }
  if (url.pathname === '/sim/reset') {
    erp = freshErp(); messages = new Map(); order = []; keyIndex = new Map(); recons = []; priceChanges = []; seq = 1000;
    SKU_MAP = { ...BASE_SKU_MAP }; WEIGHED_MAP = { ...BASE_WEIGHED_MAP };
    counters = { duplicates: 0, rejected: 0, rateLimited: 0 }; breaker = { state: 'closed', fails: [], until: 0, probing: false };
    return send(res, 200, { ok: true });
  }
  if (url.pathname === '/sim/recon') {
    const branch = body.branch || 'SUCAT';
    const date = body.business_date || businessDay(iso());
    const rows = erp.invoices.filter((i) => i.BranchCode === branch && i.DocDate === date).map((i) => ({ invoice_no: i.U_InvoiceNo, total: i.DocTotal, approval_code: i.U_ApprovalCode || undefined }));
    // the branch file also lists sales still waiting in the queue, so a gap shows up as variance
    for (const id of order) { const m = messages.get(id); if (m && m.type === 'sale' && m.branch === branch && businessDay(m.payload.timestamp) === date && m.status !== 'posted' && m.status !== 'duplicate') {
      try { rows.push({ invoice_no: m.payload.invoice_no, total: transformSale(m).DocTotal }); } catch { /* invalid sale has no total */ } } }
    if (!rows.length) return send(res, 400, { error: `No sales for ${branch} on ${date}` });
    return send(res, 200, (await gw(AGENT[branch], '/v1/reconciliation', { business_date: date, rows })).json);
  }
  if (url.pathname === '/sim/price') {
    const r = await gw('erp-admin', '/v1/prices', { item_code: body.item_code, price: body.price, starts_on: body.starts_on || undefined });
    return send(res, r.status, r.json);
  }
  const m = url.pathname.match(/^\/sim\/run\/([a-z]+)$/);
  if (m && SCENARIOS[m[1]]) return send(res, 200, await SCENARIOS[m[1]].run(body));
  send(res, 404, { error: 'Unknown simulator route' });
}

/* ---------- server ---------- */
const indexHtml = () => fs.readFileSync(path.join(__dirname, 'public', 'index.html'));
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');
  try {
    if (req.method === 'GET' && (url.pathname === '/' || url.pathname === '/index.html')) { res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' }); return res.end(indexHtml()); }
    if (url.pathname === '/health') return send(res, 200, { ok: true });
    if (url.pathname === '/api/state' && req.method === 'GET') return send(res, 200, state());
    if (url.pathname === '/api/scenarios' && req.method === 'GET') return send(res, 200, Object.entries(SCENARIOS).map(([id, s]) => ({ id, label: s.label, sub: s.sub, tc: s.tc })));
    if (url.pathname === '/api/reconciliation' && req.method === 'GET') return send(res, 200, reconciliation(url.searchParams.get('branch') || 'SUCAT', url.searchParams.get('date')));
    if (url.pathname === '/api/dlq/retry-all' && req.method === 'POST') {
      let n = 0; for (const id of order) { const m = messages.get(id); if (m && m.status === 'dlq') { requeue(m, 'manual replay requested (retry all)'); n += 1; } }
      return send(res, 200, { retried: n });
    }
    const act = url.pathname.match(/^\/api\/messages\/([^/]+)\/(replay|decide|discard)$/);
    if (act && req.method === 'POST') {
      const m = messages.get(act[1]);
      if (!m) return send(res, 404, { error: 'Unknown message' });
      if (act[2] === 'replay') {
        if (m.status !== 'dlq' && m.status !== 'review') return send(res, 404, { error: 'Only dead-letter or review messages can be replayed' });
        requeue(m, 'manual replay requested'); return send(res, 200, { ok: true });
      }
      if (act[2] === 'discard') {
        if (m.status !== 'dlq') return send(res, 409, { error: 'Only dead-letter messages can be discarded' });
        m.status = 'discarded'; log(m, 'discarded by an operator'); return send(res, 200, { ok: true });
      }
      const body = await readJson(req);
      if (m.status !== 'review') return send(res, 409, { error: 'Message is not waiting for a decision' });
      if (body.action === 'reject') { m.status = 'rejected'; log(m, 'rejected by an operator'); return send(res, 200, { ok: true }); }
      if (body.action === 'accept-mapping') {
        const r = m.review;
        if (!r || r.kind !== 'unmapped' || !r.suggestion) return send(res, 400, { error: 'This message has no suggested mapping' });
        if (r.weighed) WEIGHED_MAP[r.sku] = r.suggestion.itemCode; else SKU_MAP[r.sku] = r.suggestion.itemCode;
        let n = 0;
        for (const id of order) {
          const o = messages.get(id);
          if (o && o.status === 'review' && o.review && o.review.kind === 'unmapped' && o.review.sku === r.sku && o.review.weighed === r.weighed) { requeue(o, `mapping accepted: ${r.sku} is ${r.suggestion.itemName}`); n += 1; }
        }
        return send(res, 200, { ok: true, requeued: n });
      }
      if (body.action === 'use-day') {
        if (!m.review || m.review.kind !== 'midnight' || !m.review.days.includes(body.day)) return send(res, 400, { error: 'day must be one of the two offered days' });
        m.dayChoice = body.day; requeue(m, `business day set to ${body.day} by an operator`); return send(res, 200, { ok: true });
      }
      return send(res, 400, { error: 'action must be accept-mapping, reject or use-day' });
    }
    const one = url.pathname.match(/^\/api\/messages\/([^/]+)$/);
    if (one && req.method === 'GET') { const m = messages.get(one[1]); return m ? send(res, 200, { ...view(m), payload: m.payload, result: m.result, history: m.history }) : send(res, 404, { error: 'Unknown message' }); }
    if (url.pathname.startsWith('/sap/b1s/v1/')) return await sapRoute(req, res, url);
    if (url.pathname.startsWith('/sim/')) return await sim(req, res, url);
    if (url.pathname === '/oauth/token' || url.pathname.startsWith('/v1/')) return await gateway(req, res, url);
    send(res, 404, { error: 'Not found' });
  } catch (e) {
    send(res, e.status || 500, { error: e.message });
  }
});

if (require.main === module) server.listen(PORT, () => console.log(`Integration layer listening on ${PORT}, retry scale ${SCALE}`));
module.exports = { server, weighedBarcode };
