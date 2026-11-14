'use strict';
// End-to-end checks for the test cases in the midterm document. Starts the service on its own port.
const { spawn } = require('child_process');
const assert = require('assert');

const PORT = 3111;
const BASE = `http://127.0.0.1:${PORT}`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const results = [];

async function api(route, opts = {}) {
  const r = await fetch(BASE + route, { ...opts, headers: { 'Content-Type': 'application/json', ...(opts.headers || {}) }, body: opts.body ? JSON.stringify(opts.body) : undefined });
  return { status: r.status, json: await r.json() };
}
const run = (name, body) => api(`/sim/run/${name}`, { method: 'POST', body: body || {} });
const state = async () => (await api('/api/state')).json;
const invoices = async (q = '') => (await api('/sap/b1s/v1/Invoices' + q, { headers: { Authorization: 'Bearer sap-session-demo' } })).json.value;
async function waitFor(fn, label, ms = 20000) {
  const t = Date.now();
  while (Date.now() - t < ms) { if (await fn()) return; await sleep(100); }
  throw new Error('timed out waiting for ' + label);
}
async function check(name, fn) {
  try { await fn(); results.push([name, true]); console.log('PASS', name); }
  catch (e) { results.push([name, false]); console.log('FAIL', name, '\n    ', e.message); }
}
const reset = () => api('/sim/reset', { method: 'POST', body: {} });
const erpMode = (mode) => api('/sim/erp', { method: 'POST', body: { mode } });

(async () => {
  const child = spawn('node', ['server.js'], { env: { ...process.env, PORT: String(PORT), RETRY_SCALE: '0.01' }, stdio: 'inherit' });
  try {
    await waitFor(async () => { try { return (await fetch(BASE + '/health')).ok; } catch { return false; } }, 'server start', 5000);

    await check('Gateway rejects a call with no token and a wrong secret', async () => {
      assert.strictEqual((await api('/v1/sales', { method: 'POST', body: {} })).status, 401);
      const bad = await api('/oauth/token', { method: 'POST', body: { grant_type: 'client_credentials', client_id: 'agent-sucat', client_secret: 'nope' } });
      assert.strictEqual(bad.status, 401);
    });

    await check('Gateway rejects a malformed sale', async () => {
      await reset();
      const tok = (await api('/oauth/token', { method: 'POST', body: { grant_type: 'client_credentials', client_id: 'agent-sucat', client_secret: 'sucat-demo-secret' } })).json.access_token;
      const r = await api('/v1/sales', { method: 'POST', headers: { Authorization: 'Bearer ' + tok }, body: { sales: [{ invoice_no: '00-000001' }] } });
      assert.strictEqual(r.json.results[0].status, 'rejected');
    });

    await check('TC1 weighed pork: barcode price 135.00 at 450.00 per kilo deducts 0.30 kg from the earliest batch', async () => {
      await reset();
      await run('weighed');
      await waitFor(async () => (await invoices()).length === 1, 'invoice');
      const doc = (await invoices())[0];
      assert.strictEqual(doc.DocumentLines[0].Quantity, 0.3);
      assert.strictEqual(doc.DocumentLines[0].BatchNumbers[0].BatchNumber, 'B-0101');
      assert.strictEqual(doc.DocTotal, 135);
      assert.strictEqual((await state()).stock.SUCAT, 99.7);
    });

    await check('TC2 resend the same invoice 3 times: posts once, 2 duplicates acknowledged', async () => {
      await reset();
      const r = await run('duplicate');
      assert.deepStrictEqual(r.json.results.map((x) => x.status), ['accepted', 'duplicate', 'duplicate']);
      await waitFor(async () => (await invoices()).length === 1, 'invoice');
      await sleep(600);
      assert.strictEqual((await invoices()).length, 1);
      assert.strictEqual((await state()).stats.duplicates, 2);
    });

    await check('TC3 500 sales buffered during an outage all arrive once after reconnect, within 5 minutes', async () => {
      await reset();
      await run('outage', { count: 500 });
      await waitFor(async () => (await invoices()).length === 500, '500 invoices', 30000);
      const list = await invoices();
      assert.strictEqual(new Set(list.map((i) => i.U_InvoiceNo)).size, 500);
      const s = (await state()).stats;
      assert.strictEqual(s.within5MinPct, 100);
    });

    await check('TC4 a price change reaches all 3 pilot branches', async () => {
      await reset();
      await run('promo');
      await waitFor(async () => { const p = (await state()).prices[0]; return p && Object.values(p.deliveries).every((d) => d.status === 'acked'); }, 'acks', 10000);
    });

    await check('TC5 an unmapped SKU lands in the review queue, not the ERP', async () => {
      await reset();
      await run('unmapped');
      await waitFor(async () => (await state()).review.length === 1, 'review item');
      assert.strictEqual((await invoices()).length, 0);
      assert.match((await state()).review[0].error, /Unmapped SKU ZZZ-9999/);
    });

    await check('TC6 sales across 12:00 AM at Zapote land on the right business day', async () => {
      await reset();
      await run('midnight');
      await waitFor(async () => (await invoices()).length === 2, 'two invoices');
      const days = (await invoices()).map((i) => i.DocDate).sort();
      assert.deepStrictEqual(days, ['2026-11-15', '2026-11-16']);
    });

    await check('TC7 a debit sale labelled "Credit Card" keeps its approval code, and reconciliation matches on it', async () => {
      await reset();
      await run('card');
      await waitFor(async () => (await invoices()).length === 1, 'invoice');
      const doc = (await invoices())[0];
      assert.strictEqual(doc.U_TenderType, 'Credit Card');
      assert.match(doc.U_ApprovalCode, /^\d{6}$/);
      const ok = (await api('/sim/recon', { method: 'POST', body: { branch: 'SUCAT' } })).json;
      assert.strictEqual(ok.status, 'OK');
      const tok = (await api('/oauth/token', { method: 'POST', body: { grant_type: 'client_credentials', client_id: 'agent-sucat', client_secret: 'sucat-demo-secret' } })).json.access_token;
      const bad = await api('/v1/reconciliation', { method: 'POST', headers: { Authorization: 'Bearer ' + tok }, body: { business_date: doc.DocDate, rows: [{ invoice_no: doc.U_InvoiceNo, total: doc.DocTotal, approval_code: '000000' }] } });
      assert.strictEqual(bad.json.status, 'FLAG');
    });

    await check('Reconciliation flags a sale that is still missing from the ERP', async () => {
      await reset();
      await erpMode('down');
      await run('normal');
      await sleep(300);
      const b = (await state()).messages[0].branch;
      const r = (await api('/sim/recon', { method: 'POST', body: { branch: b } })).json;
      assert.strictEqual(r.status, 'FLAG');
      assert.ok(r.missing_in_erp.length >= 1 && r.variance_pct > 0.1);
      await erpMode('up');
    });

    await check('Expired ERP session: the layer signs in again once and the sale posts', async () => {
      await reset();
      await api('/sim/erp', { method: 'POST', body: { mode: 'expire' } });
      await run('weighed');
      await waitFor(async () => (await state()).stats.byStatus.posted === 1, 'posted sale');
      assert.strictEqual((await state()).erp.sessionExpired, false);
      assert.strictEqual((await invoices()).length, 1);
    });

    await check('ERP outage: retries back off, the circuit breaker opens, and everything posts after recovery', async () => {
      await reset();
      await erpMode('down');
      for (let i = 0; i < 3; i++) await run('normal'); // 15 sales
      await waitFor(async () => (await state()).breaker.state === 'open', 'breaker open', 5000);
      const mid = await state();
      assert.ok(mid.messages.some((m) => m.attempts >= 1));
      await erpMode('up');
      await waitFor(async () => (await state()).stats.byStatus.posted === 15, 'all 15 posted', 20000);
      assert.strictEqual((await state()).breaker.state, 'closed');
    });

    await check('Dead-letter queue: after 5 failed attempts a sale is parked, then replays after the fix', async () => {
      await reset();
      await erpMode('down');
      await run('weighed');
      await waitFor(async () => (await state()).dlq.length === 1, 'dead-letter item', 40000);
      const item = (await state()).dlq[0];
      assert.strictEqual(item.attempts, 5);
      await erpMode('up');
      await api(`/api/messages/${item.id}/replay`, { method: 'POST', body: {} });
      await waitFor(async () => (await invoices()).length === 1, 'replayed invoice', 15000);
    });
  } finally {
    child.kill();
  }
  const failed = results.filter((r) => !r[1]).length;
  console.log(`\n${results.length - failed} of ${results.length} checks passed`);
  process.exit(failed ? 1 : 0);
})();
