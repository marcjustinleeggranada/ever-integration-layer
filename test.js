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
      await run('replay', { count: 500 });
      await waitFor(async () => (await invoices()).length === 500, '500 invoices', 30000);
      const list = await invoices();
      assert.strictEqual(new Set(list.map((i) => i.U_InvoiceNo)).size, 500);
      const s = (await state()).stats;
      assert.strictEqual(s.within5MinPct, 100);
    });

    await check('TC4 a price change reaches all 3 pilot branches, and a future-dated change stays pending', async () => {
      await reset();
      await run('promo');
      await waitFor(async () => { const p = (await state()).prices.find((x) => x.status === 'applied'); return p && Object.values(p.deliveries).every((d) => d.status === 'acked'); }, 'acks', 10000);
      const prices = (await state()).prices;
      assert.strictEqual(prices.filter((p) => p.status === 'pending').length, 1);
      assert.strictEqual(prices.find((p) => p.status === 'pending').itemCode, 'ITM-6001');
      assert.strictEqual((await state()).counts.prices, 1);
    });

    await check('TC5 an unknown POS code waits in the review queue with a suggestion; accepting the mapping posts it', async () => {
      await reset();
      await run('unmapped');
      await waitFor(async () => (await state()).review.length === 1, 'review item');
      assert.strictEqual((await invoices()).length, 0);
      const item = (await state()).review[0];
      assert.strictEqual(item.review.kind, 'unmapped');
      assert.strictEqual(item.review.sku, '480012');
      assert.strictEqual(item.review.suggestion.itemCode, 'ITM-1042');
      assert.match(item.id, /^\d+$/);
      const r = await api(`/api/messages/${item.id}/decide`, { method: 'POST', body: { action: 'accept-mapping' } });
      assert.strictEqual(r.status, 200);
      await waitFor(async () => (await invoices()).length === 1, 'posted after mapping');
      assert.strictEqual((await invoices())[0].DocumentLines[0].ItemCode, 'ITM-1042');
      // the mapping is kept: a second sale with the same code now goes straight through
      await run('unmapped');
      await waitFor(async () => (await invoices()).length === 2, 'second sale posts on its own');
      assert.strictEqual((await state()).review.length, 0);
    });

    await check('TC5 rejecting an unknown POS code ends it without posting', async () => {
      await reset();
      await run('unmapped');
      await waitFor(async () => (await state()).review.length === 1, 'review item');
      const id = (await state()).review[0].id;
      await api(`/api/messages/${id}/decide`, { method: 'POST', body: { action: 'reject' } });
      await sleep(300);
      const s = await state();
      assert.strictEqual(s.review.length, 0);
      assert.strictEqual(s.messages.find((m) => m.id === id).status, 'rejected');
      assert.strictEqual((await invoices()).length, 0);
    });

    await check('TC6 sales either side of 12:00 AM at Zapote are held until a person picks the business day', async () => {
      await reset();
      await run('midnight');
      await waitFor(async () => (await state()).review.length === 2, 'two held sales');
      assert.strictEqual((await invoices()).length, 0);
      const held = (await state()).review;
      assert.ok(held.every((m) => m.review.kind === 'midnight' && m.review.days.length === 2));
      const [d1, d2] = held[0].review.days;
      await api(`/api/messages/${held[0].id}/decide`, { method: 'POST', body: { action: 'use-day', day: d1 } });
      await api(`/api/messages/${held[1].id}/decide`, { method: 'POST', body: { action: 'use-day', day: d2 } });
      await waitFor(async () => (await invoices()).length === 2, 'two invoices');
      assert.deepStrictEqual((await invoices()).map((i) => i.DocDate).sort(), [d1, d2].sort());
      const bad = await api(`/api/messages/${held[0].id}/decide`, { method: 'POST', body: { action: 'use-day', day: d1 } });
      assert.strictEqual(bad.status, 409);
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
      await run('outage');
      await waitFor(async () => (await state()).dlq.length === 1, 'dead-letter item', 40000);
      const item = (await state()).dlq[0];
      assert.strictEqual(item.attempts, 5);
      assert.strictEqual(item.label, 'Weighed');
      assert.strictEqual((await state()).counts.dlq, 1);
      await erpMode('up');
      await api('/api/dlq/retry-all', { method: 'POST', body: {} });
      await waitFor(async () => (await invoices()).length === 1, 'replayed invoice', 15000);
      assert.strictEqual((await state()).dlq.length, 0);
    });

    await check('Dead-letter queue: a parked message can be discarded', async () => {
      await reset();
      await run('outage');
      await waitFor(async () => (await state()).dlq.length === 1, 'dead-letter item', 40000);
      const id = (await state()).dlq[0].id;
      assert.strictEqual((await api(`/api/messages/${id}/discard`, { method: 'POST', body: {} })).status, 200);
      const s = await state();
      assert.strictEqual(s.dlq.length, 0);
      assert.strictEqual(s.messages.find((m) => m.id === id).status, 'discarded');
      await erpMode('up');
    });

    await check('Reconciliation breakdown lists sales still waiting, and the totals agree once they post', async () => {
      await reset();
      await erpMode('down');
      await run('normal');
      await sleep(300);
      const b = (await state()).messages[0].branch;
      const r = (await api(`/api/reconciliation?branch=${b}`)).json;
      assert.strictEqual(r.rows.length, 5);
      assert.ok(r.branchFile > 0 && r.erpPosted === 0 && r.difference === r.branchFile);
      assert.ok(r.rows.every((x) => x.reason === 'Waiting in queue' || x.reason === 'Retrying'));
      await erpMode('up');
      await waitFor(async () => (await state()).stats.byStatus.posted === 5, 'all posted', 20000);
      const r2 = (await api(`/api/reconciliation?branch=${b}`)).json;
      assert.strictEqual(r2.difference, 0);
      assert.strictEqual(r2.rows.length, 0);
    });
  } finally {
    child.kill();
  }
  const failed = results.filter((r) => !r[1]).length;
  console.log(`\n${results.length - failed} of ${results.length} checks passed`);
  process.exit(failed ? 1 : 0);
})();
