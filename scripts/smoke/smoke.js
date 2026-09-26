'use strict';

// End-to-end smoke test. Proves one payment travels through every service.
//
//   cd scripts/smoke && npm install
//   node smoke.js
//
// Settings (environment variables, defaults are for docker compose on this machine):
//   GATEWAY_URL  http://localhost:8080
//   WS_URL       ws://localhost:8084
//   OPS_URLS     comma-separated readiness URLs to check first ("" to skip)
//
// Exits 0 if every check passes, 1 otherwise, so a CI pipeline can use it
// as a post-deploy check.

const WebSocket = require('ws');

const GATEWAY = process.env.GATEWAY_URL || 'http://localhost:8080';
const WS_URL = process.env.WS_URL || 'ws://localhost:8084';
const OPS_URLS = (process.env.OPS_URLS ?? [9090, 9091, 9095, 9096, 9097].map((p) => `http://localhost:${p}/readyz`).join(','))
  .split(',').filter(Boolean);

let failures = 0;
function check(name, ok, detail) {
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}${ok ? '' : `  ->  ${JSON.stringify(detail)}`}`);
  if (!ok) failures += 1;
}

async function api(method, path, { token, body, key } = {}) {
  const headers = { 'Content-Type': 'application/json' };
  if (token) headers.Authorization = `Bearer ${token}`;
  if (key) headers['Idempotency-Key'] = key;
  const res = await fetch(GATEWAY + path, { method, headers, body: body ? JSON.stringify(body) : undefined });
  return { status: res.status, body: await res.json().catch(() => ({})) };
}

async function newUser(label, run) {
  const email = `smoke-${label}-${run}@example.com`;
  const reg = await api('POST', '/auth/register', { body: { email, password: 'smoke-test-password' } });
  check(`register ${label}`, reg.status === 201, reg);
  const login = await api('POST', '/auth/login', { body: { email, password: 'smoke-test-password' } });
  check(`login ${label}`, login.status === 200 && login.body.accessToken, login);
  const wallet = await api('GET', '/wallets/me', { token: login.body.accessToken });
  check(`${label} has a wallet with 0 balance`, wallet.status === 200 && wallet.body.balanceCents === 0, wallet);
  return { token: login.body.accessToken, walletId: wallet.body.id };
}

function listen(token) {
  return new Promise((resolve, reject) => {
    const received = [];
    const ws = new WebSocket(`${WS_URL}/ws?token=${token}`);
    ws.on('message', (m) => received.push(JSON.parse(m.toString())));
    ws.on('open', () => resolve({ ws, received }));
    ws.on('error', reject);
  });
}

async function waitFor(fn, timeoutMs = 15000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (fn()) return true;
    await new Promise((r) => setTimeout(r, 250));
  }
  return false;
}

async function main() {
  // 1. Every service reports ready
  for (const url of OPS_URLS) {
    const res = await fetch(url).catch((err) => ({ status: 0, err: err.message }));
    check(`ready: ${url}`, res.status === 200, res.err || res.status);
  }

  // 2. Two users, both listening for live notifications
  const run = `${Date.now()}-${Math.floor(Math.random() * 1e6)}`;
  const alice = await newUser('alice', run);
  const bob = await newUser('bob', run);
  const aliceWs = await listen(alice.token);
  const bobWs = await listen(bob.token);

  // 3. Money moves, exactly once
  const dep = await api('POST', '/wallets/me/deposits', { token: alice.token, key: `dep-${run}`, body: { amountCents: 2000000 } });
  check('deposit 20,000.00', dep.status === 201, dep);
  const depAgain = await api('POST', '/wallets/me/deposits', { token: alice.token, key: `dep-${run}`, body: { amountCents: 2000000 } });
  check('same Idempotency-Key returns the same deposit', depAgain.body.id === dep.body.id, depAgain);

  const t = await api('POST', '/transfers', { token: alice.token, key: `t-${run}`, body: { toWalletId: bob.walletId, amountCents: 1500000 } });
  check('transfer 15,000.00 alice -> bob', t.status === 201, t);
  const broke = await api('POST', '/transfers', { token: bob.token, key: `broke-${run}`, body: { toWalletId: alice.walletId, amountCents: 999999999 } });
  check('insufficient funds is rejected (422)', broke.status === 422, broke);

  const a = await api('GET', '/wallets/me', { token: alice.token });
  const b = await api('GET', '/wallets/me', { token: bob.token });
  check('alice balance is 5,000.00', a.body.balanceCents === 500000, a.body);
  check('bob balance is 15,000.00', b.body.balanceCents === 1500000, b.body);

  // 4. Events flowed: wallet -> outbox -> Kafka -> notification (and fraud)
  const types = (w) => w.received.map((m) => (m.rule ? `${m.type}:${m.rule}` : m.type));
  check('bob got transfer.received over WebSocket',
    await waitFor(() => types(bobWs).includes('transfer.received')), types(bobWs));
  check('alice got deposit.completed over WebSocket',
    await waitFor(() => types(aliceWs).includes('deposit.completed')), types(aliceWs));
  check('fraud flagged the 15,000.00 transfer (LARGE_AMOUNT alert reached alice)',
    await waitFor(() => types(aliceWs).includes('security.alert:LARGE_AMOUNT')), types(aliceWs));

  aliceWs.ws.close();
  bobWs.ws.close();
  console.log(failures === 0
    ? '\nALL CHECKS PASSED\nNow run scripts/reconcile.sql to confirm the ledger matches the wallets.'
    : `\n${failures} CHECK(S) FAILED`);
  process.exit(failures === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error('smoke test crashed:', err.message);
  process.exit(1);
});