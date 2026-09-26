'use strict';

// Load generator: creates USERS users, funds them, then fires TRANSFERS random
// transfers between them with CONCURRENCY requests in flight.
// Use it to build Kafka consumer lag for the KEDA autoscaling demo.
//
//   GATEWAY_URL=http://localhost:8080 USERS=20 TRANSFERS=5000 CONCURRENCY=50 node load.js
//
// The gateway's rate limits will block a load test. Start the gateway with
// RATE_LIMIT_PER_MINUTE and RATE_LIMIT_AUTH_PER_MINUTE set high for load runs.

const GATEWAY = process.env.GATEWAY_URL || 'http://localhost:8080';
const USERS = Number(process.env.USERS || 10);
const TRANSFERS = Number(process.env.TRANSFERS || 1000);
const CONCURRENCY = Number(process.env.CONCURRENCY || 20);

async function api(method, path, { token, body, key } = {}) {
  const headers = { 'Content-Type': 'application/json' };
  if (token) headers.Authorization = `Bearer ${token}`;
  if (key) headers['Idempotency-Key'] = key;
  const res = await fetch(GATEWAY + path, { method, headers, body: body ? JSON.stringify(body) : undefined });
  return { status: res.status, body: await res.json().catch(() => ({})) };
}

async function main() {
  const run = Date.now();
  const users = [];
  for (let i = 0; i < USERS; i += 1) {
    const email = `load-${run}-${i}@example.com`;
    await api('POST', '/auth/register', { body: { email, password: 'load-test-password' } });
    const { body } = await api('POST', '/auth/login', { body: { email, password: 'load-test-password' } });
    if (!body.accessToken) throw new Error(`login failed for ${email}: is the auth rate limit raised?`);
    const w = await api('GET', '/wallets/me', { token: body.accessToken });
    await api('POST', '/wallets/me/deposits', { token: body.accessToken, key: `fund-${run}-${i}`, body: { amountCents: 100000000 } });
    users.push({ token: body.accessToken, walletId: w.body.id });
  }
  console.log(`created and funded ${users.length} users`);

  const statuses = {};
  let next = 0;
  const started = Date.now();
  async function worker() {
    while (next < TRANSFERS) {
      const n = next;
      next += 1;
      const from = users[n % users.length];
      let to = users[Math.floor(Math.random() * users.length)];
      if (to === from) to = users[(n + 1) % users.length];
      const r = await api('POST', '/transfers', {
        token: from.token, key: `load-${run}-${n}`, body: { toWalletId: to.walletId, amountCents: 1 + (n % 500) },
      });
      statuses[r.status] = (statuses[r.status] || 0) + 1;
    }
  }
  await Promise.all(Array.from({ length: CONCURRENCY }, worker));
  const secs = (Date.now() - started) / 1000;
  console.log(`sent ${TRANSFERS} transfers in ${secs.toFixed(1)}s (${(TRANSFERS / secs).toFixed(0)}/s)`, statuses);
}

main().catch((err) => {
  console.error(err.message);
  process.exit(1);
});