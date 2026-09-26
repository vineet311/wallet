'use strict';

const http = require('node:http');
const client = require('@prometheus-io/client');
const log = require('./logger');

// Ops server on OPS_PORT (default 9090): /healthz, /readyz, /metrics.
// Kept off the WebSocket port so it is never exposed to users.

const register = new client.Registry();
register.setDefaultLabels({ service: 'notification' });
client.collectDefaultMetrics({ register });

const metrics = {
  connections: new client.Gauge({
    name: 'notification_ws_connections', help: 'Open WebSocket connections on this pod', registers: [register],
  }),
  pushed: new client.Counter({
    name: 'notification_ws_messages_total', help: 'Messages pushed to WebSocket clients', registers: [register],
  }),
  events: new client.Counter({
    name: 'notification_events_total', help: 'Kafka events handled, by topic and result',
    labelNames: ['topic', 'result'], registers: [register],
  }),
  webhooks: new client.Counter({
    name: 'notification_webhooks_total', help: 'Mock webhook calls by result',
    labelNames: ['result'], registers: [register],
  }),
};

const checks = new Map();
let draining = false;

function addReadinessCheck(name, fn) {
  checks.set(name, fn);
}

function setDraining() {
  draining = true;
}

async function runChecks() {
  const results = {};
  let ok = true;
  for (const [name, fn] of checks) {
    try {
      await Promise.race([
        Promise.resolve().then(fn),
        new Promise((_, reject) => setTimeout(() => reject(new Error('timed out')), 2000)),
      ]);
      results[name] = 'ok';
    } catch (err) {
      ok = false;
      results[name] = `fail: ${err.message}`;
    }
  }
  return { ok, results };
}

function startOpsServer() {
  const port = Number(process.env.OPS_PORT || 9090);
  const server = http.createServer(async (req, res) => {
    const json = (code, body) => {
      res.writeHead(code, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(body));
    };
    if (req.url === '/healthz') return json(200, { status: 'ok' });
    if (req.url === '/readyz') {
      if (draining) return json(503, { status: 'draining' });
      const { ok, results } = await runChecks();
      return json(ok ? 200 : 503, { status: ok ? 'ok' : 'not_ready', checks: results });
    }
    if (req.url === '/metrics') {
      res.writeHead(200, { 'Content-Type': register.contentType });
      return res.end(await register.metrics());
    }
    return json(404, { error: 'not found' });
  });
  server.listen(port, () => log.info({ port }, 'ops server listening'));
  return server;
}

module.exports = { startOpsServer, addReadinessCheck, setDraining, metrics };