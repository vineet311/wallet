'use strict';

// The notification service.
//
// Users connect a WebSocket to ws://host:PORT/ws?token=<JWT from the gateway>.
// Kafka events (transfers, deposits, fraud alerts) become live messages to the right users.
//
// How a message reaches the right browser when there are several pods:
//   Kafka --(consumer group "notification": each event read ONCE)--> one pod
//   that pod --> Redis PUBLISH on channel "notifications"
//   EVERY pod SUBSCRIBEs to that channel and pushes to the users connected to it.
// Without the Redis step, a user connected to pod 1 would miss events Kafka handed to pod 2.

const http = require('node:http');
const Redis = require('ioredis');
const jwt = require('jsonwebtoken');
const { Kafka, logLevel } = require('kafkajs');
const { WebSocketServer } = require('ws');
const log = require('./logger');
const { startOpsServer, addReadinessCheck, setDraining, metrics } = require('./ops');

function requireEnv(name) {
  const value = process.env[name];
  if (!value) {
    console.error(`missing required environment variable: ${name}`);
    process.exit(1);
  }
  return value;
}

const JWT_SECRET = requireEnv('JWT_SECRET');
const REDIS_URL = requireEnv('REDIS_URL');
const KAFKA_BROKERS = requireEnv('KAFKA_BROKERS').split(',');
const PORT = Number(process.env.PORT || 8084);
const WEBHOOK_URL = process.env.WEBHOOK_URL || '';
const SHUTDOWN_DELAY_MS = Number(process.env.SHUTDOWN_DELAY_MS || 5000);

const TOPICS = ['transfers.completed', 'wallet.deposited', 'fraud.alerts'];
const CHANNEL = 'notifications';

class InvalidEvent extends Error {}

// Turn one event into the messages users should see: [{ userId, message }]
function messagesFor(topic, e) {
  const need = (...fields) => {
    for (const f of fields) {
      if (e[f] === undefined || e[f] === null || e[f] === '') throw new InvalidEvent(`event is missing field "${f}"`);
    }
  };
  if (topic === 'transfers.completed') {
    need('eventId', 'transactionId', 'fromUserId', 'toUserId', 'amountCents');
    return [
      { userId: e.fromUserId, message: { type: 'transfer.sent', transactionId: e.transactionId, amountCents: e.amountCents, toWalletId: e.toWalletId } },
      { userId: e.toUserId, message: { type: 'transfer.received', transactionId: e.transactionId, amountCents: e.amountCents, fromWalletId: e.fromWalletId } },
    ];
  }
  if (topic === 'wallet.deposited') {
    need('eventId', 'transactionId', 'userId', 'amountCents');
    return [{ userId: e.userId, message: { type: 'deposit.completed', transactionId: e.transactionId, amountCents: e.amountCents } }];
  }
  if (topic === 'fraud.alerts') {
    need('eventId', 'transactionId', 'fromUserId', 'rule');
    return [{ userId: e.fromUserId, message: { type: 'security.alert', transactionId: e.transactionId, rule: e.rule } }];
  }
  throw new InvalidEvent(`unexpected topic ${topic}`);
}

async function callWebhook(payload) {
  if (!WEBHOOK_URL) return;
  try {
    const res = await fetch(WEBHOOK_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(3000),
    });
    metrics.webhooks.inc({ result: res.ok ? 'ok' : 'http_error' });
  } catch (err) {
    // A mock webhook failing must never block notifications, so no retry.
    metrics.webhooks.inc({ result: 'error' });
    log.warn({ err: err.message }, 'webhook call failed');
  }
}

async function main() {
  const opsServer = startOpsServer();

  const redis = new Redis(REDIS_URL, { maxRetriesPerRequest: 3 });
  const subscriber = new Redis(REDIS_URL); // a subscribed connection can't run other commands
  redis.on('error', (err) => log.warn({ err: err.message }, 'redis error'));
  subscriber.on('error', (err) => log.warn({ err: err.message }, 'redis subscriber error'));
  addReadinessCheck('redis', () => redis.ping());

  // ---------- WebSocket server ----------
  const sockets = new Map(); // userId -> Set of open sockets on THIS pod
  const server = http.createServer((req, res) => {
    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end('{"error":"not found"}');
  });
  const wss = new WebSocketServer({ noServer: true });

  server.on('upgrade', (req, socket, head) => {
    const url = new URL(req.url, 'http://localhost');
    if (url.pathname !== '/ws') {
      socket.destroy();
      return;
    }
    let userId;
    try {
      userId = jwt.verify(url.searchParams.get('token') || '', JWT_SECRET, { algorithms: ['HS256'] }).sub;
    } catch {
      socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
      socket.destroy();
      return;
    }
    wss.handleUpgrade(req, socket, head, (ws) => {
      ws.isAlive = true;
      if (!sockets.has(userId)) sockets.set(userId, new Set());
      sockets.get(userId).add(ws);
      metrics.connections.inc();
      ws.on('pong', () => { ws.isAlive = true; });
      ws.on('close', () => {
        sockets.get(userId)?.delete(ws);
        if (sockets.get(userId)?.size === 0) sockets.delete(userId);
        metrics.connections.dec();
      });
      ws.send(JSON.stringify({ type: 'connected' }));
    });
  });

  // Drop connections that stopped answering pings (closed laptop, lost network).
  const heartbeat = setInterval(() => {
    for (const ws of wss.clients) {
      if (!ws.isAlive) { ws.terminate(); continue; }
      ws.isAlive = false;
      ws.ping();
    }
  }, 30000);

  server.listen(PORT, () => log.info({ port: PORT }, 'WebSocket server listening'));

  // ---------- fan-out: every pod pushes to its own connected users ----------
  await subscriber.subscribe(CHANNEL);
  subscriber.on('message', (_channel, raw) => {
    const { userId, message } = JSON.parse(raw);
    for (const ws of sockets.get(userId) || []) {
      ws.send(JSON.stringify(message));
      metrics.pushed.inc();
    }
  });

  // ---------- Kafka: exactly one pod handles each event ----------
  const kafka = new Kafka({ clientId: 'notification', brokers: KAFKA_BROKERS, logLevel: logLevel.WARN });
  const consumer = kafka.consumer({ groupId: 'notification' });
  const dlqProducer = kafka.producer({ allowAutoTopicCreation: false });
  let consuming = false;
  let stopping = false;
  addReadinessCheck('kafka-consumer', () => {
    if (!consuming) throw new Error('not consuming yet');
  });

  async function handle({ topic, partition, message }) {
    let event;
    try {
      event = JSON.parse(message.value.toString());
      if (typeof event !== 'object' || event === null) throw new InvalidEvent('message is not a JSON object');
      const out = messagesFor(topic, event);

      // Skip redelivered events so users don't see the same notification twice.
      const fresh = await redis.set(`notif:seen:${event.eventId}`, '1', 'EX', 3600, 'NX');
      if (!fresh) {
        metrics.events.inc({ topic, result: 'duplicate' });
        return;
      }
      for (const m of out) await redis.publish(CHANNEL, JSON.stringify(m));
      await callWebhook({ topic, event });
      metrics.events.inc({ topic, result: 'sent' });
    } catch (err) {
      if (!(err instanceof InvalidEvent) && !(err instanceof SyntaxError)) {
        // Probably temporary (Redis down). Throwing makes kafkajs retry,
        // and the offset is not committed, so the event is not lost.
        metrics.events.inc({ topic, result: 'retry' });
        log.error({ err: err.message, topic, offset: message.offset }, 'handling failed, will retry');
        throw err;
      }
      metrics.events.inc({ topic, result: 'dlq' });
      log.error({ reason: err.message, topic, partition, offset: message.offset }, 'bad message, sent to DLQ');
      await dlqProducer.send({
        topic: `${topic}.dlq`,
        messages: [{
          key: message.key,
          value: message.value,
          headers: { ...message.headers, 'dlq-reason': err.message, 'dlq-source': `${topic}/${partition}/${message.offset}` },
        }],
      });
    }
  }

  // If Kafka is down at startup, keep trying instead of crashing.
  // Crashing would put the pod in CrashLoopBackOff, where Kubernetes waits
  // up to 5 minutes between restarts. /readyz reports not ready until connected.
  async function startConsumer() {
    let backoff = 1000;
    while (!stopping) {
      try {
        await dlqProducer.connect();
        await consumer.connect();
        for (const topic of TOPICS) await consumer.subscribe({ topic, fromBeginning: false });
        await consumer.run({ eachMessage: handle });
        consuming = true;
        log.info('notification consumer running');
        return;
      } catch (err) {
        log.warn({ err: err.message, retryInMs: backoff }, 'kafka not reachable, retrying');
        await consumer.disconnect().catch(() => {});
        await dlqProducer.disconnect().catch(() => {});
        await new Promise((r) => setTimeout(r, backoff));
        backoff = Math.min(backoff * 2, 10000);
      }
    }
  }
  consumer.on(consumer.events.CRASH, ({ payload }) => {
    consuming = false;
    log.error({ err: payload.error?.message, restart: payload.restart }, 'consumer crashed');
    if (!payload.restart) process.exit(1); // let Kubernetes restart the pod
  });
  consumer.on(consumer.events.GROUP_JOIN, () => { consuming = true; });
  const consumerStarted = startConsumer();

  // ---------- graceful shutdown ----------
  async function shutdown(signal) {
    if (stopping) return;
    stopping = true;
    log.info({ signal }, 'shutdown started, marking not ready');
    setDraining();
    setTimeout(() => { log.error('shutdown timed out, forcing exit'); process.exit(1); }, SHUTDOWN_DELAY_MS + 20000).unref();
    await new Promise((r) => setTimeout(r, SHUTDOWN_DELAY_MS));

    clearInterval(heartbeat);
    // 1001 = "going away". Clients should reconnect, and will land on another pod.
    for (const ws of wss.clients) ws.close(1001, 'server shutting down');
    await new Promise((r) => server.close(() => r()));
    await consumerStarted;
    await consumer.disconnect().catch(() => {});
    await dlqProducer.disconnect().catch(() => {});
    await redis.quit().catch(() => {});
    await subscriber.quit().catch(() => {});
    opsServer.close();
    log.info('shutdown complete');
    process.exit(0);
  }
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

main().catch((err) => {
  log.fatal({ err: err.message }, 'notification service failed to start');
  process.exit(1);
});