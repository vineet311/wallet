"""The Kafka side of the fraud service: read transfers, check them, raise alerts.

Failure handling, same idea as the ledger:
  InvalidEvent (not JSON, missing fields): sent to transfers.completed.dlq, then we move on.
  Anything else (for example Redis is down): retried with backoff until it works.
  The offset is committed only after a message is fully handled, so nothing is skipped.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from redis.asyncio import Redis

from . import metrics
from .rules import evaluate

TRANSFERS = "transfers.completed"
ALERTS = "fraud.alerts"


class InvalidEvent(Exception):
    """The message itself is broken. Retrying can never fix it."""


def parse(raw: bytes | None) -> dict:
    try:
        event = json.loads(raw or b"")
    except (ValueError, UnicodeDecodeError):
        raise InvalidEvent("message is not valid JSON")
    if not isinstance(event, dict):
        raise InvalidEvent("message is not a JSON object")
    for field in ("eventId", "transactionId", "fromWalletId", "fromUserId", "occurredAt"):
        if not isinstance(event.get(field), str) or not event[field]:
            raise InvalidEvent(f'event is missing field "{field}"')
    amount = event.get("amountCents")
    if isinstance(amount, bool) or not isinstance(amount, int) or amount <= 0:
        raise InvalidEvent("amountCents must be a positive integer")
    return event


class FraudWorker:
    def __init__(self, brokers: list[str], redis: Redis, log: logging.Logger):
        self.brokers = brokers
        self.redis = redis
        self.log = log
        self.consumer: AIOKafkaConsumer | None = None
        self.producer: AIOKafkaProducer | None = None
        self.started = False
        self.stopping = False
        self.task: asyncio.Task | None = None

    def start(self) -> None:
        """Connect and consume in the background. Returns at once, so the HTTP server
        (and /healthz) comes up even while Kafka is unreachable."""
        self.task = asyncio.create_task(self.connect_and_run())

    async def stop(self) -> None:
        """Finish the message in progress, then close. Called on SIGTERM."""
        self.stopping = True
        if self.task:
            await self.task
        await self.close()
        self.log.info("fraud consumer stopped")

    async def close(self) -> None:
        if self.consumer:
            await self.consumer.stop()
        if self.producer:
            await self.producer.stop()
        self.consumer = self.producer = None

    async def connect_and_run(self) -> None:
        """If Kafka is down at startup, keep trying instead of crashing.
        Crashing would put the pod in CrashLoopBackOff, where Kubernetes waits
        up to 5 minutes between restarts. Here /readyz reports not ready until connected."""
        backoff = 1.0
        while not self.stopping:
            try:
                self.consumer = AIOKafkaConsumer(
                    TRANSFERS,
                    bootstrap_servers=self.brokers,
                    group_id="fraud",
                    enable_auto_commit=False,
                    auto_offset_reset="earliest",
                )
                self.producer = AIOKafkaProducer(bootstrap_servers=self.brokers, acks="all")
                await self.producer.start()
                await self.consumer.start()
                break
            except Exception as e:
                await self.close()
                self.log.warning("kafka not reachable, retrying",
                                 extra={"fields": {"err": str(e), "retry_in_s": backoff}})
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 10)
        if self.stopping:
            return
        self.started = True
        self.log.info("fraud consumer running")
        await self.run()

    async def run(self) -> None:
        while not self.stopping:
            batches = await self.consumer.getmany(timeout_ms=1000, max_records=100)
            for records in batches.values():
                for msg in records:
                    if not await self.handle_with_retry(msg):
                        return  # shutting down mid-retry: don't commit, another pod will redo it
                    await self.consumer.commit()

    async def handle_with_retry(self, msg) -> bool:
        backoff = 0.5
        while True:
            try:
                with metrics.duration.time():
                    result = await self.handle(msg)
                metrics.messages.labels(result).inc()
                return True
            except InvalidEvent as e:
                await self.to_dlq(msg, str(e))
                metrics.messages.labels("dlq").inc()
                self.log.error("bad message, sent to DLQ",
                               extra={"fields": {"reason": str(e), "partition": msg.partition, "offset": msg.offset}})
                return True
            except Exception as e:
                # One line per retry, not a full traceback: an outage would flood the logs.
                metrics.messages.labels("retry").inc()
                self.log.error("handling failed, will retry", extra={"fields": {
                    "err": f"{type(e).__name__}: {e}", "retry_in_s": backoff, "offset": msg.offset}})
                if self.stopping:
                    return False
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 10)

    async def handle(self, msg) -> str:
        event = parse(msg.value)
        alerts = await evaluate(event, self.redis)
        for alert in alerts:
            # Don't raise the same alert twice for the same transfer (redelivery).
            dedupe_key = f"fraud:alerted:{event['eventId']}:{alert['rule']}"
            if await self.redis.exists(dedupe_key):
                continue
            payload = {
                "eventId": f"{event['eventId']}:{alert['rule']}",
                "type": "FraudAlert",
                "rule": alert["rule"],
                "detail": alert["detail"],
                "transactionId": event["transactionId"],
                "fromWalletId": event["fromWalletId"],
                "fromUserId": event["fromUserId"],
                "amountCents": event["amountCents"],
                "occurredAt": datetime.now(timezone.utc).isoformat(),
            }
            await self.producer.send_and_wait(
                ALERTS, key=event["fromWalletId"].encode(), value=json.dumps(payload).encode())
            await self.redis.set(dedupe_key, "1", ex=86400)
            metrics.alerts.labels(alert["rule"]).inc()
            self.log.warning("fraud alert raised", extra={"fields": {
                "transactionId": event["transactionId"], "rule": alert["rule"], "detail": alert["detail"]}})
        return "flagged" if alerts else "clean"

    async def to_dlq(self, msg, reason: str) -> None:
        headers = list(msg.headers or []) + [
            ("dlq-reason", reason.encode()),
            ("dlq-source", f"{msg.topic}/{msg.partition}/{msg.offset}".encode()),
        ]
        await self.producer.send_and_wait(f"{msg.topic}.dlq", key=msg.key, value=msg.value, headers=headers)