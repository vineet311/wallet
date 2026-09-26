"""Rule-based fraud checks. Each rule returns an alert dict, or None.

Thresholds come from environment variables, so they can change without a new image.
"""

import os
from datetime import datetime

from redis.asyncio import Redis

MAX_AMOUNT = int(os.getenv("FRAUD_MAX_AMOUNT_CENTS", "1000000"))  # 10,000.00
MAX_PER_MINUTE = int(os.getenv("FRAUD_MAX_PER_MINUTE", "5"))
WINDOW_MS = 60_000


def large_amount(event: dict) -> dict | None:
    if event["amountCents"] <= MAX_AMOUNT:
        return None
    return {
        "rule": "LARGE_AMOUNT",
        "detail": f"amount {event['amountCents']} is above the limit of {MAX_AMOUNT}",
    }


def event_time_ms(event: dict) -> int:
    """When the transfer happened, in milliseconds. Uses the event's own time, not ours,
    so a consumer that is catching up after an outage still measures the real rate."""
    try:
        return int(datetime.fromisoformat(event["occurredAt"]).timestamp() * 1000)
    except (KeyError, ValueError):
        return int(datetime.now().timestamp() * 1000)


async def velocity(event: dict, redis: Redis) -> dict | None:
    """How many transfers did this wallet send in the last 60 seconds?

    One Redis sorted set per wallet: score = event time, member = eventId.
    Using eventId as the member makes this idempotent: a redelivered event
    is the same member, so it is not counted twice.
    """
    key = f"fraud:velocity:{event['fromWalletId']}"
    at = event_time_ms(event)
    async with redis.pipeline(transaction=True) as pipe:
        pipe.zadd(key, {event["eventId"]: at})
        pipe.zremrangebyscore(key, 0, at - WINDOW_MS)
        pipe.zcount(key, at - WINDOW_MS, at)
        pipe.pexpire(key, WINDOW_MS * 2)
        _, _, count, _ = await pipe.execute()
    if count <= MAX_PER_MINUTE:
        return None
    return {
        "rule": "VELOCITY",
        "detail": f"{count} transfers in the last minute, limit is {MAX_PER_MINUTE}",
    }


async def evaluate(event: dict, redis: Redis) -> list[dict]:
    alerts = []
    if (a := large_amount(event)) is not None:
        alerts.append(a)
    if (a := await velocity(event, redis)) is not None:
        alerts.append(a)
    return alerts