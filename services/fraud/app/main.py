"""The fraud service.

A FastAPI app whose only HTTP endpoints are the ops endpoints (/healthz, /readyz, /metrics).
The real work happens in the background: FraudWorker reads transfers.completed from Kafka,
checks each one against the rules, and publishes alerts to fraud.alerts.

Run: uvicorn app.main:app --host 0.0.0.0 --port 9090
On SIGTERM, uvicorn runs the shutdown half of `lifespan`: readiness goes to 503,
then the worker finishes its current message and closes its connections.
"""

import asyncio
import os
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from redis.asyncio import Redis

from . import logs
from .worker import FraudWorker

log = logs.setup()


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        print(f"missing required environment variable: {name}", file=sys.stderr)
        sys.exit(1)
    return value


REDIS_URL = require_env("REDIS_URL")
KAFKA_BROKERS = require_env("KAFKA_BROKERS").split(",")


@asynccontextmanager
async def lifespan(app: FastAPI):
    redis = Redis.from_url(REDIS_URL, socket_timeout=2, socket_connect_timeout=2)
    worker = FraudWorker(KAFKA_BROKERS, redis, log)
    worker.start()
    app.state.redis = redis
    app.state.worker = worker
    app.state.draining = False
    yield
    log.info("shutdown started, marking not ready")
    app.state.draining = True
    await worker.stop()
    await redis.aclose()
    log.info("shutdown complete")


app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.get("/readyz")
async def readyz(response: Response):
    if app.state.draining:
        response.status_code = 503
        return {"status": "draining"}
    checks = {"kafka-consumer": "ok" if app.state.worker.started else "fail: not started"}
    try:
        await asyncio.wait_for(app.state.redis.ping(), timeout=2)
        checks["redis"] = "ok"
    except Exception as e:
        checks["redis"] = f"fail: {e}"
    ok = all(v == "ok" for v in checks.values())
    if not ok:
        response.status_code = 503
    return {"status": "ok" if ok else "not_ready", "checks": checks}


@app.get("/metrics")
async def metrics_endpoint():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)