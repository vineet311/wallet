# Build context is services/fraud:
#   docker build -t wallet-ledger/fraud services/fraud
FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
# Dependencies first, so this layer stays cached until requirements.txt changes.
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY app ./app

RUN useradd --system --uid 10001 app
USER app
EXPOSE 9090
# On SIGTERM uvicorn runs the shutdown code; 20s to finish before it force-stops.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "9090", "--timeout-graceful-shutdown", "20"]