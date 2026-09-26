# Problems we hit and how we fixed them

Each entry: what broke, what we saw, the cause, the fix, the lesson.

<!-- template
## Short title
- **Symptom:**
- **Cause:**
- **Fix:**
- **Lesson:**
-->



## Containers could not reach each other in Codespaces
- **Symptom:** kafka-init failed with "Timed out waiting for a node assignment."
  The same command worked from inside the Kafka container.
- **Cause:** Codespaces runs Docker inside a container, with two firewall systems
  installed: iptables (nft) and iptables-legacy. Docker wrote its allow rules to the
  nft one. The legacy one still had `FORWARD DROP` as its default, so all
  container-to-container traffic was silently dropped.
- **How we found it:** name lookup worked but TCP connections hung (hang = dropped,
  refused = nothing listening). Every container pair failed, not just Kafka, so it was
  the network, not Kafka. The iptables warning pointed at the legacy tables.
- **Fix:** `sudo iptables-legacy -P FORWARD ACCEPT` (dev machine only; resets when the
  Codespace stops).
- **Lesson:** the error message pointed at Kafka, but the cause was the network layer.
  Test the layer below before changing config.

  ## Optimistic locking gave up under contention
- **Symptom:** 20 simultaneous transfers from one wallet: 8 failed with "too much contention".
  No money was lost (failed transfers never moved anything), but they were rejected.
- **Cause:** only 5 retries with short, nearly identical waits, so the losers kept colliding.
- **Fix:** 10 retries with exponential backoff and full jitter (random wait up to 10ms, 20ms, 40ms ... 500ms).
  40 simultaneous transfers in both directions between two wallets now all succeed.
- **Lesson:** optimistic locking is cheap when contention is low; under a hot wallet it needs
  a real backoff strategy. Pessimistic locking (SELECT ... FOR UPDATE) is the alternative for hot rows.

## Consumers crashed when Kafka was down at startup
- **Symptom:** the fraud worker exited immediately if Kafka was unreachable when it started.
- **Cause:** the Kafka connection was made during startup; a failed connect killed the process.
- **Why it matters:** in Kubernetes that is CrashLoopBackOff, where restarts are delayed up to
  5 minutes, so recovery after a Kafka outage would be slow.
- **Fix:** connect in the background with backoff. The process stays up, /healthz answers,
  /readyz reports not ready until Kafka is reachable. Same for the notification service.
- **Lesson:** a dependency being down at startup should make a pod not ready, not dead.

## Retry logs flooded during an outage
- **Symptom:** with Redis down, every retry in the fraud worker printed a 40-line traceback.
- **Fix:** retries log one line with the error type and message.
- **Lesson:** during an outage, logs are what you read. Make them short enough to read.

## kafka-go's default batch timeout adds up to 1 second per event
- **Symptom (caught in design, not in production):** kafka-go's Writer waits up to BatchTimeout
  (default 1s) to fill a batch before sending.
- **Fix:** BatchTimeout 10ms, since the outbox relay already hands it a full batch.
- **Lesson:** read the defaults of every client library on the hot path.

## Spring Boot 4 moved to Jackson 3
- **Symptom:** Jackson 2 imports (com.fasterxml.jackson.databind) do not exist in a Boot 4 project.
- **Fix:** use the Jackson 3 packages (tools.jackson.databind) and its renamed methods
  (isString/stringValue instead of isTextual/textValue).
- **Lesson:** major framework upgrades move packages; check the dependency tree, not old examples.

## A readiness check exposed a dead database immediately
- **Observed:** when PostgreSQL stopped during testing, every service's /readyz went to 503 within
  seconds while /healthz stayed 200, and all recovered on their own when it came back.
- **Why it matters:** this is exactly the liveness/readiness split Kubernetes needs:
  stop sending traffic, but do not restart the pod.