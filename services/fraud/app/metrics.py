from prometheus_client import Counter, Histogram

messages = Counter(
    "fraud_messages_total",
    "Transfer events handled, by result: clean, flagged, dlq, retry.",
    ["result"],
)
alerts = Counter("fraud_alerts_total", "Fraud alerts raised, by rule.", ["rule"])
duration = Histogram("fraud_message_duration_seconds", "Time to check one transfer.")