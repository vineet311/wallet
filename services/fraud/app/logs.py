"""JSON logs to stdout, one object per line, same shape idea as the other services."""

import json
import logging
import os
import sys
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "time": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname.lower(),
            "service": "fraud",
            "msg": record.getMessage(),
        }
        # Extra fields passed as logger.info("...", extra={"fields": {...}})
        entry.update(getattr(record, "fields", {}))
        if record.exc_info:
            entry["error"] = self.formatException(record.exc_info)
        return json.dumps(entry)


def setup() -> logging.Logger:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(logging.DEBUG if os.getenv("LOG_LEVEL") == "debug" else logging.INFO)
    # The Kafka client is chatty at INFO; keep only its warnings.
    logging.getLogger("aiokafka").setLevel(logging.WARNING)
    return logging.getLogger("fraud")