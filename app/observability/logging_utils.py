import json
import logging
import os
from datetime import datetime, timezone


class JsonLogFormatter(logging.Formatter):
    def format(self, record):
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": getattr(record, "service", os.getenv("SERVICE_NAME", "cotacao-bolsa")),
            "event": getattr(record, "event", record.getMessage()),
            "message": record.getMessage(),
        }

        for key, value in getattr(record, "context", {}).items():
            payload[key] = value

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=False, default=str)


def configure_json_logging(service_name):
    logger = logging.getLogger()
    logger.handlers.clear()

    handler = logging.StreamHandler()
    handler.setFormatter(JsonLogFormatter())
    logger.addHandler(handler)
    logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

    os.environ["SERVICE_NAME"] = service_name
    return logging.getLogger(service_name)


def log_event(logger, level, event, **context):
    logger.log(
        level,
        event,
        extra={
            "service": os.getenv("SERVICE_NAME", logger.name),
            "event": event,
            "context": context,
        },
    )
