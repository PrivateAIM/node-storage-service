from datetime import datetime, timezone
import json
import logging


class JsonFormatter(logging.Formatter):
    """Emit each log record as a single JSON line for structured log ingestion."""

    def format(self, record: logging.LogRecord) -> str:
        log = {
            "timestamp": (
                datetime.fromtimestamp(
                    record.created,
                    tz=timezone.utc,
                )
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z")
            ),
            "level": record.levelname,
            "logger": record.name,
            "module": record.module,
            "msg": record.getMessage(),
        }

        if record.exc_info:
            log["error"] = self.formatException(record.exc_info)

        return json.dumps(log, default=str)  # for non-serializable msgs
