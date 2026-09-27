"""Structured (JSON) logging shared by every service.

One JSON object per line -> easy to grep, ship to ELK/Loki, or read with `jq`.
Every record carries `service` and `stage` (ingestion / processing / storage / serving /
orchestration) so a failure can be traced to a pipeline stage.

Usage:
    log = get_logger("stream-producer", stage="ingestion")
    log.info("batch sent", extra={"fields": {"count": 25}})
    # or the short helper:
    info(log, "batch sent", count=25)
"""
import json
import logging
import sys
from datetime import datetime, timezone

_RESERVED = {"fields"}


class JsonFormatter(logging.Formatter):
    def __init__(self, service: str, stage: str):
        super().__init__()
        self.service = service
        self.stage = stage

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "service": self.service,
            "stage": self.stage,
            "msg": record.getMessage(),
        }
        payload.update(getattr(record, "fields", {}) or {})
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_logger(service: str, stage: str, level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(f"{service}.{stage}")
    logger.setLevel(level)
    logger.propagate = False
    if not logger.handlers:  # avoid duplicate handlers on re-import
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter(service, stage))
        logger.addHandler(handler)
    return logger


def _log(logger: logging.Logger, level: int, msg: str, **fields) -> None:
    logger.log(level, msg, extra={"fields": fields})


def info(logger, msg, **fields):
    _log(logger, logging.INFO, msg, **fields)


def warning(logger, msg, **fields):
    _log(logger, logging.WARNING, msg, **fields)


def error(logger, msg, **fields):
    _log(logger, logging.ERROR, msg, **fields)
