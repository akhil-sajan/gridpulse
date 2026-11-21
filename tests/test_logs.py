import json
import logging

from ingestion.common.logs import JsonFormatter, get_logger


def test_json_formatter_includes_extra_fields():
    record = logging.makeLogRecord(
        {"name": "t", "levelname": "INFO", "msg": "hello %s", "args": ("world",), "path": "/a"}
    )
    out = json.loads(JsonFormatter().format(record))
    assert out["message"] == "hello world"
    assert out["level"] == "INFO"
    assert out["logger"] == "t"
    assert out["path"] == "/a"
    assert out["ts"].endswith("+00:00")


def test_get_logger_adds_one_handler_only():
    first = get_logger("gridpulse.test")
    second = get_logger("gridpulse.test")
    assert first is second
    assert sum(getattr(h, "_gridpulse", False) for h in first.handlers) == 1


def test_logger_writes_json_lines(capsys):
    get_logger("gridpulse.capture").info("uploaded", extra={"bytes": 10})
    line = capsys.readouterr().err.strip().splitlines()[-1]
    data = json.loads(line)
    assert data["message"] == "uploaded"
    assert data["bytes"] == 10
