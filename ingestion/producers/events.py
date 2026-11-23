"""Event envelope, data contracts and sinks shared by the streaming producers (P4.1, P4.2).

Every event is a JSON object with the same envelope::

    {
      "event_id":       UUID, deterministic (see below),
      "event_ts_utc":   when the measured thing happened (ISO 8601, UTC, "Z"),
      "produced_at":    when the producer sent it (ISO 8601, UTC, "Z"),
      "schema_version": e.g. "energy.generation.v1" (also the topic name),
      "source":         where the data comes from,
      "payload":        the measurement itself
    }

**Deterministic event IDs.** ``event_id`` is a UUID5 of the source, the key and the
values, not a random UUID4. Sending the same measurement twice (a restarted producer, an
overlapping poll window) produces the same ID, so the streaming layer can drop the copy
(P4.5). A revised value is a different event with a different ID.

**Data contracts.** Each topic has a JSON Schema in ``schemas/``. Every event is
validated before it is sent; an invalid event goes to the topic's dead-letter queue
(``<topic>.dlq`` naming: ``energy.generation.dlq.v1``) together with the reasons, so bad
data is kept for inspection but never reaches consumers of the main topic.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import cache
from pathlib import Path
from typing import Any, Protocol

import fastjsonschema

SCHEMA_DIR = Path(__file__).resolve().parent / "schemas"
NAMESPACE = uuid.UUID("6f0c1c4e-9a0b-4b8e-8a57-7d2f6a1e5c11")  # GridPulse event IDs

TOPICS = {
    "energy.generation.v1": "energy_generation.v1.json",
    "meters.readings.v1": "meter_readings.v1.json",
}


def dlq_topic(topic: str) -> str:
    """energy.generation.v1 -> energy.generation.dlq.v1"""
    name, _, version = topic.rpartition(".")
    return f"{name}.dlq.{version}"


def iso_utc(ts: datetime) -> str:
    return ts.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def event_id(*parts: object) -> str:
    return str(uuid.uuid5(NAMESPACE, "|".join(str(p) for p in parts)))


def make_event(
    topic: str,
    source: str,
    event_ts: datetime,
    payload: dict[str, Any],
    id_parts: tuple,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> dict[str, Any]:
    return {
        "event_id": event_id(source, *id_parts),
        "event_ts_utc": iso_utc(event_ts),
        "produced_at": iso_utc(now()),
        "schema_version": topic,
        "source": source,
        "payload": payload,
    }


@cache
def validator(topic: str) -> Callable[[dict], Any]:
    schema = json.loads((SCHEMA_DIR / TOPICS[topic]).read_text(encoding="utf-8"))
    return fastjsonschema.compile(schema)


def validation_error(topic: str, event: dict) -> str | None:
    """None if the event matches the topic's contract, else the reason."""
    try:
        validator(topic)(event)
    except fastjsonschema.JsonSchemaException as exc:
        return str(exc.message)
    return None


# --------------------------------------------------------------------------- sinks


class Sink(Protocol):
    def send(self, topic: str, key: str, value: dict) -> None: ...
    def flush(self) -> None: ...
    def close(self) -> None: ...


@dataclass
class MemorySink:
    """Keeps messages in a list (tests and --dry-run)."""

    messages: list[tuple[str, str, dict]] = field(default_factory=list)

    def send(self, topic: str, key: str, value: dict) -> None:
        self.messages.append((topic, key, value))

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass


class KafkaSink:
    """Sends JSON messages to Kafka / Redpanda with kafka-python (pure Python).

    Connection settings come from the environment:

    * ``GRIDPULSE_KAFKA_BOOTSTRAP``  host:port[,host:port]  (default localhost:9092)
    * ``GRIDPULSE_KAFKA_SECURITY``   PLAINTEXT (local Docker) or SASL_SSL (cloud)
    * ``GRIDPULSE_KAFKA_USERNAME`` / ``GRIDPULSE_KAFKA_PASSWORD``  for SASL
    * ``GRIDPULSE_KAFKA_MECHANISM``  default SCRAM-SHA-256

    ``acks="all"`` plus idempotence: a message counts as sent only when the broker has it
    on all replicas, and broker-side retries can't create duplicates.
    """

    def __init__(self, env: dict[str, str]) -> None:
        from kafka import KafkaProducer

        security = env.get("GRIDPULSE_KAFKA_SECURITY", "PLAINTEXT")
        options: dict[str, Any] = {
            "bootstrap_servers": env.get("GRIDPULSE_KAFKA_BOOTSTRAP", "localhost:9092").split(","),
            "security_protocol": security,
            "acks": "all",
            "enable_idempotence": True,
            "linger_ms": 50,
            "compression_type": "gzip",
            "key_serializer": lambda k: k.encode("utf-8"),
            "value_serializer": lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
        }
        if security.startswith("SASL"):
            options |= {
                "sasl_mechanism": env.get("GRIDPULSE_KAFKA_MECHANISM", "SCRAM-SHA-256"),
                "sasl_plain_username": env["GRIDPULSE_KAFKA_USERNAME"],
                "sasl_plain_password": env["GRIDPULSE_KAFKA_PASSWORD"],
            }
        self._producer = KafkaProducer(**options)

    def send(self, topic: str, key: str, value: dict) -> None:
        self._producer.send(topic, key=key, value=value)

    def flush(self) -> None:
        self._producer.flush()

    def close(self) -> None:
        self._producer.close()


@dataclass
class Publisher:
    """Validates each event against its topic's contract and routes it (main topic or DLQ)."""

    sink: Sink
    sent: int = 0
    dead_lettered: int = 0

    def publish(self, topic: str, key: str, event: dict) -> bool:
        error = validation_error(topic, event)
        if error is None:
            self.sink.send(topic, key, event)
            self.sent += 1
            return True
        self.sink.send(
            dlq_topic(topic),
            key,
            {
                "failed_at": iso_utc(datetime.now(UTC)),
                "topic": topic,
                "error": error,
                "event": event,
            },
        )
        self.dead_lettered += 1
        return False
