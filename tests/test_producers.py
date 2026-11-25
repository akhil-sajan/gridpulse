"""Tests for the streaming producers and their data contracts (P4.1, P4.2)."""

from __future__ import annotations

import csv
import io
from datetime import UTC, date, datetime, timedelta

from ingestion.producers import energy_charts as ec
from ingestion.producers import meter_replay as mr
from ingestion.producers.events import (
    MemorySink,
    Publisher,
    dlq_topic,
    event_id,
    make_event,
    validation_error,
)

T0 = 1795388400  # 2026-11-23 23:00 UTC... any quarter-hour

RESPONSE = {
    "unix_seconds": [T0, T0 + 900, T0 + 1800],
    "production_types": [
        {"name": "Solar", "data": [0.0, 12.5, None]},
        {"name": "Wind onshore", "data": [20100.4, 20333.0, 20500.1]},
        {"name": "Fossil brown coal / lignite", "data": [8000.0, None, None]},
        {"name": "Load", "data": [55000.0, 56000.0, 57000.0]},
        {"name": "Renewable share of generation", "data": [60.1, 61.0, 61.5]},
        {"name": "Cross border electricity trading", "data": [-1200.0, -900.0, -800.0]},
    ],
    "deprecated": False,
}


class FakeHttp:
    def __init__(self, response):
        self.response = response
        self.calls = 0

    def get_json(self, url, params=None):
        self.calls += 1
        return self.response


# --------------------------------------------------------------------------- envelope


def test_dlq_topic_name():
    assert dlq_topic("energy.generation.v1") == "energy.generation.dlq.v1"
    assert dlq_topic("meters.readings.v1") == "meters.readings.dlq.v1"


def test_event_id_is_deterministic():
    assert event_id("a", 1, 2.5) == event_id("a", 1, 2.5)
    assert event_id("a", 1, 2.5) != event_id("a", 1, 2.6)


def test_valid_event_goes_to_main_topic():
    sink = MemorySink()
    publisher = Publisher(sink)
    key, event = next(ec.to_events(RESPONSE, "de"))
    assert validation_error(ec.TOPIC, event) is None
    assert publisher.publish(ec.TOPIC, key, event) is True
    assert sink.messages[0][0] == "energy.generation.v1"
    assert publisher.sent == 1 and publisher.dead_lettered == 0


def test_malformed_event_lands_in_dlq():
    """P4.2 done criterion: a broken event never reaches the main topic."""
    sink = MemorySink()
    publisher = Publisher(sink)
    bad = make_event(
        ec.TOPIC,
        ec.SOURCE,
        datetime(2026, 11, 23, tzinfo=UTC),
        {
            "country": "de",
            "production_type": "Solar",
            "production_type_slug": "solar",
            "power_mw": "lots",
        },  # wrong type
        id_parts=("x",),
    )
    assert publisher.publish(ec.TOPIC, "solar", bad) is False
    [(topic, key, value)] = sink.messages
    assert topic == "energy.generation.dlq.v1" and key == "solar"
    assert value["topic"] == ec.TOPIC
    assert "power_mw" in value["error"]
    assert value["event"] == bad
    assert publisher.sent == 0 and publisher.dead_lettered == 1


def test_unknown_fields_and_missing_fields_are_rejected():
    _, event = next(ec.to_events(RESPONSE, "de"))
    extra = {**event, "surprise": 1}
    assert validation_error(ec.TOPIC, extra) is not None
    missing = {k: v for k, v in event.items() if k != "produced_at"}
    assert validation_error(ec.TOPIC, missing) is not None
    wrong_version = {**event, "schema_version": "energy.generation.v2"}
    assert validation_error(ec.TOPIC, wrong_version) is not None


# --------------------------------------------------------------------------- energy-charts


def test_to_events_skips_nulls_and_non_generation_series():
    events = list(ec.to_events(RESPONSE, "de"))
    keys = [k for k, _ in events]
    assert keys.count("solar") == 2  # third value not published yet
    assert keys.count("wind_onshore") == 3
    assert keys.count("fossil_brown_coal_lignite") == 1
    assert not {"load", "renewable_share_of_generation"} & set(keys)
    assert len(events) == 6
    _, first = events[0]
    assert first["event_ts_utc"] == datetime.fromtimestamp(T0, UTC).isoformat(
        timespec="milliseconds"
    ).replace("+00:00", "Z")
    assert all(validation_error(ec.TOPIC, e) is None for _, e in events)


def test_slug():
    assert ec.slug("Fossil brown coal / lignite") == "fossil_brown_coal_lignite"
    assert ec.slug("Hydro pumped storage") == "hydro_pumped_storage"


def test_run_once_skips_events_already_sent():
    sink = MemorySink()
    publisher = Publisher(sink)
    http = FakeHttp(RESPONSE)
    seen: dict = {}
    lookback = timedelta(days=36500)  # keep the 2026 test data inside the window
    assert ec.run_once(http, publisher, "de", lookback, seen) == 6
    assert ec.run_once(http, publisher, "de", lookback, seen) == 0
    # A revised value is a new event.
    revised = {**RESPONSE, "production_types": [{"name": "Solar", "data": [0.0, 13.0, None]}]}
    assert ec.run_once(FakeHttp(revised), publisher, "de", lookback, seen) == 1
    assert publisher.sent == 7


def test_same_measurement_has_same_id_across_restarts():
    first = [e["event_id"] for _, e in ec.to_events(RESPONSE, "de")]
    again = [e["event_id"] for _, e in ec.to_events(RESPONSE, "de")]
    assert first == again


# --------------------------------------------------------------------------- meter replay

CSV = """LCLid,tstp,energy(kWh/hh)
MAC000002,2012-10-12 00:30:00.0000000,0
MAC000002,2012-10-13 00:00:00.0000000,0.263
MAC000002,2012-10-13 00:30:00.0000000,0.269
MAC000002,2012-10-13 01:00:00.0000000,Null
MAC000003,2012-10-13 00:00:00.0000000,1.5
MAC000004,2012-10-13 00:00:00.0000000,0.4
MAC000002,2012-10-14 00:00:00.0000000,0.3
"""


def rows():
    return csv.DictReader(io.StringIO(CSV))


def test_read_window_defaults_to_first_full_day():
    slots, first_day = mr.read_window(rows(), None, 1, None)
    assert first_day == date(2012, 10, 13)
    assert sorted(slots) == [
        datetime(2012, 10, 13, 0, 0, tzinfo=UTC),
        datetime(2012, 10, 13, 0, 30, tzinfo=UTC),
    ]  # the Null reading and the other days are left out
    assert len(slots[datetime(2012, 10, 13, 0, 0, tzinfo=UTC)]) == 3


def test_read_window_limits_meters():
    slots, _ = mr.read_window(rows(), date(2012, 10, 13), 1, 2)
    meters = {m for readings in slots.values() for m, _ in readings}
    assert meters == {"MAC000002", "MAC000003"}


def test_replay_sends_in_time_order_and_waits_per_slot():
    slots, _ = mr.read_window(rows(), date(2012, 10, 13), 1, None)
    sink = MemorySink()
    waits = []
    assert mr.replay(slots, Publisher(sink), "r1", speed=1440, sleep=waits.append) == 2
    times = [v["event_ts_utc"] for _, _, v in sink.messages]
    assert times == sorted(times)
    assert waits == [1.25, 1.25]  # 30 minutes / 1440
    assert all(t == "meters.readings.v1" for t, _, _ in sink.messages)


def test_replaying_twice_gives_identical_ids():
    slots, _ = mr.read_window(rows(), date(2012, 10, 13), 1, None)
    a, b = MemorySink(), MemorySink()
    mr.replay(slots, Publisher(a), "run-a", speed=0)
    mr.replay(slots, Publisher(b), "run-b", speed=0)
    assert [v["event_id"] for *_, v in a.messages] == [v["event_id"] for *_, v in b.messages]
    assert {v["payload"]["replay_id"] for *_, v in a.messages} == {"run-a"}


def test_negative_meter_value_goes_to_dlq():
    sink = MemorySink()
    slots = {datetime(2013, 1, 1, tzinfo=UTC): [("MAC000002", -0.5)]}
    publisher = Publisher(sink)
    mr.replay(slots, publisher, "r", speed=0)
    assert sink.messages[0][0] == "meters.readings.dlq.v1"
    assert publisher.dead_lettered == 1


# --------------------------------------------------------------------------- connection


def test_connection_options_local_and_cloud():
    from ingestion.producers.events import connection_options

    local = connection_options({})
    assert local == {"bootstrap_servers": ["localhost:9092"], "security_protocol": "PLAINTEXT"}
    cloud = connection_options(
        {
            "GRIDPULSE_KAFKA_BOOTSTRAP": "abc.any.eu-central-1.mpx.prd.cloud.redpanda.com:9092",
            "GRIDPULSE_KAFKA_SECURITY": "SASL_SSL",
            "GRIDPULSE_KAFKA_USERNAME": "gridpulse",
            "GRIDPULSE_KAFKA_PASSWORD": "secret\n",
        }
    )
    assert cloud["sasl_plain_password"] == "secret"  # pasted line break removed
    assert cloud["security_protocol"] == "SASL_SSL"
    assert cloud["sasl_mechanism"] == "SCRAM-SHA-256"
    assert cloud["sasl_plain_username"] == "gridpulse"


def test_all_topics_include_dlqs():
    from ingestion.producers.kafka_tools import all_topics

    assert all_topics() == [
        "energy.generation.v1",
        "energy.generation.dlq.v1",
        "meters.readings.v1",
        "meters.readings.dlq.v1",
    ]


def test_serializers_encode_key_and_compact_json():
    from kafka.serializer import Serializer

    from ingestion.producers.events import _JsonSerializer, _KeySerializer

    key, value = _KeySerializer(), _JsonSerializer()
    # Real Serializer subclasses are what kafka-python 3 expects (no deprecation warning).
    assert isinstance(key, Serializer) and isinstance(value, Serializer)
    assert key.serialize("t", [], "solar") == b"solar"
    assert value.serialize("t", [], {"a": 1, "b": "ü"}) == b'{"a":1,"b":"\\u00fc"}'


def test_bootstrap_setting_is_cleaned():
    from ingestion.producers.events import bootstrap_servers, describe_bootstrap

    host = "abc.any.eu-central-1.mpx.prd.cloud.redpanda.com:9092"
    for raw in (host, f" {host}\n", f'"{host}"', f"https://{host}/", f"{host},"):
        assert bootstrap_servers(raw) == [host], raw
    assert bootstrap_servers("") == ["localhost:9092"]
    assert describe_bootstrap([host]) == f"1 server(s): len={len(host)} port=9092"
    assert "MISSING" in describe_bootstrap(["abc.example.com"])


def test_topics_use_broker_timestamps_and_7_day_retention():
    from ingestion.producers.kafka_tools import TOPIC_CONFIGS

    assert TOPIC_CONFIGS["message.timestamp.type"] == "LogAppendTime"
    assert TOPIC_CONFIGS["retention.ms"] == str(7 * 24 * 3600 * 1000)


# --------------------------------------------------------------------------- file fallback


def test_file_sink_writes_one_jsonl_file_per_topic_and_flush(tmp_path):
    import json as _json

    from ingestion.common.config import load_settings
    from ingestion.common.landing import LocalLandingWriter
    from ingestion.producers.events import FileSink

    settings = load_settings(
        environ={"GRIDPULSE_LANDING_BACKEND": "local", "GRIDPULSE_LOCAL_LANDING_DIR": str(tmp_path)}
    )
    now = datetime(2026, 11, 25, 15, 0, 1, 123456, tzinfo=UTC)
    sink = FileSink(writer=LocalLandingWriter(tmp_path), settings=settings, clock=lambda: now)
    publisher = Publisher(sink)
    for key, event in ec.to_events(RESPONSE, "de"):
        publisher.publish(ec.TOPIC, key, event)
    sink.flush()
    sink.flush()  # nothing buffered: no empty file
    assert len(sink.files) == 1
    path = sink.files[0]
    assert "/bronze/landing/stream/energy_generation_v1/ingest_date=2026-11-25/" in path
    assert path.endswith("energy_generation_v1_20261125T150001123456Z_00001.jsonl")
    lines = (tmp_path / path.lstrip("/")).read_text().splitlines()
    assert len(lines) == 6
    first = _json.loads(lines[0])
    assert first["topic"] == "energy.generation.v1"
    # value is the exact JSON text that would have gone to Kafka
    assert _json.loads(first["value"])["schema_version"] == "energy.generation.v1"


def test_make_sink_switches_transport():
    import pytest

    from ingestion.producers.events import FileSink, MemorySink, make_sink

    assert isinstance(make_sink({}, dry_run=True), MemorySink)
    assert isinstance(
        make_sink({"GRIDPULSE_STREAM_TRANSPORT": "file", "GRIDPULSE_LANDING_BACKEND": "local"}),
        FileSink,
    )
    with pytest.raises(ValueError):
        make_sink({"GRIDPULSE_STREAM_TRANSPORT": "pigeon"})


def test_file_folders_match_bronze_pipeline():
    import ast
    from pathlib import Path

    from ingestion.producers.events import TOPICS, FileSink, dlq_topic

    src = Path(__file__).resolve().parents[1] / "src" / "bronze" / "kafka_bronze.py"
    tree = ast.parse(src.read_text(encoding="utf-8"))
    funcs = {n.name: n for n in tree.body if isinstance(n, ast.FunctionDef)}
    ns: dict = {}
    exec(compile(ast.Module([funcs["file_entity"]], []), "x", "exec"), ns)  # noqa: S102
    for topic in [*TOPICS, *(dlq_topic(t) for t in TOPICS)]:
        assert ns["file_entity"](topic) == FileSink.entity(topic)
