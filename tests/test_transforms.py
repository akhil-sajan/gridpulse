"""Spark unit tests for the shared pipeline transformations (P5.3).

They run the exact functions the Lakeflow pipeline uses on a local SparkSession
(fixture ``spark`` in conftest.py), with small hand-made inputs.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402

from src.utils import transforms as T  # noqa: E402
from src.utils.timeutils import quarter_hours_in_local_day  # noqa: E402


def utc(*args) -> datetime:
    return datetime(*args, tzinfo=UTC)


def ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


# --------------------------------------------------------------------------- rules


def test_failed_rules_names_every_broken_rule_and_null_counts_as_broken(spark):
    rules = {"positive": "value > 0", "small": "value < 10"}
    df = spark.createDataFrame([(5,), (-1,), (20,), (None,)], "value INT")
    rows = df.select(
        "value",
        T.failed_rules(rules).alias("failed"),
        T.all_pass(rules).alias("ok"),
    ).collect()
    got = {r.value: (sorted(r.failed), r.ok) for r in rows}
    assert got[5] == ([], True)
    assert got[-1] == (["positive"], False)
    assert got[20] == (["small"], False)
    # NULL makes both rules NULL -> both broken, never silently passed.
    assert got[None] == (["positive", "small"], False)


def test_guarded_wraps_every_rule():
    assert T.guarded({"a": "x > 0"}) == {"a": "coalesce(x > 0, false)"}


# --------------------------------------------------------------------------- time / DST


def local_times(spark, instants):
    df = spark.createDataFrame([(t,) for t in instants], "ts_utc TIMESTAMP")
    return [
        (r.ts_local.strftime("%Y-%m-%d %H:%M"), r.local_date)
        for r in T.with_local_time(df).orderBy("ts_utc").collect()
    ]


def test_local_time_spring_forward(spark):
    # 29 Mar 2026: 02:00 local does not exist; 01:00 UTC is already 03:00 CEST.
    got = local_times(spark, [utc(2026, 3, 29, 0, 45), utc(2026, 3, 29, 1, 0)])
    assert got == [("2026-03-29 01:45", date(2026, 3, 29)), ("2026-03-29 03:00", date(2026, 3, 29))]


def test_local_time_fall_back_gives_same_local_time_twice(spark):
    # 25 Oct 2026: 02:30 local happens twice; the UTC keys stay distinct.
    got = local_times(spark, [utc(2026, 10, 25, 0, 30), utc(2026, 10, 25, 1, 30)])
    assert [g[0] for g in got] == ["2026-10-25 02:30", "2026-10-25 02:30"]


def test_local_date_crosses_midnight_before_utc(spark):
    got = local_times(spark, [utc(2026, 6, 30, 22, 15)])
    assert got == [("2026-07-01 00:15", date(2026, 7, 1))]


@pytest.mark.parametrize("day", [date(2026, 3, 29), date(2026, 10, 25), date(2026, 11, 25)])
def test_quarter_hours_per_local_day_match_timeutils(spark, day):
    """A full day of quarter-hours in UTC around ``day`` -> rows on that local date
    must be 92, 100 or 96, the same as the plain-Python rule in timeutils."""
    start = ms(utc(day.year, day.month, day.day)) - 4 * 3600 * 1000
    df = (
        spark.range(0, 32 * 4)  # 32 h of quarter-hours, covers the local day
        .select(F.timestamp_millis(F.lit(start) + F.col("id") * 900_000).alias("ts_utc"))
        .transform(T.with_local_time)
    )
    assert df.where(F.col("local_date") == F.lit(day)).count() == quarter_hours_in_local_day(day)


# --------------------------------------------------------------------------- unpivot

SMARD_BRONZE = (
    "series_name STRING, filter_id INT, region STRING, resolution STRING, "
    "series ARRAY<ARRAY<DOUBLE>>, _source_file STRING, _file_modified_at TIMESTAMP"
)


def test_smard_points_one_row_per_published_value(spark):
    t0 = utc(2026, 11, 25, 12, 0)
    series = [[float(ms(t0)), 100.0], [ms(t0) + 900_000.0, 110.5], [ms(t0) + 1_800_000.0, None]]
    bronze = spark.createDataFrame(
        [("solar", 4068, "DE", "quarterhour", series, "f.json", t0)], SMARD_BRONZE
    )
    rows = T.smard_points(bronze, "generation", "MWh").orderBy("ts_utc").collect()
    # The NULL (not yet published) quarter-hour is not a row.
    assert [(r.ts_utc, r.value) for r in rows] == [
        (t0.replace(tzinfo=None), 100.0),
        (utc(2026, 11, 25, 12, 15).replace(tzinfo=None), 110.5),
    ]
    assert {(r.dataset, r.unit, r.series_name, r._source_file) for r in rows} == {
        ("generation", "MWh", "solar", "f.json")
    }


WEATHER_BRONZE = (
    "location_code STRING, latitude DOUBLE, longitude DOUBLE, "
    "hourly STRUCT<time: ARRAY<STRING>, shortwave_radiation: ARRAY<DOUBLE>, "
    "cloud_cover: ARRAY<DOUBLE>, wind_speed_100m: ARRAY<DOUBLE>, "
    "temperature_2m: ARRAY<DOUBLE>>, _source_file STRING, _file_modified_at TIMESTAMP"
)


def test_weather_points_zips_hourly_arrays(spark):
    hourly = {
        "time": ["2026-11-25T12:00", "2026-11-25T13:00", "2026-11-25T14:00"],
        "shortwave_radiation": [120.0, None, None],
        "cloud_cover": [80.0, 90.0, None],
        "wind_speed_100m": [5.5, 6.0, None],
        "temperature_2m": [3.2, 3.0, None],
    }
    bronze = spark.createDataFrame(
        [("DE-HE", 50.6, 9.7, hourly, "w.json", utc(2026, 11, 25, 15))], WEATHER_BRONZE
    )
    rows = T.weather_points(bronze).orderBy("ts_utc").collect()
    # 14:00 has no values at all -> not a row; 13:00 keeps its partial values.
    assert [r.ts_utc.hour for r in rows] == [12, 13]
    assert rows[0].cloud_cover == 80.0 and rows[0].temperature_2m == 3.2
    assert rows[1].shortwave_radiation is None and rows[1].wind_speed_100m == 6.0


def test_weather_points_keeps_extra_columns(spark):
    hourly = {
        "time": ["2026-11-26T00:00"],
        "shortwave_radiation": [0.0],
        "cloud_cover": [10.0],
        "wind_speed_100m": [3.0],
        "temperature_2m": [1.0],
    }
    bronze = spark.createDataFrame(
        [("DE-BY", 48.1, 11.6, hourly, "f.json", utc(2026, 11, 25))], WEATHER_BRONZE
    ).withColumn("fetched_at", F.lit(utc(2026, 11, 25, 18)))
    row = T.weather_points(bronze, ("fetched_at",)).collect()[0]
    assert row.fetched_at == utc(2026, 11, 25, 18).replace(tzinfo=None)


# --------------------------------------------------------------------------- stream

RAW = (
    "value STRING, kafka_timestamp TIMESTAMP, _ingested_at TIMESTAMP, "
    "topic STRING, partition INT, offset BIGINT"
)
METER_PAYLOAD = "meter_id STRING, energy_kwh DOUBLE, interval_minutes INT, replay_id STRING"
METER_RULES = {
    "event_id_present": "event_id IS NOT NULL",
    "event_time_parsed": "event_ts_utc IS NOT NULL",
    "meter_id_present": "meter_id IS NOT NULL",
    "energy_in_range": "energy_kwh BETWEEN 0 AND 50",
    "schema_version_ok": "schema_ok",
}


def message(event_id, ts, kwh=0.25, meter="MAC1", version="meters.readings.v1"):
    return json.dumps(
        {
            "event_id": event_id,
            "event_ts_utc": ts,
            "produced_at": "2026-11-25T12:00:00Z",
            "schema_version": version,
            "source": "test",
            "payload": json.dumps(
                {"meter_id": meter, "energy_kwh": kwh, "interval_minutes": 30, "replay_id": "r"}
            ),
        }
    )


def raw_rows(spark, values):
    now = utc(2026, 11, 25, 12)
    return spark.createDataFrame(
        [(v, now, now, "meters.readings.v1", 0, i) for i, v in enumerate(values)], RAW
    )


def test_parse_events_good_bad_and_wrong_version(spark):
    raw = raw_rows(
        spark,
        [
            message("a", "2013-01-01T00:00:00Z"),
            "this is not json",
            message("c", "2013-01-01T00:30:00Z", version="meters.readings.v9"),
            message("d", "2013-01-01T01:00:00Z", kwh=-3.0),
        ],
    )
    parsed = T.parse_events(raw, METER_PAYLOAD, "meters.readings.v1")
    rows = {r.offset: r for r in parsed.withColumn("failed", T.failed_rules(METER_RULES)).collect()}
    good = rows[0]
    assert good.event_id == "a" and good.meter_id == "MAC1" and good.energy_kwh == 0.25
    assert good.event_ts_utc == datetime(2013, 1, 1) and good.failed == []
    # Unparseable: every field NULL, every rule broken, raw text kept for quarantine.
    assert len(rows[1].failed) == len(METER_RULES) and rows[1].raw_value == "this is not json"
    assert rows[2].failed == ["schema_version_ok"]
    assert rows[3].failed == ["energy_in_range"]


def test_streaming_dedup_and_replay_adds_nothing(spark, tmp_path):
    """dedup_events runs only on streams (as in the pipeline). Two micro-batch runs on
    one checkpoint: an exact re-send in the same batch is dropped, replaying the first
    file again adds nothing, and only the genuinely new event is added."""
    src, ckpt, out = tmp_path / "src", tmp_path / "ckpt", tmp_path / "out"
    src.mkdir()
    first = [
        message("a", "2013-01-01T00:00:00Z"),
        message("a", "2013-01-01T00:00:00Z"),  # exact re-send
        message("b", "2013-01-01T00:30:00Z"),
    ]

    def write(name, values):
        now = "2026-11-25T12:00:00Z"
        lines = [
            json.dumps(
                {
                    "value": v,
                    "kafka_timestamp": now,
                    "_ingested_at": now,
                    "topic": "t",
                    "partition": 0,
                    "offset": i,
                }
            )
            for i, v in enumerate(values)
        ]
        (src / name).write_text("\n".join(lines))

    def run():
        stream = spark.readStream.schema(RAW).json(str(src))
        parsed = T.parse_events(stream, METER_PAYLOAD, "meters.readings.v1")
        query = (
            T.dedup_events(parsed, "event_ts_utc", "30 minutes")
            .writeStream.format("parquet")
            .option("checkpointLocation", str(ckpt))
            .trigger(availableNow=True)
            .start(str(out))
        )
        query.awaitTermination()
        result = spark.read.parquet(str(out))
        assert not set(T.HELPER_COLUMNS) & set(result.columns)
        return sorted(r.event_id for r in result.collect())

    write("1.json", first)
    assert run() == ["a", "b"]
    write("2.json", [*first, message("c", "2013-01-01T01:00:00Z")])  # replay + 1 new
    assert run() == ["a", "b", "c"]


def test_meter_consumption_30min_sums_per_half_hour(spark):
    t = datetime(2013, 1, 1)
    readings = spark.createDataFrame(
        [
            ("MAC1", t, 0.2, t),
            ("MAC2", t, 0.3, t),
            ("MAC1", datetime(2013, 1, 1, 0, 30), 0.1, t),
        ],
        "meter_id STRING, event_ts_utc TIMESTAMP, energy_kwh DOUBLE, _ingested_at TIMESTAMP",
    )
    rows = T.meter_consumption_30min(readings).orderBy("window_start_utc").collect()
    assert [(r.window_start_utc.minute, r.total_kwh, r.meters, r.readings) for r in rows] == [
        (0, 0.5, 2, 2),
        (30, 0.1, 1, 1),
    ]
    assert rows[0].window_end_utc == datetime(2013, 1, 1, 0, 30)
