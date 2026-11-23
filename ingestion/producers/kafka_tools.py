"""Small Kafka helpers that work the same locally and in the cloud (P4.1, P4.3).

Uses the same ``GRIDPULSE_KAFKA_*`` environment variables as the producers, so no extra
CLI (``rpk``) is needed on the laptop.

    python -m ingestion.producers.kafka_tools create-topics          # all GridPulse topics
    python -m ingestion.producers.kafka_tools list
    python -m ingestion.producers.kafka_tools consume energy.generation.v1 -n 3
"""

from __future__ import annotations

import argparse
import json
import os
import sys

from ingestion.producers.events import TOPICS, connection_options, dlq_topic

PARTITIONS = 3
RETENTION_MS = 7 * 24 * 3600 * 1000  # 7 days


def all_topics() -> list[str]:
    return [t for topic in TOPICS for t in (topic, dlq_topic(topic))]


def create_topics(env: dict[str, str]) -> None:
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    admin = KafkaAdminClient(**connection_options(env))
    try:
        existing = set(admin.list_topics())
        missing = [t for t in all_topics() if t not in existing]
        if missing:
            try:
                admin.create_topics(
                    [
                        NewTopic(
                            t,
                            PARTITIONS,
                            replication_factor=-1,
                            topic_configs={"retention.ms": str(RETENTION_MS)},
                        )
                        for t in missing
                    ]
                )
            except TopicAlreadyExistsError:
                pass
        for topic in all_topics():
            print(f"{'created' if topic in missing else 'exists '}  {topic}")
    finally:
        admin.close()


def list_topics(env: dict[str, str]) -> None:
    from kafka.admin import KafkaAdminClient

    admin = KafkaAdminClient(**connection_options(env))
    try:
        for topic in sorted(admin.list_topics()):
            print(topic)
    finally:
        admin.close()


def consume(env: dict[str, str], topic: str, n: int, timeout_s: float) -> int:
    """Print up to n messages from the start of the topic (no consumer group, no commits)."""
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        topic,
        **connection_options(env),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=None,
        consumer_timeout_ms=int(timeout_s * 1000),
    )
    seen = 0
    try:
        for msg in consumer:
            value = json.loads(msg.value)
            print(f"partition={msg.partition} offset={msg.offset} key={msg.key.decode()}")
            print("  " + json.dumps(value, ensure_ascii=False))
            seen += 1
            if seen >= n:
                break
    finally:
        consumer.close()
    print(f"\n{seen} message(s) read from {topic}")
    return seen


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="GridPulse Kafka helpers.")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("create-topics", help="create all topics (3 partitions, 7-day retention)")
    sub.add_parser("list", help="list topics")
    c = sub.add_parser("consume", help="print messages from the start of a topic")
    c.add_argument("topic")
    c.add_argument("-n", type=int, default=5)
    c.add_argument("--timeout", type=float, default=15, help="seconds to wait for messages")
    args = parser.parse_args(argv)

    env = dict(os.environ)
    if args.command == "create-topics":
        create_topics(env)
    elif args.command == "list":
        list_topics(env)
    else:
        consume(env, args.topic, args.n, args.timeout)
    return 0


if __name__ == "__main__":
    code = main()
    if code:
        sys.exit(code)
