"""
Consume Kafka messages containing JSON payloads, filter on DISPATCHER requests,
and emit selected fields to CSV with quoted values.

Example usage:
    python consume_dispatcher_emails.py --bootstrap-servers localhost:9092 \
        --topic migration-requests --output dispatcher_emails.csv

The consumer reads from the earliest offset and stops after the configured idle
timeout (default: 5 seconds with no new messages). Only messages with
`requestHeader.type == "DISPATCHER"` are written to the CSV output.
"""

import argparse
import csv
import json
import sys
from typing import Any, Dict, Iterable, Optional

from kafka import KafkaConsumer


DISPATCHER_TYPE = "DISPATCHER"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap-servers",
        required=True,
        help="Comma-separated list of Kafka bootstrap servers (host:port)",
    )
    parser.add_argument("--topic", required=True, help="Kafka topic to consume from")
    parser.add_argument(
        "--group-id",
        default="dispatcher-email-extractor",
        help="Consumer group ID used when connecting to Kafka",
    )
    parser.add_argument(
        "--output",
        default="-",
        help="Path to output CSV file (use '-' for stdout)",
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=5000,
        help=(
            "How long the consumer should wait without receiving new messages "
            "before stopping"
        ),
    )
    parser.add_argument(
        "--security-protocol",
        default="PLAINTEXT",
        help="Kafka security protocol (e.g., PLAINTEXT, SASL_SSL)",
    )
    parser.add_argument(
        "--sasl-mechanism",
        default=None,
        help="SASL mechanism when using SASL_* security (e.g., PLAIN, SCRAM-SHA-512)",
    )
    parser.add_argument(
        "--sasl-username",
        default=None,
        help="SASL username when using authenticated clusters",
    )
    parser.add_argument(
        "--sasl-password",
        default=None,
        help="SASL password when using authenticated clusters",
    )
    return parser.parse_args()


def open_output(path: str):
    if path == "-":
        return sys.stdout
    return open(path, "w", newline="", encoding="utf-8")


def extract_dispatcher_record(payload: Dict[str, Any]) -> Optional[Dict[str, str]]:
    try:
        header = payload["requestHeader"]
        body = payload["requestBody"]
    except (KeyError, TypeError):
        return None

    if header.get("type") != DISPATCHER_TYPE:
        return None

    email = body.get("email")
    if not email:
        return None

    return {
        "correlationId": header.get("correlationId", ""),
        "migrationRunId": header.get("migrationRunId", ""),
        "email": email,
    }


def consume_messages(args: argparse.Namespace) -> Iterable[Dict[str, str]]:
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: v.decode("utf-8"),
        consumer_timeout_ms=args.timeout_ms,
        group_id=args.group_id,
        security_protocol=args.security_protocol,
        sasl_mechanism=args.sasl_mechanism,
        sasl_plain_username=args.sasl_username,
        sasl_plain_password=args.sasl_password,
    )

    try:
        for message in consumer:
            try:
                payload = json.loads(message.value)
            except json.JSONDecodeError:
                print(
                    f"Skipping non-JSON message at offset {message.offset}",
                    file=sys.stderr,
                )
                continue

            record = extract_dispatcher_record(payload)
            if record:
                yield record
    finally:
        consumer.close()


def write_csv(records: Iterable[Dict[str, str]], output_file) -> None:
    fieldnames = ["correlationId", "migrationRunId", "email"]
    writer = csv.DictWriter(
        output_file, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, lineterminator="\n"
    )
    writer.writeheader()
    for record in records:
        writer.writerow(record)


def main() -> None:
    args = parse_args()
    with open_output(args.output) as output_file:
        write_csv(consume_messages(args), output_file)


if __name__ == "__main__":
    main()
