# python-kafka

## Export DISPATCHER emails from Kafka

The repository includes `consume_dispatcher_emails.py`, a CLI script that reads
JSON messages from a Kafka topic and writes correlation IDs, migration run IDs,
and DISPATCHER request emails to a quoted CSV file.

### Prerequisites

- Python 3.8+
- Kafka cluster reachable from your machine
- [kafka-python](https://pypi.org/project/kafka-python/) installed:

```bash
pip install kafka-python
```

### Example usage

Consume from a topic starting at the earliest offset and write results to a
file:

```bash
python consume_dispatcher_emails.py \
  --bootstrap-servers localhost:9092 \
  --topic migration-requests \
  --output dispatcher_emails.csv
```

Key options:

- `--bootstrap-servers`: Comma-separated Kafka hosts (required)
- `--topic`: Topic to consume (required)
- `--output`: CSV file path (`-` for stdout)
- `--timeout-ms`: Idle timeout before exiting (default 5000)
- `--security-protocol`, `--sasl-mechanism`, `--sasl-username`,
  `--sasl-password`: Security options for SASL/SSL clusters
