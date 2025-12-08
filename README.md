# python-kafka

## Export DISPATCHER emails from Kafka

The repository includes `consume_dispatcher_emails.py`, a CLI script that reads
JSON messages from a Kafka topic and writes correlation IDs, migration run IDs,
and DISPATCHER request emails to a quoted CSV file.

### Prerequisites

- Python 3.8+
- Kafka cluster reachable from your machine
- [kafka-python](https://pypi.org/project/kafka-python/) installed
- python-snappy installed (for production)

### Dependencies
All dependecies should be listed in the pyproject.toml file. To install them just run
```bash
pip install .
```
in the root folder.

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

### Running from an IDE with a config file

If you prefer not to supply command-line arguments (for example, when running
from PyCharm), copy `config.example.json` to `config.json` and fill in your
Kafka connection details:

```json
{
  "server": "your-kafka-server:9092",
  "username": "your-username",
  "password": "your-password",
  "topic": "migration-requests",
  "output": "dispatcher_emails.csv",
  "security_protocol": "SASL_SSL",
  "sasl_mechanism": "PLAIN"
}
```

Then run the script without arguments:

```bash
python consume_dispatcher_emails.py
```

The script will read the JSON config and use those values for the Kafka server,
SASL credentials, topic, and output path. This mirrors the Kafka UI settings
where `security.protocol` is `SASL_SSL`, the JAAS config carries your
`username` and `password`, and `bootstrapServers` matches the `server` value
from the config file.

### Setting Kafka credentials

Provide your server, username, and password with the security-related CLI
flags. For a SASL/SSL cluster using the PLAIN mechanism:

```bash
python consume_dispatcher_emails.py \
  --bootstrap-servers your.kafka.host:9093 \
  --topic migration-requests \
  --security-protocol SASL_SSL \
  --sasl-mechanism PLAIN \
  --sasl-username "your-username" \
  --sasl-password "your-password" \
  --output dispatcher_emails.csv
```

Adjust `--security-protocol` and `--sasl-mechanism` to match your cluster's
configuration (for example, `SASL_PLAINTEXT` with `SCRAM-SHA-512`). Leave the
SASL flags empty when connecting to unauthenticated clusters.
