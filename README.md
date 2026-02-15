# Kafka House Triplet Pipeline

A Python producer and Go consumer that send and receive correlated "house triplets" over Apache Kafka. Each triplet consists of three messages (foundation, wall, roof) with matching IDs, emitted across 3 topic partitions. The consumer joins them by ID and writes JSONL output.

---

## Project Structure

```
docker_kafka_project/
├── config.yaml          # Kafka, producer, consumer configuration
├── docker-compose.yml   # Apache Kafka (KRaft mode)
├── requirements.txt    # Python dependencies
├── README.md
├── python/              # Python source code
│   ├── producer_partitioned.py   # 3-partition producer (main)
│   ├── producer.py              # Simple producer
│   └── consumer.py               # Simple consumer
├── go/                  # Go source code
│   ├── main.go          # Triplet-joining consumer
│   ├── go.mod
│   └── go.sum
├── run/                 # Startup scripts
│   ├── kafka.ps1        # Start Kafka
│   ├── producer.ps1     # Start producer
│   └── consumer.ps1     # Start consumer
└── output/              # Consumer output
    ├── houses.jsonl     # Triplet JSONL output
    └── consumer_offsets.json  # Crash-safe offset file (partition → last offset)
```

---

## Prerequisites

- **Docker Desktop 4.58.0** (or compatible) – for running Apache Kafka
- **Python 3.10+** – for the producer
- **Go 1.21+** – for the consumer

---

## Installation

```powershell
# Create and activate virtual environment
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

---

## How to Start the Project

Run these commands from the **project root** (`docker_kafka_project/`). Use **two terminals**: one for the consumer, one for the producer.

### Terminal 1 – Kafka

```powershell
cd c:\Users\Tulajdonos\Desktop\docker_kafka_project
.\run\kafka.ps1
```

Or:
```powershell
cd c:\Users\Tulajdonos\Desktop\docker_kafka_project
docker compose up -d
```

### Terminal 2 – Consumer (start first)

```powershell
cd c:\Users\Tulajdonos\Desktop\docker_kafka_project
.\run\consumer.ps1
```

### Terminal 3 – Producer

```powershell
cd c:\Users\Tulajdonos\Desktop\docker_kafka_project
.\run\producer.ps1
```

> **Note:** The producer script activates the venv automatically. Start the consumer first, then the producer. The producer sends ~3000 msg/s (1000 per partition). Completed triplets are written to `output/houses.jsonl`.

---

## Technical Details

### Kafka & Docker

- **Image:** `apache/kafka:latest`
- **Mode:** KRaft (no Zookeeper)
- **Bootstrap:** `127.0.0.1:9092`
- **Topic:** `teszt-partitioned` (3 partitions)
- **Listeners:** `PLAINTEXT://0.0.0.0:9092`, controller on `9093`
- **Advertised:** `PLAINTEXT://127.0.0.1:9092` (use `127.0.0.1` for local clients)

### Producer

- 3 threads, one per partition
- ~1000 messages/second per partition (configurable via `config.yaml`)
- Snappy compression, `linger_ms=5`, `batch_size=32768`
- Message format: `partition_N|id:X|ts:TIMESTAMP|name:NAME|task:TASK`

### Consumer

- Reads from all 3 partitions (simple consumer, reliable)
- **Offset persistence:** Saves last read position per partition to `output/consumer_offsets.json`; on restart, continues from where it left off
- Joins messages by ID into triplet (foundation + wall + roof)
- Writes JSONL to `output/houses.jsonl`
- Configurable: `flush_every`, `log_every`, `output_file`

### Offset File – Crash Safety

The offset file is written in a crash-safe way:

1. **Write to temp file** (`consumer_offsets.json.tmp`)
2. **`Sync()`** – flushes data to disk
3. **`Rename`** – atomic rename to final filename

If the process crashes during write, the original `consumer_offsets.json` stays intact. On restart, the consumer uses the last valid state. Delete `output/consumer_offsets.json` to re-read from the beginning.

### Alternative Offset Solutions

| Method | Description | Pros | Cons |
|--------|-------------|------|------|
| **File (current)** | Offset stored in JSON file | Simple, no extra dependencies, works with any Kafka setup | Single consumer only, file must be writable |
| **Kafka Consumer Group** | Kafka stores offsets in `__consumer_offsets` topic | Built-in, automatic, scales to multiple consumers | May not work with KRaft/older setups; this project has `consumer_group: true` as optional (unstable in some envs) |
| **Database** | Store offsets in PostgreSQL, MySQL, etc. | Shared state, transactional, queryable | Extra dependency, more complex |
| **Redis** | Store offsets in Redis | Fast, shared across instances | Extra service, persistence config needed |

### Crash Safety (Output & Shutdown)

The consumer is designed for safe shutdown and minimal data loss:

1. **Signal handling:** Listens for `SIGINT` (Ctrl+C) and `SIGTERM`
2. **Graceful shutdown:** On signal, stops new processing and flushes buffers
3. **Buffered I/O:** Uses `bufio.Writer`; flushes every N triplets (default: 100)
4. **Exit flush:** Before exit, flushes remaining buffer and calls `Sync()` so data reaches disk
5. **Shutdown sequence:** `Flush()` → `Sync()` → `Close()` on the output file

If the process is killed abruptly (e.g. `kill -9`), at most `flush_every - 1` triplets may be lost from the buffer.

---

## Configuration (`config.yaml`)

```yaml
kafka:
  bootstrap_servers: "127.0.0.1:9092"
  topic: "teszt-partitioned"
  num_partitions: 3

producer:
  msgs_per_sec: 1000
  compression_type: "snappy"
  linger_ms: 5
  batch_size: 32768

consumer:
  output_file: "../output/houses.jsonl"
  flush_every: 100
  log_every: 1000
  # Offsets saved to output/consumer_offsets.json; delete that file to re-read from beginning
```

---

## Stopping

- **Producer/Consumer:** Ctrl+C (handled gracefully)
- **Kafka:** `docker compose down`
