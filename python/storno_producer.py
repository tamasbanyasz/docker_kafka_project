"""
Storno producer: sends house storno requests to Kafka topic "house-storno".
Full storno: {"id": 123}  or  "123"  → entire house cancelled
Partial storno: {"id": 123, "partition": 2}  → exclude partition_2 (wall) for that house
Run: python storno_producer.py 123
     python storno_producer.py 123 --partition 2
"""
import argparse
import json
import sys
from pathlib import Path

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

CONFIG_PATH = Path(__file__).resolve().parent.parent / "config.yaml"

def load_config():
    import yaml
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)

def ensure_topic(bootstrap: str, topic: str):
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    try:
        admin.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])
        print(f"Topic '{topic}' created")
    except Exception as e:
        if "exists" not in str(e).lower() and "TopicExistsException" not in str(type(e).__name__):
            raise
    finally:
        admin.close()

def main():
    parser = argparse.ArgumentParser(description="Send storno request to house-storno topic")
    parser.add_argument("id", type=int, help="House ID to storno")
    parser.add_argument("--partition", "-p", type=int, choices=[1, 2, 3],
                        default=0, help="Partition to exclude (1=foundation, 2=wall, 3=roof). 0=full storno")
    args = parser.parse_args()

    print("=== PROCESS: Storno (one-shot) | house-storno topic ===")
    cfg = load_config()
    bootstrap = cfg["kafka"]["bootstrap_servers"]
    topic = cfg.get("consumer", {}).get("storno_topic", "house-storno")

    ensure_topic(bootstrap, topic)

    producer = KafkaProducer(bootstrap_servers=bootstrap)
    if args.partition == 0:
        msg = json.dumps({"id": args.id}).encode("utf-8")
    else:
        msg = json.dumps({"id": args.id, "partition": args.partition}).encode("utf-8")
    producer.send(topic, msg)
    producer.flush()
    producer.close()

    part_str = f" partition {args.partition}" if args.partition else " (full)"
    print(f"Storno sent: id={args.id}{part_str}")

if __name__ == "__main__":
    main()
    sys.exit(0)
