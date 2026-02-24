"""
Periodically storno random houses from houses.jsonl with random partition.
Usage: python storno_loop.py [--interval 10] [--houses ../output/houses.jsonl]
Run with producer + consumer to test the 3-process flow. Ctrl+C to stop.
"""
import argparse
import json
import random
import signal
import sys
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

CONFIG_PATH = Path(__file__).resolve().parent.parent / "config.yaml"
DEFAULT_HOUSES = Path(__file__).resolve().parent.parent / "output" / "houses.jsonl"


def load_config():
    import yaml
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


def ensure_topic(bootstrap: str, topic: str):
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    try:
        admin.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])
    except Exception:
        pass
    finally:
        admin.close()


def pick_random_house(houses_path: Path, max_lines: int = 2000):
    """Use deque for bounded memory; regex for speed (no full JSON parse)."""
    if not houses_path.exists():
        return None
    import re
    from collections import deque
    id_re = re.compile(r'"id"\s*:\s*(\d+)')
    ids = deque(maxlen=max_lines)
    try:
        with open(houses_path, encoding="utf-8") as f:
            for line in f:
                m = id_re.search(line)
                if m:
                    ids.append(int(m.group(1)))
    except OSError:
        return None
    if not ids:
        return None
    return random.choice(list(ids))


def main():
    parser = argparse.ArgumentParser(description="Periodically storno random houses with random partition")
    parser.add_argument("--interval", "-i", type=float, default=15, help="Seconds between stornos (default 15)")
    parser.add_argument("--houses", type=Path, default=DEFAULT_HOUSES, help="Path to houses.jsonl")
    args = parser.parse_args()

    print("=== PROCESS: Storno loop (periodic) | house-storno topic ===")
    cfg = load_config()
    bootstrap = cfg["kafka"]["bootstrap_servers"]
    topic = cfg.get("consumer", {}).get("storno_topic", "house-storno")
    ensure_topic(bootstrap, topic)

    producer = KafkaProducer(bootstrap_servers=bootstrap)
    running = True

    def stop(sig, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    print(f"Interval: {args.interval}s | Houses: {args.houses}")
    print("Ctrl+C to stop\n")

    count = 0
    while running:
        house_id = pick_random_house(args.houses)
        if house_id is not None:
            partition = random.randint(1, 3)
            msg = json.dumps({"id": house_id, "partition": partition}).encode("utf-8")
            producer.send(topic, msg)
            producer.flush()
            count += 1
            print(f"[{count}] Storno: id={house_id} partition={partition}")
        else:
            print("(No houses yet, waiting...)")
        time.sleep(args.interval)

    producer.close()
    print(f"\nStopped. Sent {count} stornos.")


if __name__ == "__main__":
    main()
    sys.exit(0)
