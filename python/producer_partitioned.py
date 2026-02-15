"""
3-partition Kafka producer - all 3 partitions send ~1000 msg/s with matching IDs.
Partition 1: foundation, Partition 2: wall, Partition 3: roof
Each second: 1000 triplets (id 0-999, 1000-1999, ...) – all 3 partitions send same 1000 ids.
Message format: partition_N|id:X|ts:TIMESTAMP|name:NAME|task:TASK
"""
import random
import threading
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import yaml

CONFIG_PATH = Path(__file__).resolve().parent.parent / "config.yaml"

BASE_ID = {"value": 0}
SEC_LOCK = threading.Lock()
LAST_SEC = {"value": -1}


def load_config():
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


def ensure_topic(admin_cfg):
    from kafka.errors import TopicAlreadyExistsError
    topic = admin_cfg["topic"]
    num_partitions = admin_cfg["num_partitions"]
    admin = KafkaAdminClient(bootstrap_servers=admin_cfg["bootstrap_servers"])
    try:
        admin.create_topics([NewTopic(name=topic, num_partitions=num_partitions, replication_factor=1)])
        print(f"Topic '{topic}' created with {num_partitions} partitions")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic}' already exists")
    except Exception as e:
        if "TopicExistsException" in str(type(e).__name__) or "already exists" in str(e).lower():
            print(f"Topic '{topic}' already exists")
        else:
            raise
    finally:
        admin.close()


def make_message(part_id: int, corr_id: int, name: str, task: str) -> str:
    ts = time.strftime("%Y%m%d_%H%M%S")
    return f"partition_{part_id}|id:{corr_id}|ts:{ts}|name:{name}|task:{task}"


def get_ids_for_second(cfg) -> tuple[int, int]:
    msgs_per_sec = cfg["producer"]["msgs_per_sec"]
    now = int(time.time())
    with SEC_LOCK:
        if now != LAST_SEC["value"]:
            LAST_SEC["value"] = now
            BASE_ID["value"] += msgs_per_sec
        base = BASE_ID["value"] - msgs_per_sec
        return base, base + msgs_per_sec


def thread_part1(producer, cfg):
    names = ["alice", "bob", "charlie", "diana", "eve"]
    topic = cfg["kafka"]["topic"]
    msgs_per_sec = cfg["producer"]["msgs_per_sec"]
    total = 0
    while True:
        base, end = get_ids_for_second(cfg)
        for i in range(msgs_per_sec):
            corr_id = base + i
            name = random.choice(names)
            msg = make_message(1, corr_id, name, "foundation")
            producer.send(topic, value=msg.encode("utf-8"), partition=0)
            total += 1
            if total % 5000 == 0:
                print(f"  [Partition 1] {total} messages")
        time.sleep(1)


def thread_part2(producer, cfg):
    names = ["wall_alice", "wall_bob"]
    topic = cfg["kafka"]["topic"]
    msgs_per_sec = cfg["producer"]["msgs_per_sec"]
    total = 0
    while True:
        base, end = get_ids_for_second(cfg)
        for i in range(msgs_per_sec):
            corr_id = base + i
            msg = make_message(2, corr_id, random.choice(names), "wall")
            producer.send(topic, value=msg.encode("utf-8"), partition=1)
            total += 1
            if total % 5000 == 0:
                print(f"  [Partition 2] {total} messages")
        time.sleep(1)


def thread_part3(producer, cfg):
    names = ["roof_alice", "roof_bob"]
    topic = cfg["kafka"]["topic"]
    msgs_per_sec = cfg["producer"]["msgs_per_sec"]
    total = 0
    while True:
        base, end = get_ids_for_second(cfg)
        for i in range(msgs_per_sec):
            corr_id = base + i
            msg = make_message(3, corr_id, random.choice(names), "roof")
            producer.send(topic, value=msg.encode("utf-8"), partition=2)
            total += 1
            if total % 5000 == 0:
                print(f"  [Partition 3] {total} messages")
        time.sleep(1)


def main():
    cfg = load_config()
    kafka_cfg = cfg["kafka"]
    prod_cfg = cfg["producer"]

    ensure_topic(kafka_cfg)

    producer = KafkaProducer(
        bootstrap_servers=kafka_cfg["bootstrap_servers"],
        compression_type=prod_cfg.get("compression_type", "snappy"),
        linger_ms=prod_cfg.get("linger_ms", 5),
        batch_size=prod_cfg.get("batch_size", 32768),
        api_version_auto_timeout_ms=5000,
        request_timeout_ms=10000,
    )

    msgs_per_sec = prod_cfg["msgs_per_sec"]
    print(f"3 threads: each sends {msgs_per_sec} msg/s with matching IDs")
    print(f"Compression: {prod_cfg.get('compression_type', 'snappy')}, linger_ms: {prod_cfg.get('linger_ms', 5)}")
    print("Ctrl+C to exit\n")

    t1 = threading.Thread(target=thread_part1, args=(producer, cfg), daemon=True)
    t2 = threading.Thread(target=thread_part2, args=(producer, cfg), daemon=True)
    t3 = threading.Thread(target=thread_part3, args=(producer, cfg), daemon=True)

    t1.start()
    t2.start()
    t3.start()

    try:
        t1.join()
        t2.join()
        t3.join()
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()
        print("\nStopped.")


if __name__ == "__main__":
    main()
