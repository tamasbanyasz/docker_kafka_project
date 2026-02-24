"""
3-partition Kafka producer - all 3 partitions send ~1000 msg/s with matching IDs.
Partition 1: foundation, Partition 2: wall, Partition 3: roof
Each second: 1000 triplets (id 0-999, 1000-1999, ...) – all 3 partitions send same 1000 ids.
Message format: partition_N|id:X|ts:TIMESTAMP|name:NAME|task:TASK
"""
import json
import os
import random
import threading
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import yaml

CONFIG_PATH = Path(__file__).resolve().parent.parent / "config.yaml"
PROJECT_ROOT = CONFIG_PATH.parent
BASE_ID_FILE = PROJECT_ROOT / "output" / "producer_base_id.json"

BASE_ID = {"value": 0}
SEC_LOCK = threading.Lock()
LAST_SEC = {"value": -1}


def load_config():
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _delete_and_recreate_topic(bootstrap, topic, num_partitions, max_attempts=6):
    """Delete topic and recreate with num_partitions. Retries until Kafka completes deletion."""
    from kafka.errors import TopicAlreadyExistsError
    for _ in range(max_attempts):
        admin = KafkaAdminClient(bootstrap_servers=bootstrap)
        try:
            admin.create_topics([NewTopic(name=topic, num_partitions=num_partitions, replication_factor=1)])
            return True
        except TopicAlreadyExistsError:
            admin.delete_topics([topic])
        finally:
            admin.close()
        time.sleep(5)
    return False


def ensure_topic(admin_cfg):
    from kafka.errors import TopicAlreadyExistsError

    topic = admin_cfg["topic"]
    num_partitions = admin_cfg["num_partitions"]
    bootstrap = admin_cfg["bootstrap_servers"]
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)

    try:
        topics_list = admin.list_topics()
        if topic in topics_list:
            topics_meta = admin.describe_topics([topic])
            for t in topics_meta:
                if t.get("topic") == topic:
                    parts = t.get("partitions", [])
                    if len(parts) != num_partitions:
                        admin.delete_topics([topic])
                        admin.close()
                        if _delete_and_recreate_topic(bootstrap, topic, num_partitions):
                            print(f"Topic '{topic}' recreated with {num_partitions} partitions (was {len(parts)})")
                        else:
                            raise RuntimeError(
                                f"Topic '{topic}' has {len(parts)} partitions, need {num_partitions}. "
                                "Run .\\run\\reset.ps1, wait 15s, then start producer."
                            )
                    else:
                        print(f"Topic '{topic}' already exists with {num_partitions} partitions")
                    admin.close()
                    return
    except Exception:
        pass

    try:
        admin.create_topics([NewTopic(name=topic, num_partitions=num_partitions, replication_factor=1)])
        print(f"Topic '{topic}' created with {num_partitions} partitions")
    except TopicAlreadyExistsError:
        admin.delete_topics([topic])
        admin.close()
        if _delete_and_recreate_topic(bootstrap, topic, num_partitions):
            print(f"Topic '{topic}' recreated with {num_partitions} partitions")
        else:
            raise RuntimeError(
                f"Topic '{topic}' deletion is slow. Run .\\run\\reset.ps1, wait 15s, then start producer."
            )
    except Exception as e:
        if "TopicExistsException" in str(type(e).__name__) or "already exists" in str(e).lower():
            admin.delete_topics([topic])
            admin.close()
            if _delete_and_recreate_topic(bootstrap, topic, num_partitions):
                print(f"Topic '{topic}' recreated with {num_partitions} partitions")
            else:
                raise RuntimeError("Topic recreation failed. Run .\\run\\reset.ps1, wait 15s.")
        else:
            raise
    finally:
        admin.close()


def ensure_storno_topic(cfg):
    """Create house-storno topic for consumer storno goroutine."""
    from kafka.errors import TopicAlreadyExistsError
    storno_topic = cfg.get("consumer", {}).get("storno_topic", "house-storno")
    admin = KafkaAdminClient(bootstrap_servers=cfg["kafka"]["bootstrap_servers"])
    try:
        admin.create_topics([NewTopic(name=storno_topic, num_partitions=1, replication_factor=1)])
        print(f"Topic '{storno_topic}' created")
    except TopicAlreadyExistsError:
        pass
    except Exception as e:
        if "exists" not in str(e).lower():
            raise
    finally:
        admin.close()


def make_message(part_id: int, corr_id: int, name: str, task: str) -> str:
    ts = time.strftime("%Y%m%d_%H%M%S")
    return f"partition_{part_id}|id:{corr_id}|ts:{ts}|name:{name}|task:{task}"


def _load_base_id() -> int:
    try:
        if BASE_ID_FILE.exists():
            data = json.loads(BASE_ID_FILE.read_text(encoding="utf-8"))
            return int(data.get("base_id", 0))
    except (json.JSONDecodeError, OSError):
        pass
    return 0


def _save_base_id(value: int) -> None:
    """Crash-safe: write to .tmp, sync to disk, then atomic rename."""
    try:
        BASE_ID_FILE.parent.mkdir(parents=True, exist_ok=True)
        data = json.dumps({"base_id": value})
        tmp_path = BASE_ID_FILE.with_suffix(".json.tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        tmp_path.replace(BASE_ID_FILE)
    except OSError:
        pass


def get_ids_for_second(cfg) -> tuple[int, int]:
    msgs_per_sec = cfg["producer"]["msgs_per_sec"]
    now = int(time.time())
    with SEC_LOCK:
        if now != LAST_SEC["value"]:
            LAST_SEC["value"] = now
            BASE_ID["value"] += msgs_per_sec
            _save_base_id(BASE_ID["value"])
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
    print("=== PROCESS: Producer (Python) | Kafka -> houses ===")
    cfg = load_config()
    kafka_cfg = cfg["kafka"]
    prod_cfg = cfg["producer"]
    v = os.environ.get("MSGS_PER_SEC")
    if v:
        prod_cfg["msgs_per_sec"] = int(v)

    BASE_ID["value"] = _load_base_id()
    if BASE_ID["value"] > 0:
        print(f"Resuming from base_id={BASE_ID['value']}")

    ensure_topic(kafka_cfg)
    ensure_storno_topic(cfg)

    acks_cfg = prod_cfg.get("acks", 1)
    retries_cfg = prod_cfg.get("retries", 3)
    producer = KafkaProducer(
        bootstrap_servers=kafka_cfg["bootstrap_servers"],
        compression_type=prod_cfg.get("compression_type", "snappy"),
        linger_ms=prod_cfg.get("linger_ms", 5),
        batch_size=prod_cfg.get("batch_size", 8192),
        buffer_memory=prod_cfg.get("buffer_memory", 8388608),
        acks=acks_cfg,
        retries=retries_cfg,
        api_version_auto_timeout_ms=5000,
        request_timeout_ms=10000,
        metadata_max_age_ms=2000,
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
