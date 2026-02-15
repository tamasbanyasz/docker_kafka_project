"""
Simple Kafka consumer - reads messages from a topic.
Run: python consumer.py
"""
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP_SERVERS = "127.0.0.1:9092"
TOPIC = "teszt-topic"


def main():
    print("Connecting to Kafka...")
    for attempt in range(30):
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                auto_offset_reset="earliest",
                value_deserializer=lambda v: v.decode("utf-8"),
                api_version_auto_timeout_ms=5000,
                request_timeout_ms=10000,
            )
            break
        except NoBrokersAvailable:
            if attempt < 29:
                print(f"  Waiting... ({attempt + 1}/30)")
                time.sleep(2)
            else:
                raise

    print(f"Consumer listening to '{TOPIC}' topic (Ctrl+C to exit)...")
    for msg in consumer:
        print(f"  [{msg.partition}:{msg.offset}] {msg.value}")


if __name__ == "__main__":
    main()
