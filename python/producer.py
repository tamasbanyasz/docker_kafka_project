"""
Simple Kafka producer - sends messages to a topic.
Run: python producer.py
"""
import time
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "127.0.0.1:9092"
TOPIC = "teszt-topic"


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: v.encode("utf-8"),
        api_version_auto_timeout_ms=5000,
        request_timeout_ms=10000,
    )

    print(f"Producer sending to '{TOPIC}' topic (Ctrl+C to exit)...")
    for i in range(100):
        msg = f"Message #{i} - {time.strftime('%H:%M:%S')}"
        producer.send(TOPIC, value=msg)
        print(f"  Sent: {msg}")
        time.sleep(1)

    producer.flush()
    producer.close()
    print("Done.")


if __name__ == "__main__":
    main()
