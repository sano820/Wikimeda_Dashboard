"""
Producer
- Wikimedia EventStreams에서 이벤트를 받아 Kafka topic에 JSON으로 전송
"""
import json
import os
import time
from confluent_kafka import Producer
from dotenv import load_dotenv

from api_client import iter_recentchange_events

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "wiki-events")
CLIENT_ID = os.getenv("KAFKA_CLIENT_ID", "wikimeda-producer")


def main():
    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "client.id": CLIENT_ID,
            "acks": "all",
            "retries": 10,
            "linger.ms": 50,
        }
    )

    print(f"[producer] bootstrap={KAFKA_BOOTSTRAP}, topic={TOPIC}")

    sent = 0
    try:
        for event in iter_recentchange_events():
            # 키는 있으면 좋음(파티셔닝/순서). 없으면 None.
            key = (event.get("wiki") or event.get("title") or "").encode("utf-8") if (event.get("wiki") or event.get("title")) else None

            producer.send(TOPIC, key=key, value=event)
            sent += 1

            if sent % 100 == 0:
                producer.flush()
                print(f"[producer] sent={sent}")

    except KeyboardInterrupt:
        print("[producer] stopped by user")
    except Exception as e:
        print(f"[producer] error: {e}")
        time.sleep(2)
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
