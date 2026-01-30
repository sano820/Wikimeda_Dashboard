"""
Consumer
- Kafka topic에서 메시지를 읽어 파일로 저장(JSON Lines)
"""
import json
import os
from datetime import datetime
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "wiki-events")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "wikimeda-consumer-group")

OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/data")
OUTPUT_FILE = os.getenv("OUTPUT_FILE", "")  # 비우면 자동 파일명 생성


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    out_path = OUTPUT_FILE.strip()
    if not out_path:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = os.path.join(OUTPUT_DIR, f"recentchange_{ts}.jsonl")

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset="latest",   # 처음 실행 시 최신부터
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )

    print(f"[consumer] bootstrap={KAFKA_BOOTSTRAP}, topic={TOPIC}, group={GROUP_ID}")
    print(f"[consumer] writing to {out_path}")

    count = 0
    with open(out_path, "a", encoding="utf-8") as f:
        for msg in consumer:
            event = msg.value
            f.write(json.dumps(event, ensure_ascii=False) + "\n")
            count += 1
            if count % 100 == 0:
                f.flush()
                print(f"[consumer] saved={count}")


if __name__ == "__main__":
    main()
