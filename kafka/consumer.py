import json
import os
import sys
import time
from confluent_kafka import Consumer, KafkaError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'consumed')
BATCH_SIZE = 1000

def consume_messages():
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'nyc_taxi_consumer_group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
    })
    consumer.subscribe([KAFKA_TOPIC])

    batch, batch_num, total = [], 1, 0
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"Consumer started. Reading from '{KAFKA_TOPIC}'...")
    empty_polls = 0

    try:
        while empty_polls < 10:
            msg = consumer.poll(timeout=3.0)
            if msg is None:
                empty_polls += 1
                print(f"  Waiting... ({empty_polls}/10 empty polls)")
                continue
            if msg.error():
                print(f"  Error: {msg.error()}")
                continue

            empty_polls = 0
            batch.append(json.loads(msg.value().decode('utf-8')))
            total += 1

            if len(batch) >= BATCH_SIZE:
                filepath = os.path.join(OUTPUT_DIR, f"batch_{batch_num:04d}.json")
                with open(filepath, 'w') as f:
                    json.dump(batch, f)
                print(f"  Wrote batch {batch_num}: {len(batch)} messages")
                batch, batch_num = [], batch_num + 1

        if batch:
            filepath = os.path.join(OUTPUT_DIR, f"batch_{batch_num:04d}.json")
            with open(filepath, 'w') as f:
                json.dump(batch, f)
            print(f"  Wrote final batch {batch_num}: {len(batch)} messages")

    except KeyboardInterrupt:
        print("\nInterrupted.")
    finally:
        consumer.close()

    print(f"\nTotal consumed: {total:,}")
    print(f"Batches written: {batch_num}")

if __name__ == "__main__":
    consume_messages()
