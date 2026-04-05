import json
import os
import sys
import time
import boto3
from datetime import datetime
from confluent_kafka import Consumer, KafkaError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC,
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
    AWS_REGION, S3_BUCKET_NAME
)

BATCH_SIZE = 1000

def consume_to_s3():
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'nyc_taxi_s3_group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
    })
    consumer.subscribe([KAFKA_TOPIC])

    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

    batch, batch_num, total = [], 1, 0
    empty_polls = 0
    print(f"S3 Consumer started -> s3://{S3_BUCKET_NAME}/raw/")

    try:
        while empty_polls < 10:
            msg = consumer.poll(timeout=3.0)
            if msg is None:
                empty_polls += 1
                print(f"  Waiting... ({empty_polls}/10)")
                continue
            if msg.error():
                print(f"  Error: {msg.error()}")
                continue

            empty_polls = 0
            batch.append(json.loads(msg.value().decode('utf-8')))
            total += 1

            if len(batch) >= BATCH_SIZE:
                now = datetime.utcnow()
                key = (f"raw/year={now.year}/month={now.month:02d}/"
                       f"day={now.day:02d}/batch_{batch_num:04d}.json")
                s3.put_object(
                    Bucket=S3_BUCKET_NAME,
                    Key=key,
                    Body=json.dumps(batch),
                    ContentType='application/json'
                )
                print(f"  Uploaded batch {batch_num} ({len(batch)} msgs) -> {key}")
                batch, batch_num = [], batch_num + 1

        if batch:
            now = datetime.utcnow()
            key = (f"raw/year={now.year}/month={now.month:02d}/"
                   f"day={now.day:02d}/batch_{batch_num:04d}.json")
            s3.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=key,
                Body=json.dumps(batch),
                ContentType='application/json'
            )
            print(f"  Uploaded final batch {batch_num} ({len(batch)} msgs)")

    except KeyboardInterrupt:
        print("\nInterrupted.")
    finally:
        consumer.close()

    print(f"\nTotal consumed: {total:,}")
    print(f"Batches uploaded to S3: {batch_num}")

if __name__ == "__main__":
    consume_to_s3()
