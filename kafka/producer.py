import json
import os
import sys
import time
import pandas as pd
from confluent_kafka import Producer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

PARQUET_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'yellow_tripdata_2023-01.parquet')
BATCH_SIZE = 10000
MAX_ROWS = 50000

def delivery_report(err, msg):
    if err:
        print(f"  Delivery failed: {err}")

def main():
    producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})

    print(f"Reading parquet...")
    df = pd.read_parquet(PARQUET_FILE)
    if MAX_ROWS:
        df = df.head(MAX_ROWS)
    print(f"Loaded {len(df):,} rows")

    start = time.time()
    for idx, row in df.iterrows():
        msg = {}
        for col, val in row.items():
            if pd.isna(val):
                msg[col] = None
            elif isinstance(val, pd.Timestamp):
                msg[col] = val.isoformat()
            else:
                msg[col] = val

        producer.produce(
            KAFKA_TOPIC,
            key=str(msg.get('VendorID', 'unknown')),
            value=json.dumps(msg),
            callback=delivery_report
        )
        producer.poll(0)

        if (idx + 1) % BATCH_SIZE == 0:
            elapsed = time.time() - start
            print(f"  Sent {idx+1:,}/{len(df):,} | {(idx+1)/elapsed:,.0f} msgs/sec")

    producer.flush()
    elapsed = time.time() - start
    print(f"\nDone. Sent {len(df):,} messages in {elapsed:.1f}s")

if __name__ == "__main__":
    main()
