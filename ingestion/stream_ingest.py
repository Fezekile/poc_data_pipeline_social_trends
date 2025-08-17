import os
import sys
import time
import json
import signal
import random
import backoff
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any
from ingestion.config import SCHEDULE_CRON
from ingestion.config import (
    TWITTER_BEARER, STREAM_URL, TWEET_FIELDS, USER_FIELDS,
    STREAM_DIR, USE_KAFKA, KAFKA_BROKER, KAFKA_TOPIC
)

# Optional Kafka producer
producer = None
if USE_KAFKA:
    try:
        from confluent_kafka import Producer, KafkaException
        producer = Producer({'bootstrap.servers': KAFKA_BROKER, 'linger.ms': 50})
    except Exception as e:
        print(f"[stream][WARN] Kafka not available: {e}")
        producer = None

STREAM_DIR.mkdir(parents=True, exist_ok=True)

stop_flag = False
def handle_stop(signum, frame):
    global stop_flag
    stop_flag = True
signal.signal(signal.SIGINT, handle_stop)
signal.signal(signal.SIGTERM, handle_stop)

def _kafka_delivery(err, msg):
    if err is not None:
        print(f"[stream][KAFKA][ERROR] {err}", file=sys.stderr)

def flush_kafka():
    if producer:
        producer.flush(5)

def write_parquet_batch(batch: List[Dict[str, Any]]):
    if not batch:
        return
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    out = STREAM_DIR / f"tweets_{ts}.parquet"
    # Flatten minimal fields for demo
    rows = []
    for t in batch:
        data = t.get('data', {})
        user = {}
        includes = t.get('includes', {})
        users = includes.get('users', [])
        if users:
            user = users[0]
        rows.append({
            "id": data.get("id"),
            "text": data.get("text"),
            "author_id": data.get("author_id"),
            "created_at": data.get("created_at"),
            "lang": data.get("lang"),
            "user_username": user.get("username"),
            "user_name": user.get("name"),
        })
    df = pd.DataFrame(rows)
    df.to_parquet(out, index=False)
    print(f"[stream] Wrote {len(df)} tweets -> {out}")

def auth_headers():
    if not TWITTER_BEARER:
        raise RuntimeError("TWITTER_BEARER is not set.")
    return {"Authorization": f"Bearer {TWITTER_BEARER}"}

def stream_params():
    return {
        "tweet.fields": TWEET_FIELDS,
        "user.fields": USER_FIELDS,
        "expansions": "author_id"
    }

def should_retry(e):
    # Network and HTTP-level retry conditions
    if isinstance(e, requests.RequestException):
        return True
    return False

@backoff.on_exception(
    backoff.expo,
    (requests.RequestException, RuntimeError),
    max_time=3600,  # backoff up to 1 hour total before giving up
    jitter=backoff.full_jitter,
)
def connect_stream():
    headers = auth_headers()
    params = stream_params()
    resp = requests.get(STREAM_URL, headers=headers, params=params, stream=True, timeout=90)
    if resp.status_code >= 400:
        # Trigger backoff retry
        raise requests.RequestException(f"HTTP {resp.status_code}: {resp.text[:200]}")
    return resp

def main():
    global stop_flag
    buffer = []
    max_batch = 100
    last_write = time.time()
    max_interval = 30  # seconds

    while not stop_flag:
        try:
            print("[stream] Connecting to X API sampled stream...")
            with connect_stream() as resp:
                for line in resp.iter_lines():
                    if stop_flag:
                        break
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    # Push to Kafka, if enabled
                    if producer is not None:
                        try:
                            producer.produce(KAFKA_TOPIC, json.dumps(obj).encode("utf-8"), callback=_kafka_delivery)
                        except BufferError:
                            producer.poll(0.5)

                    buffer.append(obj)

                    # Flush conditions
                    now = time.time()
                    if len(buffer) >= max_batch or (now - last_write) >= max_interval:
                        write_parquet_batch(buffer)
                        buffer.clear()
                        last_write = now

                    # Poll Kafka events
                    if producer is not None:
                        producer.poll(0)

        except Exception as e:
            print(f"[stream][WARN] stream error: {e}", file=sys.stderr)
            # brief sleep before backoff retry decorator re-invokes connect_stream
            time.sleep(1)
        finally:
            flush_kafka()

    # Final flush on exit
    if buffer:
        write_parquet_batch(buffer)
        buffer.clear()
    flush_kafka()
    print("[stream] Exiting.")

if __name__ == "__main__":
    main()
