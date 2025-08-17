# POC: Batch + Streaming Data Pipeline

This compact POC ingests batch data (UN Sanctions XLSX from FIC) and streaming data (Twitter/X /X API v2 Sampled Stream), 
processes/cleans the batch data, stores streaming tweets to Parquet, and/or publishes to Kafka, 
and uses a lightweight scheduler to run the batch job daily.

## Services
- pipeline: Python app running:
  - A daily-scheduled batch job (downloads & cleans sanctions list).
  - A streaming job (Twitter/X sampled stream) with reconnection and error handling.
- **redpanda**: Kafka-compatible broker (single node) for a simple streaming sink.

## Quick Start

1. Setting up a tweeter bearer token for X API:
   ```bash
   export TWITTER_BEARER=""
   # Optional Kafka sink
   export USE_KAFKA="true"            
   export KAFKA_BROKER="redpanda:9092"
   export KAFKA_TOPIC="tweets"
   ```

2. Build & run with Docker Compose:
   ```bash
   docker compose up --build
   ```

3. Outputs:
   - Batch parquet: `./data/bronze/sanctions_YYYY-MM-DD.parquet`
   - Cleaned parquet: `./data/silver/sanctions_clean.parquet`
   - Streaming parquet files (rolling): `./data/stream/*.parquet`
   - (Optional) Tweets in Kafka topic: `${KAFKA_TOPIC}`

## Stopping
```bash
docker compose down
```

## Notes
- Scheduler uses APScheduler (inside the `pipeline` container). Default: daily at 02:00 UTC.
- Streaming runs continuously and auto-reconnects with exponential backoff and jitter.
- Redpanda is used for a compact, single-binary Kafka alternative (Kafka API compatible).

## Structure
```
poc-data-pipeline/
├── docker-compose.yml
├── Dockerfile
├── entrypoint.sh
├── requirements.txt
├── README.md
├── ingestion/
│   ├── batch_ingest.py
│   ├── stream_ingest.py
│   └── config.py
├── processing/
│   └── clean_sanctions.py
├── scheduler/
│   └── run.py
└── data/
    ├── bronze/
    ├── silver/
    └── stream/
```


##Local Warehouse (DuckDB)
This POC includes a local DuckDB database under `./data/warehouse/warehouse.duckdb`:

- Views over parquet are created/updated by the scheduler step `warehouse/load_to_duckdb.py`:
  - `silver.sanctions` → `./data/silver/sanctions_clean.parquet`
  - `bronze.tweets_raw` → `./data/stream/*.parquet`
  - `curated.tweets` and `curated.tweet_counts_by_lang` (example curated views)

Run ad-hoc queries with:
```bash
docker compose exec pipeline python /app/warehouse/query_examples.py
```
