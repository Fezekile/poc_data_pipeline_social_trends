#!/usr/bin/env bash
set -euo pipefail


python -u /app/scheduler/run.py &
python -u /app/ingestion/stream_ingest.py
