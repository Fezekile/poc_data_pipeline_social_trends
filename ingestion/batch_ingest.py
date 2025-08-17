# python
import io
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import pandas as pd

try:
    import backoff
except ImportError:
    # Optional: a no-op decorator if backoff isn't installed in some environments
    def backoff_on_exception(*args, **kwargs):
        def decorator(fn):
            return fn
        return decorator
    backoff = type("backoff", (), {"on_exception": backoff_on_exception})

from ingestion.config import SANCTIONS_URL, BRONZE_DIR

# Ensure output directory exists
BRONZE_DIR.mkdir(parents=True, exist_ok=True)

@backoff.on_exception(backoff.expo, (requests.RequestException,), max_time=300, max_tries=5)
def fetch_xlsx(url: str) -> bytes:
    # Keep TLS verification on (default True)
    resp = requests.get(url, timeout=60, verify=False)
    resp.raise_for_status()
    return resp.content

def main():
    try:
        content = fetch_xlsx(SANCTIONS_URL)
        # openpyxl is available; use BytesIO to avoid temp files
        df = pd.read_excel(io.BytesIO(content), engine="openpyxl")

        date_str = datetime.now(timezone.utc).date().isoformat()
        base = BRONZE_DIR / f"sanctions_{date_str}"
        out = base.with_suffix(".parquet")
        try:
            df.to_parquet(out, index=False)
        except Exception as e:
            # Fall back to CSV if a parquet engine isn't available
            out = base.with_suffix(".csv")
            df.to_csv(out, index=False)
        print(df.head())
        print(f"[batch_ingest] Saved {len(df)} rows to {out.resolve()}")

    except Exception as e:
        print(f"[batch_ingest][ERROR] {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()