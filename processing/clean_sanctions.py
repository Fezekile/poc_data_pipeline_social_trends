import sys
from pathlib import Path
import pandas as pd
from ingestion.config import BRONZE_DIR, SILVER_DIR

SILVER_DIR.mkdir(parents=True, exist_ok=True)

def main():
    try:
        files = sorted(BRONZE_DIR.glob("sanctions_*.parquet"))
        if not files:
            print("[clean_sanctions] No bronze files found.")
            return
        src = files[-1]
        df = pd.read_parquet(src)
        # basic clean-up
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        df = df.drop_duplicates()
        out = SILVER_DIR / "sanctions_clean.parquet"
        df.to_parquet(out, index=False)
        print(f"[clean_sanctions] Cleaned {len(df)} rows -> {out}")
    except Exception as e:
        print(f"[clean_sanctions][ERROR] {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
