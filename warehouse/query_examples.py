import duckdb
from ingestion.config import DATA_DIR
from pathlib import Path

DB_PATH = Path(DATA_DIR) / "warehouse" / "warehouse.duckdb"
con = duckdb.connect(str(DB_PATH))

print("Counts by language (top 10):")
print(con.execute("SELECT * FROM curated.tweet_counts_by_lang LIMIT 10;").fetchdf())

print("\nSanctions rows (sample 5):")
print(con.execute("SELECT * FROM silver.sanctions LIMIT 5;").fetchdf())

con.close()
