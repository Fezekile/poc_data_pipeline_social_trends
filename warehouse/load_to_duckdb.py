import os
from pathlib import Path
import duckdb
from ingestion.config import DATA_DIR

WAREHOUSE_DIR = Path(DATA_DIR) / "warehouse"
WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = WAREHOUSE_DIR / "warehouse.duckdb"

def main():
    con = duckdb.connect(str(DB_PATH))


    sanctions_path = Path(DATA_DIR) / "silver" / "sanctions_clean.parquet"
    tweets_glob = str(Path(DATA_DIR) / "stream" / "*.parquet")


    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("CREATE SCHEMA IF NOT EXISTS curated;")


    if sanctions_path.exists():
        con.execute(f'''
            CREATE OR REPLACE VIEW silver.sanctions AS
            SELECT * FROM read_parquet('{sanctions_path.as_posix()}');
        ''')
    else:

        con.execute("CREATE OR REPLACE TABLE silver.sanctions(id BIGINT);")
        con.execute("DELETE FROM silver.sanctions;")


    con.execute(f'''
        CREATE OR REPLACE VIEW bronze.tweets_raw AS
        SELECT * FROM read_parquet('{tweets_glob}');
    ''')


    con.execute('''
        CREATE OR REPLACE VIEW curated.tweets AS
        SELECT 
            try_cast(id AS VARCHAR) AS id,
            text,
            try_cast(author_id AS VARCHAR) AS author_id,
            try_cast(created_at AS TIMESTAMP) AS created_at,
            coalesce(lang, 'und') AS lang,
            user_username,
            user_name
        FROM bronze.tweets_raw;
    ''')


    con.execute('''
        CREATE OR REPLACE VIEW curated.tweet_counts_by_lang AS
        SELECT lang, COUNT(*) AS cnt
        FROM curated.tweets
        GROUP BY 1
        ORDER BY cnt DESC;
    ''')

    con.close()
    print(f"[warehouse] DuckDB ready at {DB_PATH}")

if __name__ == "__main__":
    main()
