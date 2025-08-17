import os
from pathlib import Path
import requests
import time



def _in_container() -> bool:
    # /.dockerenv is present in Docker containers
    return Path("/.dockerenv").exists()


if "DATA_DIR" in os.environ and os.environ["DATA_DIR"].strip():
    DATA_DIR = Path(os.environ["DATA_DIR"]).expanduser().resolve()
elif _in_container():
    DATA_DIR = Path("/app/data")
else:

    DATA_DIR = (Path(__file__).resolve().parents[1] / "data").resolve()


SANCTIONS_URL = (
    "https://transfer.fic.gov.za/public/folder/SE-MPQpJlUKmxgfA11NwZQ/Downloads/"
    "Consolidated%20United%20Nations%20Security%20Council%20Sanctions%20List.xlsx"
)

DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data"))
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
STREAM_DIR = DATA_DIR / "stream"


TWITTER_BEARER = os.getenv("TWITTER_BEARER", "")
STREAM_URL = "https://api.x.com/2/tweets/sample/stream"  # sampled stream
TWEET_FIELDS = "id,text,author_id,created_at,lang,public_metrics,possibly_sensitive,source"
USER_FIELDS = "username,name,public_metrics,verified,created_at"


USE_KAFKA = os.getenv("USE_KAFKA", "false").lower() == "true"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "tweets")


SCHEDULE_CRON = os.getenv("SCHEDULE_CRON", "05 * * * *")
