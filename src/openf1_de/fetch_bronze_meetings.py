import os
from dotenv import load_dotenv
from urllib.request import urlopen
import json
import pandas as pd
from io import BytesIO
from google.cloud import storage
import logging

logger = logging.getLogger(__name__)


def _configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )


def fetch_data_from_api(url):
    logger.info(f"URL: {url}")
    with urlopen(url, timeout=120) as resp:
        raw = resp.read().decode("UTF-8")
    data = json.loads(raw)
    df = pd.DataFrame(data)
    logger.info(f"Adding timestamp column")
    df["ingested_at"] = pd.Timestamp.utcnow()
    return df


def upload_to_parquet(df, bucket, path, year, dir):
    buf = BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    payload = buf.getvalue()

    blob = bucket.blob(path)
    blob.upload_from_file(
        BytesIO(payload),
        content_type="application/vnd.apache.parquet",
        size=len(payload),
    )
    
    # df.to_parquet(dir, index=False)



def main():
    load_dotenv()

    BASE_URL = os.getenv("BASE_URL", "").strip("/")
    YEAR = int(os.getenv("YEAR", ""))
    URL = f"{BASE_URL}/meetings?year={YEAR}"
    GCS_BUCKET = os.getenv("GCS_BUCKET")
    GCS_MEDAL_PUSH = os.getenv("GCS_MEDAL_PUSH")
    BLOB_PATH = f"{GCS_MEDAL_PUSH}/meetings/{YEAR}.parquet"
    DIR_PATH = f"../../data/bronze/meetings/{YEAR}.parquet"

    _configure_logging()
    logger.info(f"Start Fetching")
    logger.info(f"Fetching Meetings")
    meetings_df = fetch_data_from_api(URL)

    logger.info(f"Init GCP Storage")
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)

    logger.info(f"Uploading meetings_df to GCP")
    upload_to_parquet(meetings_df, bucket, BLOB_PATH, YEAR, DIR_PATH)

if __name__ == "__main__":
    main()
