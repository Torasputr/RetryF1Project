from google.cloud import bigquery, storage
import os
from dotenv import load_dotenv
import logging
import pandas as pd
from pathlib import Path
import json
from urllib.request import urlopen
from urllib.error import HTTPError
from datetime import datetime
import time
from io import BytesIO

load_dotenv()

logger = logging.getLogger(__name__)

PROJECT = os.getenv("PROJECT").strip()
LOCATION = os.getenv("LOCATION").strip()
DATASET_FETCH = f"{PROJECT}.openf1_staging.stg_sessions"
BASE_URL = os.getenv("BASE_URL").strip("/")
URL = f"{BASE_URL}/session_result?session_key="
GCS_BUCKET = os.getenv("GCS_BUCKET").strip()
GCS_MEDAL_PUSH = os.getenv("GCS_MEDAL_PUSH")
YEAR = int(os.getenv("YEAR", ""))
SR_BLOB_PATH = f"{GCS_MEDAL_PUSH}/session_results/{YEAR}.parquet"

def _configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

def calculate_sleep_secs():
    now = datetime.now()
    seconds_into_minute = now.second + now.microsecond / 1_000_000
    sleep_secs = 60 - seconds_into_minute
    return sleep_secs


def upload_to_gcs(df, buck, path):
    buf = BytesIO()
    df.to_parquet(buf, index=False) 
    buf.seek(0)
    payload = buf.getvalue()

    clients = storage.Client()
    bucket = clients.bucket(buck)
    blob = bucket.blob(path)
    blob.upload_from_file(
        BytesIO(payload),
        content_type="application/vnd.apache.parquet",
        size=len(payload),
    )
    try:
        df.to_parquet("../../data/bronze/session_results/2026.parquet", index=False)
    except Exception as e:
        logger.info(f"WIthout local save {e}")

def main():
    _configure_logging()
    logger.info(f"Init Bigquery Client")
    client = bigquery.Client(project=PROJECT, location=LOCATION)
    logger.info(f"Fetch stg_sessions")
    sql = f"""
        SELECT *
        FROM {DATASET_FETCH}
    """
    df_sessions = client.query(sql).to_dataframe()

    df_used_cols = ["session_key", "date_start", "session_type", "session_name", "year"]
    now = pd.Timestamp.now(tz="UTC")
    logger.info(f"Using the only necessary tables columns: {df_used_cols} and before {now}")
    df_used = df_sessions[df_used_cols]
    df_used = df_used.loc[
        df_used["date_start"] < now
    ]

    logger.info(f"Fetching the Session Results")
    # cache_path = Path("session_key_cache.json")
    # if cache_path.exists():
    #     seen_session_keys = set(json.loads(cache_path.read_text()))
    # else:
    #     seen_session_keys = set()
    
    frames = []
    for s in df_used.loc[df_used["year"] == YEAR]["session_key"]:
        s = int(s)
        # if s in seen_session_keys:
        #     print(f"Skip cached session_key: {s}")
        #     continue
        url = f"{URL}{s}"
        logger.info(f"Fetching for url: {url}")

        while True:
            try:
                with urlopen(url, timeout=120) as resp:
                    raw = resp.read().decode("utf-8")
                data = json.loads(raw)
                break
            except HTTPError as e:
                if e.code == 429:
                    sleep_secs = calculate_sleep_secs()
                    if sleep_secs <= 0:
                        sleep_secs = 60
                    logger.error(f"{e.code}: Waiting {sleep_secs:.1f}s until next minute...")
                    time.sleep(sleep_secs)
                    continue
                if e.code == 404:
                    logger.error(f"{e.code}: Not found for {url}")
                    data = None
                    break
            raise
        if not data:
            continue
        tmp = pd.DataFrame(data)
        tmp["ingested_at"] = pd.Timestamp.now(tz="UTC")
        frames.append(tmp)

        # seen_session_keys.add(s)
        # cache_path.write_text(json.dumps(sorted(seen_session_keys)))

    logger.info(f"Concatting the dataframes")
    df_sr = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    logger.info(f"Merging df_sr with df_sessions")
    df_sr = pd.merge(left=df_sr, right=df_used, on="session_key", how="left")    

    convert_to_string = ["duration", "gap_to_leader"]
    logger.info(f"Convert the {convert_to_string} columns into string")
    for col in convert_to_string:
        df_sr[col] = df_sr[col].astype(str)

    logger.info(f"Uploading to GCS to {SR_BLOB_PATH}")
    upload_to_gcs(df_sr, GCS_BUCKET, SR_BLOB_PATH)
    


if __name__ == "__main__":
    main()