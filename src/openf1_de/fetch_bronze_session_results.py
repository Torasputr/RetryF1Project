from google.cloud import bigquery, storage
import os
from dotenv import load_dotenv
import logging
import pandas as pd
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
SR_URL = f"{BASE_URL}/session_result?session_key="
DRIVERS_URL = f"{BASE_URL}/drivers?session_key="
GCS_BUCKET = os.getenv("GCS_BUCKET").strip()
GCS_MEDAL_PUSH = os.getenv("GCS_MEDAL_PUSH")
YEAR = int(os.getenv("YEAR", ""))
SR_BLOB_PATH = f"{GCS_MEDAL_PUSH}/session_results/{YEAR}.parquet"
DRIVERS_BLOB_PATH = f"{GCS_MEDAL_PUSH}/drivers/{YEAR}.parquet"
SR_LOCAL_PATH = f"../../data/bronze/session_results/{YEAR}.parquet"
DRIVERS_LOCAL_PATH = f"../../data/bronze/drivers/{YEAR}.parquet"


def _configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )


def calculate_sleep_secs():
    now = datetime.now()
    seconds_into_minute = now.second + now.microsecond / 1_000_000
    return 60 - seconds_into_minute


def fetch_json(url):
    while True:
        try:
            with urlopen(url, timeout=120) as resp:
                return json.loads(resp.read().decode("utf-8"))
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
                return None
            raise


def upload_to_gcs(df, bucket_name, blob_path, local_path=None):
    if df.empty:
        logger.info(f"Skipping upload for empty dataframe: {blob_path}")
        return

    buf = BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    payload = buf.getvalue()

    bucket = storage.Client().bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_file(
        BytesIO(payload),
        content_type="application/vnd.apache.parquet",
        size=len(payload),
    )

    if local_path:
        try:
            df.to_parquet(local_path, index=False)
        except Exception as e:
            logger.info(f"Without local save {e}")


def main():
    _configure_logging()
    logger.info("Init Bigquery Client")
    client = bigquery.Client(project=PROJECT, location=LOCATION)

    logger.info("Fetch stg_sessions")
    df_sessions = client.query(f"SELECT * FROM {DATASET_FETCH}").to_dataframe()

    df_used_cols = ["session_key", "date_start", "session_type", "session_name", "year"]
    now = pd.Timestamp.now(tz="UTC")
    logger.info(f"Using columns {df_used_cols} for sessions before {now}")
    df_used = df_sessions[df_used_cols].loc[df_sessions["date_start"] < now]

    sr_frames = []
    driver_frames = []
    ingested_at = pd.Timestamp.now(tz="UTC")

    logger.info("Fetching session results and drivers per session")
    for session_key in df_used.loc[df_used["year"] == YEAR]["session_key"]:
        session_key = int(session_key)
        sr_url = f"{SR_URL}{session_key}"
        drivers_url = f"{DRIVERS_URL}{session_key}"

        logger.info(f"Fetching session results: {sr_url}")
        sr_data = fetch_json(sr_url)
        if sr_data:
            tmp_sr = pd.DataFrame(sr_data)
            tmp_sr["ingested_at"] = ingested_at
            sr_frames.append(tmp_sr)

        logger.info(f"Fetching drivers: {drivers_url}")
        drivers_data = fetch_json(drivers_url)
        if drivers_data:
            tmp_drivers = pd.DataFrame(drivers_data)
            if "session_key" not in tmp_drivers.columns:
                tmp_drivers["session_key"] = session_key
            if "year" not in tmp_drivers.columns:
                tmp_drivers["year"] = YEAR
            tmp_drivers["ingested_at"] = ingested_at
            driver_frames.append(tmp_drivers)

    logger.info("Concatenating session results")
    df_sr = pd.concat(sr_frames, ignore_index=True) if sr_frames else pd.DataFrame()
    if not df_sr.empty:
        df_sr = pd.merge(df_sr, df_used, on="session_key", how="left")
        for col in ["duration", "gap_to_leader"]:
            if col in df_sr.columns:
                df_sr[col] = df_sr[col].astype(str)

    logger.info("Concatenating drivers")
    df_drivers = pd.concat(driver_frames, ignore_index=True) if driver_frames else pd.DataFrame()
    if not df_drivers.empty:
        df_drivers = df_drivers.drop_duplicates(subset=["driver_number", "year"], keep="last")

    logger.info(f"Uploading session results to GCS: {SR_BLOB_PATH}")
    upload_to_gcs(df_sr, GCS_BUCKET, SR_BLOB_PATH, SR_LOCAL_PATH)

    logger.info(f"Uploading drivers to GCS: {DRIVERS_BLOB_PATH}")
    upload_to_gcs(df_drivers, GCS_BUCKET, DRIVERS_BLOB_PATH, DRIVERS_LOCAL_PATH)


if __name__ == "__main__":
    main()
