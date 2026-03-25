import requests
import os
import gzip
import json
from datetime import datetime, timedelta
from google.cloud import storage
from dotenv import load_dotenv
import calendar
import urllib3
urllib3.disable_warnings()

load_dotenv()

GCS_BUCKET = os.getenv('GCS_BUCKET_NAME')
RAW_DATA_PATH = 'raw/github'
BASE_URL = "https://data.gharchive.org"

def download_to_gcs(date_str, hour):
    """
    Download one hour of GitHub Archive data directly to GCS
    date_str: '2025-01-01'
    hour: 0-23
    """
    filename = f"{date_str}-{hour}.json.gz"
    url = f"{BASE_URL}/{filename}"
    gcs_path = f"{RAW_DATA_PATH}/{date_str}/{filename}"

    try:
        
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET)
        blob = bucket.blob(gcs_path)

        # Check if file already exists in GCS
        if blob.exists():
            print(f"⏭️  {filename} already exists in GCS, skipping...")
            return True

        # Stream download directly to GCS
        response = requests.get(url, stream=True, verify=False, timeout=60)
        response.raise_for_status()

        blob.upload_from_string(
            response.content,
            content_type='application/gzip'
        )

        size_mb = len(response.content) / 1024 / 1024
        print(f"✅ {filename} → GCS ({size_mb:.1f} MB)")
        return True

    except Exception as e:
        print(f"❌ Error downloading {filename}: {e}")
        return False

def download_day(date_str):
    """Download all 24 hours of a day"""
    print(f"\n📅 Downloading {date_str}...")
    success = 0
    failed = 0

    for hour in range(24):
        if download_to_gcs(date_str, hour):
            success += 1
        else:
            failed += 1

    print(f"📊 {date_str}: {success} success, {failed} failed")
    return success, failed

def download_month(year, month):
    """Download entire month"""
    print(f"🚀 Starting download for {year}-{month:02d}")
    print(f"📦 Destination: gs://{GCS_BUCKET}/{RAW_DATA_PATH}/\n")

    # Get number of days in month
    days = calendar.monthrange(year, month)[1]

    total_success = 0
    total_failed = 0

    for day in range(1, days + 1):
        date_str = f"{year}-{month:02d}-{day:02d}"
        success, failed = download_day(date_str)
        total_success += success
        total_failed += failed

    print(f"\n✅ Download complete!")
    print(f"📊 Total: {total_success} success, {total_failed} failed")
    print(f"📦 Data saved to: gs://{GCS_BUCKET}/{RAW_DATA_PATH}/")

if __name__ == '__main__':
    # Download January 2025
    download_month(year=2025, month=1)