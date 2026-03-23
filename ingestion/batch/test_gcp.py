from google.cloud import bigquery, storage
from dotenv import load_dotenv
import os

load_dotenv()

# Test BigQuery
bq_client = bigquery.Client(project=os.getenv('GCP_PROJECT_ID'))
print(f"✅ BigQue ry connected: {os.getenv('GCP_PROJECT_ID')}")

# Test GCS
gcs_client = storage.Client()
bucket = gcs_client.bucket(os.getenv('GCS_BUCKET_NAME'))
print(f"✅ GCS connected: {os.getenv('GCS_BUCKET_NAME')}")