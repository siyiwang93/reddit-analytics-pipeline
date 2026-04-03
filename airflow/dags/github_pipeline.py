from airflow.decorators import dag, task
from datetime import datetime, timedelta


@dag(
    dag_id="github_pipeline",
    schedule="0 6 * * *",
    start_date=datetime(2025, 1, 15),
    end_date=datetime(2025, 1, 31),  # ← stops after Jan 30
    catchup=True,                    # ← backfill all dates from start to end
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5)
    },
    tags=["github", "spark", "dbt"]
)
def github_pipeline():

    @task
    def check_data_available(ds=None): #ds - execution date string
        import requests
        url = f"https://data.gharchive.org/{ds}-0.json.gz"
        response = requests.head(url) # HEAD request — check if URL exists
        if response.status_code != 200:
            raise Exception(f"GHArchive data not available for {ds}") # raise = task fails → triggers retry
        print(f"✅ Data available for {ds}")

    @task
    def ingest_to_gcs(ds=None):
        import sys
        sys.path.insert(0, "/opt/airflow/ingestion/batch")  # ← updated path
        from github_ingestion import download_day
        success, failed = download_day(ds)
        if failed > 0:
            print(f"⚠️ {failed} hours failed for {ds}")
        if success == 0:
            raise Exception(f"All downloads failed for {ds}")
        print(f"✅ Ingestion complete: {success} success, {failed} failed")

    # @task
    # def spark_transform(ds=None):
    #     import sys
    #     sys.path.insert(0, "/opt/airflow/processing/spark")  # Add spark script to Python path so it can be imported
    #     from transform import run_transformation
    #     success = run_transformation(ds)
    #     if not success:
    #         raise Exception(f"Spark transform failed for {ds}")
    #     print(f"✅ Spark transform complete for {ds}")

    @task
    def spark_transform(ds=None):
        import subprocess
        result = subprocess.run(
            ["python", "/opt/airflow/processing/spark/transform.py", ds],
            capture_output=True,
            text=True,
            timeout=3600  # 1 hour timeout
        )
        print(result.stdout)
        print(result.stderr)
        if result.returncode != 0:
            raise Exception(f"Spark transform failed: {result.stderr}")
        print(f"✅ Spark transform complete for {ds}")

    @task
    def dbt_run():
        import subprocess
        result = subprocess.run(
            ["dbt", "run", "--project-dir", "/opt/airflow/github_analytics", "--profiles-dir", "/opt/airflow/github_analytics"],
            capture_output=True,
            text=True
        )
        print(result.stdout)
        if result.returncode != 0:
            raise Exception(f"dbt run failed: {result.stderr}")
        print("✅ dbt run complete")

    @task
    def dbt_test():
        import subprocess
        result = subprocess.run(
            ["dbt", "test", "--project-dir", "/opt/airflow/github_analytics", "--profiles-dir", "/opt/airflow/github_analytics"],
            capture_output=True,
            text=True
        )
        print(result.stdout)
        if result.returncode != 0:
            raise Exception(f"dbt test failed: {result.stderr}")
        print("✅ dbt test complete")

    check_data_available() >> ingest_to_gcs() >> spark_transform() >> dbt_run() >> dbt_test()


github_pipeline()# Call the function to register the DAG with Airflow