import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp,to_date,
    hour, dayofweek, dayofmonth, month, year,
    when, split
)
from datetime import datetime, timedelta


load_dotenv()

GCS_BUCKET = os.getenv('GCS_BUCKET_NAME')
GCP_PROJECT = os.getenv('GCP_PROJECT_ID')
BIGQUERY_DATASET = os.getenv('BIGQUERY_DATASET')


def create_spark_session():
    #print(f"⚙️  Creating Spark session with {shuffle_partitions} shuffle partitions")

    # Path to application default credentials
    credentials_path = os.getenv(
        "GOOGLE_APPLICATION_CREDENTIALS",
        os.path.expanduser("~/.config/gcloud/application_default_credentials.json")
    )
    #print(f"🔑 Using credentials: {credentials_path}")

    java_opts = (
    "--add-opens java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-opens java.base/java.nio=ALL-UNNAMED "
    "--add-opens java.base/java.lang=ALL-UNNAMED "
    "--add-opens java.base/java.util=ALL-UNNAMED "
    "--add-opens java.base/java.lang.invoke=ALL-UNNAMED "
    "--add-opens java.base/sun.security.action=ALL-UNNAMED"
    )

    return (SparkSession.builder
        .appName("GitHubArchiveTransformation")
        .config("spark.driver.extraJavaOptions", java_opts)
        .config("spark.executor.extraJavaOptions", java_opts)

        # GCS connector
        .config("spark.jars",
                "/opt/airflow/jars/gcs-connector-hadoop3-latest.jar,"
                "/opt/airflow/jars/spark-bigquery-with-dependencies_2.12-0.36.1.jar")
        .config("spark.hadoop.fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

        # Use JSON key file for authentication
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                credentials_path)

        # Performance
        #.config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")

        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.shutdown.hooks.enabled", "false")

        .getOrCreate()
    )

def read_raw_data(spark, date_str):
    """Read all hourly files for a given date from GCS"""
    # Wildcard picks up all 24 hourly files: 2025-01-01-0 to 2025-01-01-23
    gcs_path = f"gs://{GCS_BUCKET}/raw/github/{date_str}/{date_str}-*.json.gz"
    print(f"\n📖 Reading all hourly files for {date_str}...")
    print(f"   Path: {gcs_path}")

    df = spark.read.json(gcs_path)

    #print(f"📊 Total rows: {df.count():,}") # ← this forces a full data scan
    print(f"📊 Execution partitions after reading: {df.rdd.getNumPartitions()}")

    return df

def transform_events(df):
    """Extract and transform relevant fields"""
    print("\n🔄 Transforming events...")

    transformed = df.select(
        # Event info
        col("id").alias("event_id"),
        col("type").alias("event_type"),
        to_timestamp(col("created_at")).alias("created_at"),

        # Date dimensions
        to_date(col("created_at")).alias("event_date"),
        year(col("created_at")).alias("event_year"),
        month(col("created_at")).alias("event_month"),
        dayofmonth(col("created_at")).alias("event_day"),
        hour(col("created_at")).alias("event_hour"),
        dayofweek(col("created_at")).alias("day_of_week"),

        # Weekend flag
        when(
            dayofweek(col("created_at")).isin([1, 7]), True
        ).otherwise(False).alias("is_weekend"),

        # Public flag
        col("public").alias("is_public"),

        # Actor info
        col("actor.id").alias("actor_id"),
        col("actor.login").alias("actor_login"),

        # Org info
        col("org.login").alias("org_login"),
        col("org.id").alias("org_id"),

        # Repo info
        col("repo.id").alias("repo_id"),
        col("repo.name").alias("repo_name"),
        split(col("repo.name"), "/")[0].alias("repo_owner"),
        split(col("repo.name"), "/")[1].alias("repo_short_name"),

        # PushEvent fields
        col("payload.size").cast("integer").alias("push_size"),
        col("payload.distinct_size").cast("integer").alias("push_distinct_size"),
        col("payload.ref").alias("push_ref"),
        col("payload.repository_id").alias("push_repository_id"),

        # WatchEvent
        col("payload.action").alias("action"),

        # PullRequestEvent fields
        col("payload.number").alias("pr_number"),
        col("payload.pull_request.merged").alias("pr_merged"),
        col("payload.pull_request.state").alias("pr_state"),
        col("payload.pull_request.additions").cast("integer").alias("pr_additions"),
        col("payload.pull_request.deletions").cast("integer").alias("pr_deletions"),
        col("payload.pull_request.changed_files").cast("integer").alias("pr_changed_files"),
        col("payload.pull_request.base.repo.language").alias("repo_language"),
        col("payload.pull_request.title").alias("pr_title"),

        # IssuesEvent fields
        col("payload.issue.state").alias("issue_state"),
        col("payload.issue.number").alias("issue_number"),
        col("payload.issue.title").alias("issue_title"),
        col("payload.issue.comments").cast("integer").alias("issue_comments"),

        # ForkEvent fields
        col("payload.forkee.full_name").alias("forkee_name"),
        col("payload.forkee.language").alias("forkee_language"),
        col("payload.forkee.stargazers_count").cast("integer").alias("forkee_stars"),
        col("payload.forkee.forks_count").cast("integer").alias("forkee_forks"),
        col("payload.forkee.description").alias("forkee_description"),
    )

    return transformed

def filter_public_events(df):
    """Keep only public events"""
    print("\n🔍 Filtering public events...")
    filtered = df.filter(col("is_public") == True)
    return filtered

def save_to_gcs_parquet(df, date_str):
    """
    Save transformed data as parquet to GCS for a full day.
    Partitioned by event_date for fast reads later.
    """

    output_path = f"gs://{GCS_BUCKET}/processed/github/{date_str}"
    print(f"\n💾 Saving parquet to: {output_path}")

    df.write \
        .mode("overwrite") \
        .partitionBy("event_date") \
        .parquet(output_path)

    print(f"✅ Saved to: {output_path}")

def save_to_bigquery(spark, table_name, date_str):
    full_table = f"{GCP_PROJECT}.{BIGQUERY_DATASET}.{table_name}"
    parquet_path = f"gs://{GCS_BUCKET}/processed/github/{date_str}"
    print(f"\n💾 Loading to BigQuery: {full_table}")
    print(f"   Source: {parquet_path}")

    # Delete existing partition for this date before writing
    from google.cloud import bigquery
    from google.cloud.exceptions import NotFound

    client = bigquery.Client(project=GCP_PROJECT)
    
    # Only delete partition if table already exists
    try:
        client.get_table(full_table)  # check if table exists
        query = f"""
            DELETE FROM `{full_table}`
            WHERE event_date = '{date_str}'
        """
        client.query(query).result()
        print(f"🗑️  Cleared partition: {date_str}")
    except NotFound:
        print(f"📋 Table does not exist yet, will be created on write")

    df_parquet = spark.read.parquet(parquet_path)

    df_parquet.write \
         .format("bigquery") \
         .option("table", full_table) \
         .option("temporaryGcsBucket", GCS_BUCKET) \
         .option("partitionField", "event_date") \
         .option("clusteredFields", "event_type") \
         .option("allowFieldAddition", "true") \
         .option("allowFieldRelaxation", "true") \
         .mode("append") \
         .save()

    # df.write \
    #     .format("bigquery") \
    #     .option("table", full_table) \
    #     .option("temporaryGcsBucket", GCS_BUCKET) \
    #     .option("partitionField", "event_date") \
    #     .option("clusteredFields", "event_type") \
    #     .option("allowFieldAddition", "true") \
    #     .option("allowFieldRelaxation", "true") \
    #     .mode("append") \
    #     .save()

    print(f"✅ Loaded to BigQuery: {full_table}")

def run_transformation(date_str):
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")  
    print(f"\n{'='*50}")
    print(f"📅 Processing: {date_str}")
    print(f"{'='*50}")

    try:
        # Step 1 — Read
        df = read_raw_data(spark, date_str)

        # Step 2 — Transform
        transformed_df = transform_events(df)

        # Step 3 — Filter
        filtered_df = filter_public_events(transformed_df)

        # Step 4: Save to GCS as parquet
        save_to_gcs_parquet(filtered_df, date_str)

        # Step 5: Load to BigQuery from parquet
        save_to_bigquery(spark, "github_events", date_str)
        
        print(f"\n✅ Transformation complete for {date_str}!")
        return True
    except Exception as e:
        print(f"❌ Failed: {date_str} — {str(e)}")
        return False 
    finally:
        try:
            spark.stop()   
        except Exception:
            pass

if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        date_str = sys.argv[1]
        run_transformation(date_str)
    else: 
        start_date = datetime(2025, 1, 11)
        end_date = datetime(2025, 1, 31)
        failed_dates = []

        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            success = run_transformation(date_str)
            if not success:
                failed_dates.append(date_str)
            current_date += timedelta(days=1)

        total_dates = (end_date - start_date).days + 1
        print(f"\n{'='*50}")
        print(f"✅ Successful: {total_dates - len(failed_dates)}/{total_dates}")
        if failed_dates:
            print(f"❌ Failed dates: {failed_dates}")
