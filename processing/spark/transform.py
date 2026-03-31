import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, date_format,to_date,
    hour, dayofweek, dayofmonth, month, year,
    when, split, get_json_object
)

from datetime import datetime, timedelta
from pyspark.sql.types import IntegerType, BooleanType

load_dotenv()

GCS_BUCKET = os.getenv('GCS_BUCKET_NAME')
GCP_PROJECT = os.getenv('GCP_PROJECT_ID')
BIGQUERY_DATASET = os.getenv('BIGQUERY_DATASET')

def get_shuffle_partitions(data_size_gb_compressed):
    """
    Calculate optimal shuffle partitions based on data size
    Rule: uncompressed size / 128MB
    json.gz expands ~5x when uncompressed
    """
    uncompressed_gb = data_size_gb_compressed * 5
    uncompressed_mb = uncompressed_gb * 1024
    partitions = int(uncompressed_mb / 128)
    # Minimum 200, maximum 5000
    return max(200, min(partitions, 5000))

"""
shuffle_partitions: set based on data size
    1 day   (~2.1GB compressed)  → 200
    1 week  (~15GB compressed)   → 600
    1 month (~66GB compressed)   → 2000
"""

def create_spark_session(shuffle_partitions=200):
    print(f"⚙️  Creating Spark session with {shuffle_partitions} shuffle partitions")

    # Path to application default credentials
    credentials_path = os.path.expanduser(
        "~/.config/gcloud/application_default_credentials.json"
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
                "/home/codespace/gcs-connector-hadoop3-latest.jar,"
                "/home/codespace/spark-bigquery-with-dependencies_2.12-0.36.1.jar")
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

        .getOrCreate()
    )

def read_raw_data(spark, date_str):
    """Read all hourly files for a given date from GCS"""
    # Wildcard picks up all 24 hourly files: 2025-01-01-0 to 2025-01-01-23
    gcs_path = f"gs://{GCS_BUCKET}/raw/github/{date_str}/{date_str}-*.json.gz"
    print(f"\n📖 Reading all hourly files for {date_str}...")
    print(f"   Path: {gcs_path}")

    df = spark.read.json(gcs_path)

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

def save_to_bigquery(df, table_name, date_str):
    full_table = f"{GCP_PROJECT}.{BIGQUERY_DATASET}.{table_name}"
    print(f"\n💾 Saving to BigQuery: {full_table}")

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


    df.write \
        .format("bigquery") \
        .option("table", full_table) \
        .option("temporaryGcsBucket", GCS_BUCKET) \
        .option("partitionField", "event_date") \
        .option("clusteredFields", "event_type") \
        .mode("append") \
        .save()

    print(f"✅ Saved to BigQuery: {full_table}")

def run_transformation(
    date_str="2025-01-01",
    data_size_gb=2.1):
    """
    Main transformation job

    Args:
        date_str: date to process e.g. "2025-01-01"
        data_size_gb: compressed size in GB
            1 day   = ~2.1GB
            1 week  = ~15GB
            1 month = ~66GB
        save_to_bq: True = BigQuery, False = GCS parquet
    """
    print(f"🚀 Starting transformation for {date_str}")
    print(f"📊 Estimated data size: {data_size_gb}GB compressed")

    # Calculate shuffle partitions based on data size
    shuffle_partitions = get_shuffle_partitions(data_size_gb)
    print(f"⚙️  Calculated shuffle partitions: {shuffle_partitions}")

    spark = create_spark_session(shuffle_partitions)

    try:
        # Step 1 — Read
        gcs_path = f"gs://{GCS_BUCKET}/raw/github/{date_str}/*.json.gz"
        df = read_raw_data(spark, gcs_path)

        # Step 2 — Transform
        transformed_df = transform_events(df)

        # Step 3 — Filter
        filtered_df = filter_public_events(transformed_df)

        # Step 4: Save to GCS as parquet
        save_to_gcs_parquet(filtered_df, date_str)

        # Step 5: Read back from parquet
        parquet_path = f"gs://{GCS_BUCKET}/processed/github/{date_str}"
        df_parquet = spark.read.parquet(parquet_path)

        # Step 6: Load to BigQuery from parquet
        save_to_bigquery(df_parquet, "github_events", date_str)
        
        print(f"\n✅ Transformation complete for {date_str}!")

    finally:
        spark.stop()

if __name__ == '__main__':
    spark = create_spark_session(shuffle_partitions=200)
    spark.sparkContext.setLogLevel("ERROR")  # ← suppress logs immediately
    run_transformation()

    spark.stop()