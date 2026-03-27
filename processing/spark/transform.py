import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, date_format,
    hour, dayofweek, dayofmonth, month, year,
    when, split, get_json_object
)
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
    print(f"🔑 Using credentials: {credentials_path}")

    return (SparkSession.builder
        .appName("GitHubArchiveTransformation")

        # GCS connector
        .config("spark.jars.packages",
                "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22")
        .config("spark.hadoop.fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

        # Use JSON key file for authentication
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                credentials_path)

        # Performance
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")

        .getOrCreate()
    )

def read_raw_data(spark, gcs_path):
    """Read raw json.gz files from GCS"""
    print(f"\n📖 Reading data from {gcs_path}...")

    df = spark.read.json(gcs_path)

    print(f"📊 Execution partitions after reading: {df.rdd.getNumPartitions()}")
    print(f"📊 Schema:")
    df.printSchema()

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
        date_format(col("created_at"), "yyyy-MM-dd").alias("event_date"),
        year(col("created_at")).alias("event_year"),
        month(col("created_at")).alias("event_month"),
        dayofmonth(col("created_at")).alias("event_day"),
        hour(col("created_at")).alias("event_hour"),
        dayofweek(col("created_at")).alias("day_of_week"),

        # Weekend flag
        when(
            dayofweek(col("created_at")).isin([1, 7]), True
        ).otherwise(False).alias("is_weekend"),

        # Actor info
        col("actor.id").alias("actor_id"),
        col("actor.login").alias("actor_login"),

        # Repo info
        col("repo.id").alias("repo_id"),
        col("repo.name").alias("repo_name"),
        split(col("repo.name"), "/")[0].alias("repo_owner"),
        split(col("repo.name"), "/")[1].alias("repo_short_name"),

        # Public flag
        col("public").alias("is_public"),

        # PushEvent fields
        get_json_object(
            col("payload").cast("string"), "$.size"
        ).cast(IntegerType()).alias("push_size"),

        get_json_object(
            col("payload").cast("string"), "$.distinct_size"
        ).cast(IntegerType()).alias("push_distinct_size"),

        # WatchEvent / general action
        get_json_object(
            col("payload").cast("string"), "$.action"
        ).alias("action"),

        # PullRequestEvent fields
        get_json_object(
            col("payload").cast("string"), "$.pull_request.merged"
        ).cast(BooleanType()).alias("pr_merged"),

        get_json_object(
            col("payload").cast("string"), "$.pull_request.additions"
        ).cast(IntegerType()).alias("pr_additions"),

        get_json_object(
            col("payload").cast("string"), "$.pull_request.deletions"
        ).cast(IntegerType()).alias("pr_deletions"),

        # IssuesEvent fields
        get_json_object(
            col("payload").cast("string"), "$.issue.state"
        ).alias("issue_state"),
    )

    return transformed

def filter_public_events(df):
    """Keep only public events"""
    filtered = df.filter(col("is_public") == True)
    return filtered

def show_statistics(df):
    """Show useful statistics"""
    total = df.count()
    print(f"\n📊 Total events: {total:,}")
    print(f"📊 Execution partitions: {df.rdd.getNumPartitions()}")

    print("\n📊 Event type distribution:")
    df.groupBy("event_type") \
        .count() \
        .orderBy("count", ascending=False) \
        .show()

    print("\n📊 Sample data:")
    df.select(
        "event_type", "event_date", "event_hour",
        "actor_login", "repo_name", "is_weekend"
    ).show(10, truncate=False)

def save_to_gcs_parquet(df, date_str):
    """
    Save transformed data as parquet to GCS
    Partitioned by event_date for fast reads later
    """
    output_path = f"gs://{GCS_BUCKET}/processed/github/{date_str}"
    print(f"\n💾 Saving parquet to: {output_path}")

    # Calculate optimal output files
    # Rule: ~128MB per file
    num_partitions = df.rdd.getNumPartitions()
    optimal_partitions = max(1, num_partitions // 4)
    print(f"📊 Writing with {optimal_partitions} output files...")

    df.coalesce(optimal_partitions) \
        .write \
        .mode("overwrite") \
        .partitionBy("event_date") \
        .parquet(output_path)

    print(f"✅ Saved to: {output_path}")

def save_to_bigquery(df, table_name):
    """Save DataFrame to BigQuery"""
    full_table = f"{GCP_PROJECT}.{BIGQUERY_DATASET}.{table_name}"
    print(f"\n💾 Saving to BigQuery: {full_table}")

    df.write \
        .format("bigquery") \
        .option("table", full_table) \
        .option("temporaryGcsBucket", GCS_BUCKET) \
        .mode("overwrite") \
        .save()

    print(f"✅ Saved to BigQuery: {full_table}")

def run_transformation(
    date_str="2025-01-01",
    data_size_gb=2.1,
    save_to_bq=False
):
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

        # Step 4 — Show statistics
        show_statistics(filtered_df)

        # Step 5 — Save
        if save_to_bq:
            save_to_bigquery(filtered_df, "github_events")
        else:
            save_to_gcs_parquet(filtered_df, date_str)

        print(f"\n✅ Transformation complete for {date_str}!")

    finally:
        spark.stop()

if __name__ == '__main__':
    spark = create_spark_session(shuffle_partitions=200)
    spark.sparkContext.setLogLevel("ERROR")  # ← suppress logs immediately

    gcs_path = f"gs://{GCS_BUCKET}/raw/github/2025-01-01/2025-01-01-0.json.gz"
    df = read_raw_data(spark, gcs_path)

    # Show root level fields only
    print("\n📊 Root level fields:")
    for field in df.schema.fields:
        print(f"  {field.name}: {field.dataType.simpleString()}")

    # Full schema
    print("\n📊 Full Schema:")
    df.printSchema()

    print(f"\n📊 Row count: {df.count():,}")
    df.show(5, truncate=True)  # truncate=True to fit screen better

    spark.stop()