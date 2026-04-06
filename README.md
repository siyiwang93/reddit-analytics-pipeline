# GitHub Analytics Pipeline

A production-grade batch data pipeline that ingests, transforms, and visualizes millions of GitHub events using modern data engineering tools.

## Architecture
```
GitHub Archive (source)
↓
Python ingestion → Google Cloud Storage (raw JSON)
↓
Apache Spark (PySpark) → Google Cloud Storage (processed Parquet)
↓
Apache Spark → BigQuery (raw events table)
↓
dbt (staging → intermediate → marts)
↓
Tableau dashboard
```
All steps orchestrated by **Apache Airflow** running in **Docker**.

## Tech Stack
```
| Layer | Technology |
|---|---|
| Ingestion | Python, GHArchive API |
| Data Lake | Google Cloud Storage |
| Batch Processing | Apache Spark (PySpark) |
| Data Warehouse | Google BigQuery |
| Transformation | dbt |
| Orchestration | Apache Airflow (Docker) |
| Visualization | Tableau |
| Infrastructure | Docker, Google Cloud Platform |
```

## Data

**Source:** [GitHub Archive](https://www.gharchive.org/) — public dataset of GitHub events

**Volume:** ~4.5M events per day, 30 days (January 2025)

## Pipeline

### 1. Ingestion
- Downloads hourly GHArchive files (24 files/day) directly to GCS
- Skips already-downloaded files (idempotent)
- Handles failures gracefully with error reporting

### 2. Spark Transformation
- Reads raw JSON from GCS
- Flattens nested JSON structure
- Casts and cleans data types
- Filters public events only
- Saves processed Parquet to GCS
- Loads to BigQuery

### 3. dbt Models
staging/
stg_github_events          # light cleaning on raw table
intermediate/
int_events                 # deduplicated events
marts/
dim_actors                 # unique actors (users + bots)
dim_repos                  # unique repositories
fct_events                 # base fact table
fct_push_events            # push-specific fields
fct_pr_events              # PR-specific fields
github_daily_activity      # daily event counts
github_repo_summary        # per-repo aggregates
github_platform_metrics    # DAU, engagement metrics
github_trending_repos      # trending repos by score

### 4. Airflow DAG

Daily pipeline with 5 tasks:
check_data_available → ingest_to_gcs → spark_transform → dbt_run → dbt_test

- Runs daily at 6am UTC
- Sequential execution (1 DAG run at a time)
- 3 retries per task with 5 minute delay
- Dockerized for reproducibility

## Dashboard

Built in Tableau Public connecting directly to BigQuery:
- Daily active developers trend
- Top trending repositories
- Bot vs human activity ratio
- PR merge rates by language

## Key Learnings
- Handling large-scale JSON ingestion with PySpark
- Dimensional data modeling with dbt (star schema)
- Containerizing data pipelines with Docker
- Managing GCP credentials across services
- Debugging JVM memory issues in PySpark
- Airflow DAG design for batch backfilling

## Project Structure
```
├── airflow/
│   ├── dags/
│   │   └── github_pipeline.py    # Airflow DAG
│   ├── Dockerfile
│   └── docker-compose.yml
├── ingestion/
│   └── batch/
│       └── github_ingestion.py   # GHArchive downloader
├── processing/
│   └── spark/
│       └── transform.py          # PySpark transformation
└── github_analytics/             # dbt project
    ├── models/
    │   ├── staging/
    │   ├── intermediate/
    │   └── marts/
    └── dbt_project.yml
```