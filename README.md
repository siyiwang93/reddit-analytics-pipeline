#Analytics Pipeline

A data engineering portfolio project that builds end-to-end batch 
and streaming data pipelines to analyze large-scale event data.

## Current Dataset
GitHub Archive — millions of GitHub events (stars, commits, PRs, issues)

## Tech Stack
- **Ingestion**: Python
- **Data Lake**: Google Cloud Storage
- **Batch Processing**: Apache Spark (PySpark)
- **Stream Processing**: Apache Kafka + PyFlink
- **Transformation**: dbt
- **Warehouse**: Google BigQuery
- **Orchestration**: Apache Airflow
- **Visualization**: Tableau

## Architecture
```
GitHub Archive (external)
    ↓ Python ingestion
GCS (data lake)
    ↓ PySpark transformation
BigQuery (warehouse)
    ↓ dbt models
    ↓ Airflow orchestration
    ↓ Tableau dashboard

Streaming:
GitHub Archive (hourly)
    ↓ Kafka producer
    ↓ PyFlink processing
    ↓ BigQuery (real-time)
```

## Status
🚧 In Development