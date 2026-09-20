from datetime import datetime

from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

@dag(
    dag_id="transform_data",
    description="DAG for transforming raw YouTube and TikTok data",
    start_date=datetime(2026, 1, 1),
    catchup=False
)

def transform_raw_data():
    prepare_initial_youtube_data = SparkSubmitOperator(
        task_id="transform_youtube_data",
        application="/opt/airflow/files/spark/transformation_jobs/prepare_youtube_data.py",
        conn_id="SPARK_CONNECTION",
        application_args=[
            "{{ var.value.HDFS_DEFAULT_FS }}/data/historical_youtube_gaming_data.csv",
            "{{ var.value.HDFS_DEFAULT_FS }}/transformed_youtube_data",
        ],
    )

    prepare_initial_tiktok_data = SparkSubmitOperator(
            task_id="transform_tiktok_data",
            application="/opt/airflow/files/spark/transformation_jobs/prepare_tiktok_data.py",
            conn_id="SPARK_CONNECTION",
            application_args=[
                "{{ var.value.HDFS_DEFAULT_FS }}/data/tiktok_enriched_data.csv",
                "{{ var.value.HDFS_DEFAULT_FS }}/transformed_enriched_tiktok_data",
            ],
        )

    (
        prepare_initial_youtube_data,
        prepare_initial_tiktok_data
    )

transform_raw_data()
