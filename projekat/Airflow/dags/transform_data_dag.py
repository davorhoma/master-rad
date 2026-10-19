from datetime import datetime, timedelta
import os

from airflow.decorators import dag
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import PythonOperator


JOINED_DATA_DIR = os.getenv("AIRFLOW_DATA_DIR", "/opt/airflow/files/data")
HDFS_DATA_DIR = "/data"


def upload_joined_files_if_missing():
    hdfs = WebHDFSHook(webhdfs_conn_id="HDFS_CONNECTION")
    client = hdfs.get_conn()

    if not hdfs.check_for_path(HDFS_DATA_DIR):
        client.makedirs(HDFS_DATA_DIR)

    for filename in ("youtube_all_joined.csv", "tiktok_all_joined.csv"):
        local_path = os.path.join(JOINED_DATA_DIR, filename)
        hdfs_path = f"{HDFS_DATA_DIR}/{filename}"

        if not os.path.isfile(local_path):
            raise FileNotFoundError(f"Joined CSV fajl nije pronađen: {local_path}")

        if hdfs.check_for_path(hdfs_path):
            print(f"Fajl već postoji u HDFS-u, preskačem upload: {hdfs_path}")
            continue

        hdfs.load_file(
            source=local_path,
            destination=hdfs_path,
            overwrite=False,
        )
        print(f"Uspešno poslat fajl u HDFS: {hdfs_path}")

@dag(
    dag_id="transform_data",
    description="DAG for transforming raw YouTube and TikTok data",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
)

def transform_raw_data():
    upload_joined_data = PythonOperator(
        task_id="upload_joined_data_to_hdfs",
        python_callable=upload_joined_files_if_missing,
    )

    prepare_initial_youtube_data = SparkSubmitOperator(
        task_id="transform_youtube_data",
        application="/opt/airflow/files/spark/transformation_jobs/prepare_youtube_data.py",
        conn_id="SPARK_CONNECTION",
        application_args=[
            "{{ var.value.HDFS_DEFAULT_FS }}/data/youtube_all_joined.csv",
            "{{ var.value.get('YOUTUBE_PARQUET_PATH', var.value.HDFS_DEFAULT_FS ~ '/transformed_youtube_data') }}",
        ],
    )

    prepare_initial_tiktok_data = SparkSubmitOperator(
            task_id="transform_tiktok_data",
            application="/opt/airflow/files/spark/transformation_jobs/prepare_tiktok_data.py",
            conn_id="SPARK_CONNECTION",
            application_args=[
                "{{ var.value.HDFS_DEFAULT_FS }}/data/tiktok_all_joined.csv",
                "{{ var.value.get('TIKTOK_PARQUET_PATH', var.value.HDFS_DEFAULT_FS ~ '/transformed_tiktok_data') }}",
            ],
        )

    postgres_spark_jars = {
        "spark.jars": "/opt/spark/jars/postgresql-42.7.8.jar",
        "spark.submit.pyFiles": "/opt/airflow/files/spark/postgres_sink.py",
        "spark.executor.extraClassPath": "/opt/spark/jars/*",
        "spark.driver.extraClassPath": "/opt/spark/jars/*",
    }

    build_dashboard_2_analytics = SparkSubmitOperator(
        task_id="build_dashboard_2_analytics",
        application="/opt/airflow/files/spark/analytical_jobs/theme_dashboard_batch.py",
        conn_id="SPARK_CONNECTION",
        conf=postgres_spark_jars,
        env_vars={
            "PYTHONPATH": (
                "/opt/spark/python/lib/pyspark.zip:"
                "/opt/spark/python/lib/py4j-0.10.9.7-src.zip"
            )
        },
        application_args=[
            "{{ var.value.get('YOUTUBE_PARQUET_PATH', var.value.HDFS_DEFAULT_FS ~ '/transformed_youtube_data') }}",
            "{{ var.value.get('TIKTOK_PARQUET_PATH', var.value.HDFS_DEFAULT_FS ~ '/transformed_tiktok_data') }}",
            "{{ var.value.ANALYTICS_POSTGRES_JDBC_URL }}",
            "{{ var.value.ANALYTICS_POSTGRES_SCHEMA }}",
            "{{ var.value.get('THEME_DAILY_COLLECTION', 'theme_daily_metrics_2') }}",
            "{{ var.value.get('THEME_TAG_DAILY_COLLECTION', 'theme_tag_daily_metrics_2') }}",
            "{{ var.value.get('THEME_RANKINGS_COLLECTION', 'theme_rankings_2') }}",
            "{{ var.value.get('THEME_MINIMUM_VIDEOS', '50') }}",
        ],
    )

    upload_joined_data >> [prepare_initial_youtube_data, prepare_initial_tiktok_data]
    [prepare_initial_youtube_data, prepare_initial_tiktok_data] >> build_dashboard_2_analytics

transform_raw_data()
