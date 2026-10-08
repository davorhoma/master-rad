from datetime import datetime
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
    catchup=False
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
            "{{ var.value.HDFS_DEFAULT_FS }}/transformed_youtube_data",
        ],
    )

    prepare_initial_tiktok_data = SparkSubmitOperator(
            task_id="transform_tiktok_data",
            application="/opt/airflow/files/spark/transformation_jobs/prepare_tiktok_data.py",
            conn_id="SPARK_CONNECTION",
            application_args=[
                "{{ var.value.HDFS_DEFAULT_FS }}/data/tiktok_all_joined.csv",
                "{{ var.value.HDFS_DEFAULT_FS }}/transformed_tiktok_data",
            ],
        )

    upload_joined_data >> [prepare_initial_youtube_data, prepare_initial_tiktok_data]

transform_raw_data()
