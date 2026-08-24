from datetime import datetime
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(
    dag_id="test_dag",
    description="DAG for testing 1 query",
    start_date=datetime(2026, 1, 1),
    catchup=False,
)
def run_top_videos_analysis():

    spark_jars = {
        "spark.jars": (
            "/opt/spark/jars/mongo-spark-connector_2.12-10.2.0.jar,"
            "/opt/spark/jars/mongodb-driver-sync-4.8.2.jar,"
            "/opt/spark/jars/mongodb-driver-core-4.8.2.jar,"
            "/opt/spark/jars/bson-4.8.2.jar"
        ),
        "spark.executor.extraClassPath": "/opt/spark/jars/*",
        "spark.driver.extraClassPath": "/opt/spark/jars/*",
    }

    calculate_viral_tag_combinations = SparkSubmitOperator(
            task_id="viral_tag_combinations",
            application="/opt/airflow/files/spark/analytical_jobs/9_viral_tag_combinations.py",
            conn_id="SPARK_CONNECTION",
            conf=spark_jars,
            application_args=[
                "{{ var.value.HDFS_DEFAULT_FS }}/transformed_youtube_data",
                "{{ var.value.HDFS_DEFAULT_FS }}/transformed_tiktok_data",
                "{{ var.value.MONGO_URI }}",
                "historical_data",
                "viral_tag_combinations",
            ],
        )

    calculate_viral_tag_combinations


run_top_videos_analysis()
