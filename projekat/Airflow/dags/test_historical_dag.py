from datetime import datetime
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(
    dag_id="historical_test_dag",
    description="Historical queries DAG",
    start_date=datetime(2026, 1, 1),
    catchup=False,
)
def run_top_videos_analysis():

    spark_jars = {
        "spark.jars": (
            "/opt/spark/jars/mongo-spark-connector_2.12-10.5.0.jar,"
            "/opt/spark/jars/mongodb-driver-sync-5.1.1.jar,"
            "/opt/spark/jars/mongodb-driver-core-5.1.1.jar,"
            "/opt/spark/jars/bson-5.1.1.jar"
        ),
        "spark.executor.extraClassPath": "/opt/spark/jars/*",
        "spark.driver.extraClassPath": "/opt/spark/jars/*",
    }

    # calculate_top_channels = SparkSubmitOperator(
    #     task_id="top_channels_task",
    #     application="/opt/airflow/files/spark/historical_jobs/1_channel_popularity.py",
    #     conn_id="SPARK_CONNECTION",
    #     conf=spark_jars,
    #     application_args=[
    #         "{{ var.value.HDFS_DEFAULT_FS }}/transformed_youtube_data",
    #         "{{ var.value.HDFS_DEFAULT_FS }}/transformed_tiktok_data",
    #         "{{ var.value.MONGO_URI }}",
    #         "historical_data",
    #         "channel_popularity_1",
    #     ],
    # )

    # calculate_channel_timeline = SparkSubmitOperator(
    #         task_id="timeline_task",
    #         application="/opt/airflow/files/spark/historical_jobs/2_channel_timeline.py",
    #         conn_id="SPARK_CONNECTION",
    #         conf=spark_jars,
    #         application_args=[
    #             "{{ var.value.HDFS_DEFAULT_FS }}/transformed_youtube_data",
    #             "{{ var.value.HDFS_DEFAULT_FS }}/transformed_tiktok_data",
    #             "{{ var.value.MONGO_URI }}",
    #             "historical_data",
    #             "channel_timeline_2",
    #         ],
    #     )

    calculate_channel_videos_popularity = SparkSubmitOperator(
                task_id="channel_videos_popularity",
                application="/opt/airflow/files/spark/historical_jobs/1_channel_videos_popularity.py",
                conn_id="SPARK_CONNECTION",
                conf=spark_jars,
                application_args=[
                    "{{ var.value.HDFS_DEFAULT_FS }}/transformed_youtube_data",
                    "{{ var.value.HDFS_DEFAULT_FS }}/transformed_tiktok_data",
                    "{{ var.value.MONGO_URI }}",
                    "historical_data",
                    "channel_videos_popularity_1",
                ],
            )

    # calculate_top_channels
    # calculate_channel_timeline
    calculate_channel_videos_popularity


run_top_videos_analysis()
