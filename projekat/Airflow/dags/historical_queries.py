from datetime import datetime
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(
    dag_id="analytic_top_gaming_videos",
    description="DAG for calculating top 10 gaming videos by year (YouTube & TikTok)",
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

    calculate_top_videos = SparkSubmitOperator(
        task_id="spark_top_videos_task",
        application="/opt/airflow/files/spark/analytical_jobs/1_top_gaming_videos_by_year.py",
        conn_id="SPARK_CONNECTION",
        conf=spark_jars,
        application_args=[
            "{{ var.value.HDFS_DEFAULT_FS }}/transformed_youtube_data",
            "{{ var.value.HDFS_DEFAULT_FS }}/transformed_enriched_tiktok_data",
            "{{ var.value.MONGO_URI }}",
            "historical_data",
            "top_gaming_videos_by_year",
        ],
    )

    calculate_top_channels = SparkSubmitOperator(
        task_id="spark_top_channels_task",
        application="/opt/airflow/files/spark/analytical_jobs/2_top_gaming_channels_by_year.py",
        conn_id="SPARK_CONNECTION",
        conf=spark_jars,
        application_args=[
            "{{ var.value.HDFS_DEFAULT_FS }}/transformed_youtube_data",
            "{{ var.value.HDFS_DEFAULT_FS }}/transformed_enriched_tiktok_data",
            "{{ var.value.MONGO_URI }}",
            "historical_data",
            "top_gaming_channels_by_year",
        ],
    )

    # calculate_top_keywords = SparkSubmitOperator(
    #     task_id="spark_top_keywords_task",
    #     application="/opt/airflow/files/spark/analytical_jobs/3_top_gaming_keywords_by_platform.py",
    #     conn_id="SPARK_CONNECTION",
    #     conf=spark_jars,
    #     application_args=[
    #         "{{ var.value.HDFS_DEFAULT_FS }}/transformed_youtube_data",
    #         "{{ var.value.HDFS_DEFAULT_FS }}/transformed_tiktok_data",
    #         "{{ var.value.MONGO_URI }}",
    #         "historical_data",
    #         "top_gaming_keywords_by_platform",
    #     ],
    # )

    calculate_viral_score_distribution = SparkSubmitOperator(
        task_id="spark_viral_score_distribution_task",
        application="/opt/airflow/files/spark/analytical_jobs/4_viral_score_distribution.py",
        conn_id="SPARK_CONNECTION",
        conf=spark_jars,
        application_args=[
            "{{ var.value.HDFS_DEFAULT_FS }}/transformed_youtube_data",
            "{{ var.value.HDFS_DEFAULT_FS }}/transformed_enriched_tiktok_data",
            "{{ var.value.MONGO_URI }}",
            "historical_data",
            "viral_score_distribution",
        ],
    )

    [
        calculate_top_videos,
        calculate_top_channels,
        # calculate_top_keywords >> calculate_viral_score_distribution,
    ]


run_top_videos_analysis()
