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
        "spark.jars": "/opt/spark/jars/postgresql-42.7.8.jar",
        "spark.submit.pyFiles": "/opt/airflow/files/spark/postgres_sink.py,/opt/airflow/files/spark/batch_utils.py",
        "spark.executor.extraClassPath": "/opt/spark/jars/*",
        "spark.driver.extraClassPath": "/opt/spark/jars/*",
    }

    spark_conf = {
        **spark_jars,
        "spark.pyspark.python": "/usr/local/bin/python3.11",
        "spark.pyspark.driver.python": ("/home/airflow/.local/bin/python3"),
    }

    spark_env = {
        "PYSPARK_PYTHON": "/usr/local/bin/python3.11",
        "PYSPARK_DRIVER_PYTHON": ("/home/airflow/.local/bin/python3"),
        "PYTHONPATH": (
            "/opt/spark/python/lib/pyspark.zip:" "/opt/spark/python/lib/py4j-0.10.9.7-src.zip"
        ),
    }

    # calculate_top_channels = SparkSubmitOperator(
    #     task_id="top_channels_task",
    #     application="/opt/airflow/files/spark/historical_jobs/1_channel_popularity.py",
    #     conn_id="SPARK_CONNECTION",
    #     conf=spark_jars,
    #     application_args=[
    #         "{{ var.value.HDFS_DEFAULT_FS }}/transformed_youtube_data",
    #         "{{ var.value.HDFS_DEFAULT_FS }}/transformed_tiktok_data",
    #         "{{ var.value.ANALYTICS_POSTGRES_JDBC_URL }}",
    #         "{{ var.value.ANALYTICS_POSTGRES_SCHEMA }}",
    #         "channel_popularity_1",
    #     ],
    # )

    # calculate_channel_timeline = SparkSubmitOperator(
    #         task_id="timeline_task",
    #         application="/opt/airflow/files/spark/historical_jobs/1_channel_timeline.py",
    #         conn_id="SPARK_CONNECTION",
    #         conf=spark_jars,
    #         application_args=[
    #             "{{ var.value.HDFS_DEFAULT_FS }}/transformed_youtube_data",
    #             "{{ var.value.HDFS_DEFAULT_FS }}/transformed_tiktok_data",
    #             "{{ var.value.ANALYTICS_POSTGRES_JDBC_URL }}",
    #             "{{ var.value.ANALYTICS_POSTGRES_SCHEMA }}",
    #             "channel_timeline_2",
    #         ],
    #     )

    # calculate_channel_videos_popularity = SparkSubmitOperator(
    #     task_id="channel_videos_popularity",
    #     application="/opt/airflow/files/spark/historical_jobs/1_channel_videos_popularity.py",
    #     conn_id="SPARK_CONNECTION",
    #     conf=spark_jars,
    #     application_args=[
    #         "{{ var.value.HDFS_DEFAULT_FS }}/transformed_youtube_data",
    #         "{{ var.value.HDFS_DEFAULT_FS }}/transformed_tiktok_data",
    #         "{{ var.value.ANALYTICS_POSTGRES_JDBC_URL }}",
    #         "{{ var.value.ANALYTICS_POSTGRES_SCHEMA }}",
    #         "channel_videos_popularity_1",
    #     ],
    # )

    # build_dashboard_2_analytics = SparkSubmitOperator(
    #     task_id="build_dashboard_2_analytics",
    #     conn_id="SPARK_CONNECTION",
    #     application=("/opt/airflow/files/spark/" "analytical_jobs/theme_dashboard_batch.py"),
    #     conf=spark_conf,
    #     env_vars=spark_env,
    #     application_args=[
    #         "{{ var.value.get('YOUTUBE_PARQUET_PATH', var.value.HDFS_DEFAULT_FS ~ '/transformed_youtube_data') }}",
    #         "{{ var.value.get('TIKTOK_PARQUET_PATH', var.value.HDFS_DEFAULT_FS ~ '/transformed_tiktok_data') }}",
    #         "{{ var.value.ANALYTICS_POSTGRES_JDBC_URL }}",
    #         "{{ var.value.ANALYTICS_POSTGRES_SCHEMA }}",
    #         "{{ var.value.get('THEME_DAILY_COLLECTION', 'theme_daily_metrics_2') }}",
    #         "{{ var.value.get('THEME_TAG_DAILY_COLLECTION', 'theme_tag_daily_metrics_2') }}",
    #         "{{ var.value.get('THEME_RANKINGS_COLLECTION', 'theme_rankings_2') }}",
    #         "{{ var.value.get('THEME_MINIMUM_VIDEOS', '50') }}",
    #     ],
    #     verbose=True,
    # )

    # build_dashboard_3_video_metrics = SparkSubmitOperator(
    #     task_id="build_dashboard_3_video_metrics",
    #     conn_id="SPARK_CONNECTION",
    #     application=(
    #         "/opt/airflow/files/spark/" "historical_jobs/3_creator_video_daily_metrics.py"
    #     ),
    #     conf=spark_conf,
    #     env_vars=spark_env,
    #     application_args=[
    #         "{{ var.value.get('YOUTUBE_PARQUET_PATH', "
    #         "var.value.HDFS_DEFAULT_FS ~ "
    #         "'/transformed_youtube_data') }}",
    #         "{{ var.value.get('TIKTOK_PARQUET_PATH', "
    #         "var.value.HDFS_DEFAULT_FS ~ "
    #         "'/transformed_tiktok_data') }}",
    #         "{{ var.value.ANALYTICS_POSTGRES_JDBC_URL }}",
    #         "{{ var.value.ANALYTICS_POSTGRES_SCHEMA }}",
    #         "{{ var.value.get('CREATOR_VIDEO_DAILY_METRICS_TABLE', "
    #         "'creator_video_daily_metrics') }}",
    #     ],
    #     verbose=True,
    # )

    build_dashboard_3_video_comparison = SparkSubmitOperator(
        task_id="build_dashboard_3_video_comparison",
        conn_id="SPARK_CONNECTION",
        application=("/opt/airflow/files/spark/historical_jobs/3_creator_video_comparison.py"),
        conf={
            **spark_conf,
            "spark.sql.shuffle.partitions": "16",
            "spark.default.parallelism": "16",
        },
        env_vars=spark_env,
        application_args=[
            "{{ var.value.get('YOUTUBE_PARQUET_PATH', "
            "var.value.HDFS_DEFAULT_FS ~ "
            "'/transformed_youtube_data') }}",
            "{{ var.value.get('TIKTOK_PARQUET_PATH', "
            "var.value.HDFS_DEFAULT_FS ~ "
            "'/transformed_tiktok_data') }}",
            "{{ var.value.ANALYTICS_POSTGRES_JDBC_URL }}",
            "{{ var.value.ANALYTICS_POSTGRES_SCHEMA }}",
            "{{ var.value.get('CREATOR_VIDEO_SIMILARITY_TABLE', " "'creator_video_similarity') }}",
            "{{ var.value.get('CREATOR_VIDEO_LSH_DISTANCE_THRESHOLD', " "'0.8') }}",
            "{{ var.value.get('CREATOR_VIDEO_LSH_NUM_HASH_TABLES', " "'5') }}",
            "{{ var.value.get('CREATOR_VIDEO_SIMILARITY_TOP_K', " "'20') }}",
        ],
        verbose=True,
    )

    # calculate_top_channels
    # calculate_channel_timeline
    # calculate_channel_videos_popularity
    # build_dashboard_2_analytics
    # build_dashboard_3_video_metrics
    build_dashboard_3_video_comparison


run_top_videos_analysis()
