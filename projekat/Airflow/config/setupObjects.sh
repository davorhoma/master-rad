#!/bin/bash

# Set up connections
echo ">>> Setting up Airflow connections"

airflow connections add 'AIRFLOW_DB_CONNECTION' \
	--conn-json '{
        "conn_type": "postgres",
        "login": "airflow",
        "password": "airflow",
        "host": "postgres",
        "port": 5432,
        "schema": "airflow"
    }'

airflow connections add 'HDFS_CONNECTION' \
	--conn-json '{
        "conn_type": "hdfs",
        "host": "namenode",
        "port": 9870,
        "extra": {
            "use_ssl": false
        }
    }'

airflow connections add 'SPARK_CONNECTION' \
	--conn-json '{
    "conn_type": "spark",
    "host": "spark://spark-master",
    "port": 7077
  }'

airflow connections add 'ANALYTICS_POSTGRES' \
	--conn-json '{
    "conn_type": "postgres",
    "host": "postgres",
    "port": 5432,
    "schema": "analytics",
    "login": "'"${POSTGRES_USER:-admin}"'",
    "password": "'"${POSTGRES_PASSWORD:-admin}"'"
  }'

airflow connections add 'LOCAL_FS_FILES' \
	--conn-json '{
        "conn_type": "fs",
        "extra": "{ \"path\": \"/opt/airflow/files\"}"
    }'

# Set up variables
echo ">> Setting up airflow variables"
airflow variables set HDFS_DEFAULT_FS "hdfs://namenode:9000"
airflow variables set ANALYTICS_POSTGRES_JDBC_URL "jdbc:postgresql://postgres:5432/${POSTGRES_DB:-analytics}"
airflow variables set ANALYTICS_POSTGRES_SCHEMA "public"
airflow variables set KAFKA_BOOTSTRAP_SERVERS "broker1:9092"
airflow variables set AIRFLOW_BASE_URL "http://airflow-airflow-apiserver-1:8080"
airflow variables set SPARK_MASTER_URL "http://spark-master:8080"

airflow variables set CREATOR_VIDEO_DAILY_METRICS_TABLE "creator_video_daily_metrics"
