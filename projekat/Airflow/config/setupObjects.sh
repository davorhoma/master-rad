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

airflow connections add 'MONGO_CONNECTION' \
  --conn-json '{
    "conn_type": "mongo",
    "host": "mongodb",
    "port": 27017,
    "login": "admin",
    "password": "admin"
  }'

airflow connections add 'LOCAL_FS_FILES' \
    --conn-json '{
        "conn_type": "fs",
        "extra": "{ \"path\": \"/opt/airflow/files\"}"
    }'

# Set up variables
echo ">> Setting up airflow variables"
airflow variables set HDFS_DEFAULT_FS "hdfs://namenode:9000"
airflow variables set MONGO_URI "mongodb://admin:admin@mongodb:27017"
airflow variables set KAFKA_BOOTSTRAP_SERVERS "broker1:9092"
airflow variables set AIRFLOW_BASE_URL "http://airflow-airflow-apiserver-1:8080"
airflow variables set SPARK_MASTER_URL "http://spark-master:8080"