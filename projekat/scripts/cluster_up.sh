#!/bin/bash

echo "> Starting up cluster"

echo "> Creating docker network 'yt-tt-analysis'"
docker network create yt-tt-analysis

echo ">> Starting up HDFS"
docker compose -f Hadoop/docker-compose.yml up -d

echo ">> Starting up Apache Spark"
docker compose -f Spark/docker-compose.yml up -d

echo ">> Starting up MongoDB"
docker compose -f MongoDB/docker-compose.yml up -d

echo ">> Starting up Apache Superset"
docker compose -f Superset/docker-compose.yml up -d

echo ">> Starting up Airflow"
docker compose -f Airflow/docker-compose.yml up -d

echo ">> Starting up Kafka"
docker compose -f Kafka/docker-compose.yml up -d

sleep 25

echo "> Setting up services"

echo ">> Setting up Airflow objects"
cmd='bash -c "/opt/airflow/config/setupObjects.sh"'
docker exec -it airflow-airflow-apiserver-1 $cmd
