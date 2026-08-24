#!/bin/bash

echo "> Bringing down cluster services"

echo ">> Shutting down Airflow"
docker compose -f Airflow/docker-compose.yml down

echo ">> Shutting down Apache Superset"
docker compose -f Superset/docker-compose.yml down

echo ">> Shutting down MongoDB"
docker compose -f MongoDB/docker-compose.yml down

echo ">> Shutting down Apache Spark"
docker compose -f Spark/docker-compose.yml down

echo ">> Shutting down Hadoop"
docker compose -f Hadoop/docker-compose.yml down

echo ">> Shutting down Kafka"
docker compose -f Kafka/docker-compose.yml down

echo "> Deleting 'yt-tt-analysis' network"
docker network rm yt-tt-analysis