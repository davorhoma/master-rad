#!/bin/bash

echo "> Starting up cluster"

echo "> Checking docker network 'yt-tt-analysis'"
if ! docker network inspect yt-tt-analysis >/dev/null 2>&1; then
	echo ">> Creating docker network 'yt-tt-analysis'"
	docker network create yt-tt-analysis
else
	echo ">> Docker network 'yt-tt-analysis' already exists"
fi

start_postgres() {
	echo ">> Starting up PostgreSQL and Adminer"
	docker compose -f Postgres/docker-compose.yml up -d postgres adminer
}

start_batch() {
	echo ">> Starting HDFS"
	docker compose -f Hadoop/docker-compose.yml up -d

	echo ">> Starting Apache Spark"
	docker compose -f Spark/docker-compose.yml up -d --build

	start_postgres

	echo ">> Starting Airflow"
	docker compose -f Airflow/docker-compose.yml up -d --build
}

start_realtime() {
	start_postgres

	echo ">> Starting up Kafka"
	docker compose -f Kafka/docker-compose.yml up -d
}

start_dashboard() {
	start_postgres

	echo ">> Starting up Apache Superset"
	docker compose -f Superset/docker-compose.yml up -d
}

start_all() {
	echo ">> Starting up HDFS"
	docker compose -f Hadoop/docker-compose.yml up -d

	echo ">> Starting up Apache Spark"
	docker compose -f Spark/docker-compose.yml up -d --build

	start_postgres

	echo ">> Starting up Apache Superset"
	docker compose -f Superset/docker-compose.yml up -d

	echo ">> Starting up Airflow"
	docker compose -f Airflow/docker-compose.yml up -d --build

	echo ">> Starting up Kafka"
	docker compose -f Kafka/docker-compose.yml up -d
}

case "$1" in

batch)
	start_batch
	;;

realtime)
	start_realtime
	;;

dashboard)
	start_dashboard
	;;

all)
	start_all
	;;

*)
	echo ""
	echo "Usage: $0 {batch|realtime|dashboard|all}"
	echo ""
	echo "Available modes:"
	echo "  batch      - HDFS + Spark + PostgreSQL + Airflow"
	echo "  realtime   - PostgreSQL + Kafka"
	echo "  dashboard  - PostgreSQL + Superset"
	echo "  all        - HDFS + Spark + PostgreSQL + Superset + Airflow + Kafka"
	echo ""
	exit 1
	;;

esac

# Airflow setup is only needed when the complete cluster is started.
if [ "$1" = "batch" ] || [ "$1" = "all" ]; then
	echo ""
	echo "> Waiting for services to start"
	sleep 25

	echo "> Setting up services"

	echo ">> Setting up Airflow objects"
	cmd='bash -c "/opt/airflow/config/setupObjects.sh"'
	docker exec -it airflow-airflow-apiserver-1 $cmd
fi

echo ""
echo "> Cluster startup completed"
