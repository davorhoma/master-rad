#!/bin/bash
docker compose -f ./Airflow/docker-compose.yml down -v
docker rmi airflow_airflow-triggerer:latest airflow_airflow-worker:latest airflow_airflow-apiserver:latest airflow_airflow-dag-processor:latest airflow_airflow-init:latest airflow_airflow-scheduler:latest