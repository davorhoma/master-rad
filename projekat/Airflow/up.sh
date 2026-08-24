#!/bin/bash
docker compose -f ./Airflow/docker-compose.yml up -d
sleep 60
cmd='bash -c "/opt/airflow/config/setupObjects.sh"'
docker exec -it airflow-airflow-apiserver-1 $cmd