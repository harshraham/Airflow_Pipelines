#!/bin/bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker-compose up airflow-init
docker-compose up
docker exec -d piwebscraping-airflow-webserver-1 airflow dags unpause pipeline_1 pipeline_2
