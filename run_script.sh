#!/bin/bash
env AIRFLOW_UID=50000
env AIRFLOW_GID=0
docker-compose up airflow-init
docker-compose up
docker exec -d piwebscraping-airflow-webserver-1 airflow dags unpause pipeline_1 pipeline_2
