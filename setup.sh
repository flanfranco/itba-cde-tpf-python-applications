# Set user
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env

# Init airflow db metadata
docker-compose up airflow-init

# Run airflow
docker-compose up -d
