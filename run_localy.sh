export AIRFLOW_PROJ_DIR=$(pwd)
echo $AIRFLOW_PROJ_DIR

docker-compose up airflow-init

docker-compose up
