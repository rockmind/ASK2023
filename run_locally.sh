source <(cat .env | sed 's/^/export /g')

mkdir -p ${AIRFLOW_PROJ_LOG_DIR} ${AIRFLOW_PROJ_PLUGINS_DIR}

docker-compose up airflow-init

docker-compose up
