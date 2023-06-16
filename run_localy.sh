source <(cat .env | sed 's/^/export /g')

docker-compose up airflow-init

docker-compose up
