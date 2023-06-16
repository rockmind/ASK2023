source <(cat .env | sed 's/^/export /g')

cd airflow_docker

docker pull ${BASE_AIRFLOW_IMAGE_NAME}:${BASE_AIRFLOW_IMAGE_TAG}

docker build \
  --build-arg BASE_AIRFLOW_VERSION=${BASE_AIRFLOW_VERSION} \
  --build-arg IMAGE_PYTHON_VERSION=$IMAGE_PYTHON_VERSION \
  --build-arg BASE_AIRFLOW_IMAGE_TAG=$BASE_AIRFLOW_IMAGE_TAG \
  --build-arg BASE_AIRFLOW_IMAGE_NAME=$BASE_AIRFLOW_IMAGE_NAME \
  -f Dockerfile \
  -t $ASK_AIRFLOW_IMAGE_NAME:$ASK_AIRFLOW_IMAGE_TAG .
