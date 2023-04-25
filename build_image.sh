source <(cat .env | sed 's/^/export /g')

BASE_AIRFLOW_IMAGE_TAG=slim-${AIRFLOW_VERSION}-python${IMAGE_PYTHON_VERSION}

AIRFLOW_IMAGE_NAME=ask-airflow
AIRFLOW_IMAGE_TAG=latest

cd airflow_docker

docker build \
  --build-arg AIRFLOW_VERSION=${AIRFLOW_VERSION} \
  --build-arg IMAGE_PYTHON_VERSION=$IMAGE_PYTHON_VERSION \
  --build-arg BASE_AIRFLOW_IMAGE_TAG=$BASE_AIRFLOW_IMAGE_TAG \
  --build-arg BASE_AIRFLOW_IMAGE_NAME=$BASE_AIRFLOW_IMAGE_NAME \
  -f Dockerfile \
  -t $AIRFLOW_IMAGE_NAME:$AIRFLOW_IMAGE_TAG .
