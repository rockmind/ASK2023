ARG BASE_AIRFLOW_IMAGE_NAME=apache/airflow
ARG BASE_AIRFLOW_IMAGE_TAG=slim-2.5.3-python3.10

FROM BASE_AIRFLOW_IMAGE_NAME:BASE_AIRFLOW_IMAGE_TAG

COPY requirements.txt .

RUN pip install -r requirements.txt --no-cache-dir

RUM rm requirements.txt