from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from ask import (
    AskSpreadsheetSynchronizeQueryOperator,
    GoogleSheetsReadSpreadsheetOperator,
)

with DAG(
        dag_id="transfer_data",
        default_args={
            "retries": 0,
        },
        schedule_interval=timedelta(minutes=1),
        start_date=datetime(2023, 4, 1),
        catchup=False,
) as dag:
    read_data_from_sheet = GoogleSheetsReadSpreadsheetOperator(
        task_id="read_data_from_sheet",
        gcp_conn_id="ask_google_cloud",
        spreadsheet_id="{{ var.value.ask_google_sheet_id }}",
    )

    create_query = AskSpreadsheetSynchronizeQueryOperator(
        task_id="create_query",
        user="{{ conn.ask_db.login }}",
        schema="{{ var.value.ask_db_schema_name }}",
        data="{{ task_instance.xcom_pull(task_ids='read_data_from_sheet') }}",
    )

    update_table = PostgresOperator(
        task_id="update_table",
        postgres_conn_id="ask_db",
        sql="{{ task_instance.xcom_pull(task_ids='create_query') }}",
    )

    read_data_from_sheet >> create_query >> update_table
