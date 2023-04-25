from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from ask_operator import AskCreateInsertQueryOperator, GoogleSheetsReadSpreadsheetOperator


with DAG(
    dag_id='transfer_data',
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 1),
    catchup=False,
) as dag:

    read_data_from_sheet = GoogleSheetsReadSpreadsheetOperator(
        task_id='read_data_from_sheet',
        range_="A:G",
        gcp_conn_id='ask-google-cloud',
        spreadsheet_id='{{ var.value.ask_google_sheet_id }}',
    )

    create_query = AskCreateInsertQueryOperator(
        task_id='create_query',
        table_name='users',
        data="{{ task_instance.xcom_pull(task_ids='read_data_from_sheet') }}",
        unique_fields=['user_name'],
    )

    update_table = PostgresOperator(
        task_id="update_table",
        postgres_conn_id="my_postgres",
        sql="{{ task_instance.xcom_pull(task_ids='create_query') }}",
    )

    read_data_from_sheet >> create_query >> update_table
