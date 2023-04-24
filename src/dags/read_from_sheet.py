from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from AskCustomOperator.ask_create_insert_query import AskCreateInsertQueryOperator
from AskCustomOperator.ask_google_sheets_read_spreadsheet_operator import AskGoogleSheetsReadSpreadsheetOperator

with DAG(
    dag_id='read_from_sheet',
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 20),
    catchup=False,
) as dag:

    read_data_from_sheet = AskGoogleSheetsReadSpreadsheetOperator(
        task_id='read_data_from_sheet',
        spreadsheet_id='1WumnL8PAZGZWvEEp7u7UEVCVLvBw7QZJvN89fUQdHoc',
        spreadsheet_gid='0',
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
