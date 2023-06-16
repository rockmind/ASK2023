from typing import List
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable, Connection
from airflow.providers.postgres.hooks.postgres import PostgresHook

from ask import (
    SpreadsheetsDataType,
    get_spreadsheet_data,
    create_sql_query_to_synchronize,
)


@dag(
    dag_id='transfer_data_task_flow',
    default_args={
        'retries': 0,
    },
    schedule_interval=timedelta(minutes=1),
    start_date=datetime(2023, 4, 1),
    catchup=False,
    render_template_as_native_obj=True,
)
def transfer_data_task_flow():
    @task()
    def read_data_from_sheet() -> List[SpreadsheetsDataType]:
        spreadsheet_id = Variable.get("ask_google_sheet_id")

        spreadsheets_data = get_spreadsheet_data(
            gcp_conn_id="ask_google_cloud",
            spreadsheet_id=spreadsheet_id,
        )
        return spreadsheets_data

    @task()
    def create_query(spreadsheets_data: List[SpreadsheetsDataType]) -> str:
        user = Connection.get_connection_from_secrets("ask_db").login
        schema = Variable.get("ask_db_schema_name")

        query = create_sql_query_to_synchronize(
            spreadsheet_data=spreadsheets_data,
            schema=schema,
            user=user,
        )

        return query

    @task()
    def update_table(sql_query: str):
        hook = PostgresHook(postgres_conn_id="ask_db")
        hook.run(
            sql=sql_query,
        )

    ask_spreadsheets_data = read_data_from_sheet()
    spreadsheet_synchronize_query = create_query(spreadsheets_data=ask_spreadsheets_data)
    update_table(sql_query=spreadsheet_synchronize_query)


transfer_data_task_flow()
