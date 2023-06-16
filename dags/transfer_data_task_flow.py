from typing import Dict

from pandas import DataFrame
from sqlalchemy import create_engine
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable, Connection
from airflow.providers.google.suite.hooks.sheets import GSheetsHook


@dag(
    dag_id='transfer_data_task_flow',
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 1),
    catchup=False,
)
def transfer_data_task_flow():
    @task(multiple_outputs=True)
    def read_data_from_sheet() -> Dict:
        spreadsheet_id = Variable.get("ask_google_sheet_id")

        hook = GSheetsHook(gcp_conn_id="ask-google-cloud")
        spreadsheet_value = hook.get_values(spreadsheet_id=spreadsheet_id, range_="A:G")

        headers, data = spreadsheet_value[0], spreadsheet_value[1:]

        return DataFrame(data, columns=headers).to_dict()

    @task()
    def update_table(data: Dict):
        df = DataFrame.from_dict(data)

        conn = Connection.get_connection_from_secrets("ask_db")
        engine = create_engine(
            f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        )

        df.to_sql('users', engine, if_exists='replace')

    sheet_data_df = read_data_from_sheet()
    update_table(sheet_data_df)


transfer_data_task_flow()
