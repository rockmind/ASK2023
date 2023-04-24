from datetime import datetime, timedelta
from pandas import read_csv
from json import loads

from airflow.decorators import dag, task


@dag(
    dag_id='read_from_sheet_task_flow',
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 20),
    catchup=False,
)
def read_from_sheet_task_flow():

    @task()
    def read_data_from_sheet():
        spreadsheet_id = '1WumnL8PAZGZWvEEp7u7UEVCVLvBw7QZJvN89fUQdHoc'
        spreadsheet_gid = '0'
        url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={spreadsheet_gid}"
        sheet_url = url.replace("/edit#gid=", "/export?format=csv&gid=")

        df = read_csv(sheet_url)
        return df.to_json(orient='table')

    @task()
    def create_query(data):
        table_name = 'users'
        unique_fields = ['user_name']

        raw_data = loads(data)

        fields = [field['name'] for field in raw_data['schema']['fields']][1:]

        values = "),\n        (".join(
            [
                ", ".join([
                    f"'{str(row[field])}'" if row[field] is not None else 'NULL'
                    for field in fields
                ])
                for row in raw_data['data']
            ]
        )

        str_fields = ", ".join(fields)

        query = f"""
        INSERT INTO {table_name} ({str_fields})
        VALUES
        ({values})
        """

        if unique_fields:
            conflict_fields = ",\n                ".join([
                f"COALESCE(EXCLUDED.{field}, {table_name}.{field})"
                for field in fields
            ])
            query += f"""
            ON CONFLICT ({', '.join(unique_fields)}) DO UPDATE SET 
            ({str_fields}) = (
                {conflict_fields}
            )
            """

        return query

    def update_table(sql):
        pass

    sheet_data = read_data_from_sheet()
    query_sql = create_query(sheet_data)
    update_table(query_sql)


read_from_sheet_task_flow_dag = read_from_sheet_task_flow()
