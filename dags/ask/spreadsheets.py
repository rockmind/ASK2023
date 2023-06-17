from itertools import zip_longest
from typing import List, Sequence, TypedDict

from airflow.providers.google.suite.hooks.sheets import GSheetsHook


class SpreadsheetsDataType(TypedDict):
    name: str
    headers: List[str]
    values: List[List[str]]


def get_spreadsheet_data(
        gcp_conn_id: str,
        spreadsheet_id: str,
        api_version: str = "v4",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
) -> List[SpreadsheetsDataType]:
    hook = GSheetsHook(
        gcp_conn_id=gcp_conn_id,
        api_version=api_version,
        delegate_to=delegate_to,
        impersonation_chain=impersonation_chain,
    )

    spreadsheets_metadata = hook.get_spreadsheet(spreadsheet_id=spreadsheet_id)

    sheets_range = [
        f"{sheet['properties']['title']}!"
        "R1C1:"
        f"R{sheet['properties']['gridProperties']['rowCount']}"
        f"C{sheet['properties']['gridProperties']['columnCount']}"
        for sheet in spreadsheets_metadata.get('sheets', [])
    ]

    if not sheets_range:
        return []

    spreadsheets = hook.batch_get_values(
        spreadsheet_id=spreadsheet_id,
        ranges=sheets_range,
    )
    spreadsheets_data = [
        {
            'name': sheet['range'].split('!')[0][1:-1],
            'headers': sheet.get('values', [[]])[0],
            'values': sheet.get('values', [])[1:],
        }
        for sheet in spreadsheets.get('valueRanges', [])
    ]

    return spreadsheets_data


def create_sql_query_to_synchronize(
        spreadsheet_data: List[SpreadsheetsDataType],
        schema: str,
        user: str,
) -> str:
    drop_schema_query = '\n'.join([
        "  DO $$ DECLARE",
        "      r RECORD;",
        "  BEGIN",
        "      FOR r IN (",
        "              SELECT tablename",
        "              FROM pg_tables",
        f"             WHERE schemaname = '{schema}'",
        "          ) LOOP",
        f"          EXECUTE 'DROP TABLE IF EXISTS"
        f" {schema}.' || quote_ident(r.tablename) || ' CASCADE';",
        "      END LOOP;",
        "  END $$;",
    ])
    data_insertion_queries = "\n".join([
        (
            f"  CREATE TABLE \"{schema}\".\"{sheet['name']}\" (\n"
            f"    {_prepare_columns(sheet['headers'])}\n"
            f"  );\n"
            f"  INSERT INTO \"{schema}\".\"{sheet['name']}\" (\n"
            f"    {_join_column_names(sheet['headers'])}\n"
            f"  )\n"
            f"  VALUES\n"
            f"    ({_prepare_values(sheet['values'], len(sheet['headers']))});"
        )
        for sheet in spreadsheet_data
        if sheet['headers']
    ])

    query = (
        "\nBEGIN;\n"
        f"{drop_schema_query}\n"
        f"{data_insertion_queries}\n"
        "COMMIT;"
    )

    return query


def _join_column_names(headers: List[str]):
    return ',\n    '.join(headers)


def _prepare_columns(headers: List[str]):
    return " TEXT,\n    ".join(headers) + " TEXT"


def _prepare_values(rows: List[List[str]], row_len: int):
    values = "),\n    (".join(
        [
            ", ".join([
                "NULL" if value is None else f"'{str(value)}'"
                for value, _ in zip_longest(row, range(row_len))
            ])
            for row in rows
        ]
    )
    return values
