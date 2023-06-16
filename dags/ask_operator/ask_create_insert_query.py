import json
from itertools import zip_longest
from typing import TYPE_CHECKING, Sequence, List

from airflow.models import BaseOperator


if TYPE_CHECKING:
    from airflow.utils.context import Context


class AskCreateInsertQueryOperator(BaseOperator):
    template_fields: Sequence[str] = (
        "schema",
        "user",
        "data",
    )

    def __init__(
            self,
            *,
            data: str,
            user: str,
            schema: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.data = data
        self.user = user
        self.schema = schema

    def execute(self, context: "Context") -> str:
        raw_data = json.loads(self.data)

        drop_schema_query = f"  DROP SCHEMA IF EXISTS {self.schema} CASCADE;"
        create_schema_query = f"  CREATE SCHEMA {self.schema} AUTHORIZATION {self.user};"
        data_insertion_queries = "\n".join([
            (
                f"  CREATE TABLE \"{self.schema}\".\"{sheet['name']}\" (\n"
                f"    {self._prepare_columns(sheet['headers'])}\n"
                f"  );\n"
                f"  INSERT INTO \"{self.schema}\".\"{sheet['name']}\" (\n"
                f"    {self._join_column_names(sheet['headers'])}\n"
                f"  )\n"
                f"  VALUES\n"
                f"    ({self._prepare_values(sheet['values'], len(sheet['headers']))});"
            )
            for sheet in raw_data
            if sheet['headers']
        ])

        query = (
            "\nBEGIN;\n"
            f"{drop_schema_query}\n"
            f"{create_schema_query}\n"
            f"{data_insertion_queries}\n"
            "COMMIT;"
        )

        return query

    @staticmethod
    def _join_column_names(headers: List[str]):
        return ',\n    '.join(headers)

    @staticmethod
    def _prepare_columns(headers: List[str]):
        return " TEXT,\n    ".join(headers) + " TEXT"

    @staticmethod
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
