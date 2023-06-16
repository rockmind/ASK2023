import json
from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator

from ask.spreadsheets import create_sql_query_to_synchronize

if TYPE_CHECKING:
    from airflow.utils.context import Context


class AskSpreadsheetSynchronizeQueryOperator(BaseOperator):
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

        query = create_sql_query_to_synchronize(
            spreadsheet_data=raw_data,
            schema=self.schema,
            user=self.user,
        )

        return query
