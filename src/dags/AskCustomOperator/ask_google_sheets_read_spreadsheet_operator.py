from typing import Any, Dict

from airflow.models import BaseOperator
from pandas import read_csv


class AskGoogleSheetsReadSpreadsheetOperator(BaseOperator):

    def __init__(
            self,
            *,
            spreadsheet_id: str,
            spreadsheet_gid: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={spreadsheet_gid}"
        self.sheet_url = url.replace("/edit#gid=", "/export?format=csv&gid=")

    def execute(self, context: Any) -> Dict[str, Any]:
        df = read_csv(self.sheet_url)
        return df.to_json(orient='table')
