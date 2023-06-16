import json
from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator

from ask.spreadsheets import get_spreadsheet_data

if TYPE_CHECKING:
    from airflow.utils.context import Context


class GoogleSheetsReadSpreadsheetOperator(BaseOperator):
    template_fields: Sequence[str] = (
        "impersonation_chain",
        "spreadsheet_id",
    )

    def __init__(
            self,
            *,
            spreadsheet_id: str,
            gcp_conn_id: str = "google_cloud_default",
            api_version: str = "v4",
            delegate_to: str | None = None,
            impersonation_chain: str | Sequence[str] | None = None,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.spreadsheet_id = spreadsheet_id
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: "Context") -> str:
        spreadsheets_data = get_spreadsheet_data(
            gcp_conn_id=self.gcp_conn_id,
            spreadsheet_id=self.spreadsheet_id,
            api_version=self.api_version,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        return json.dumps(spreadsheets_data)
