from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Sequence

from airflow.models import BaseOperator
from airflow.providers.google.suite.hooks.sheets import GSheetsHook

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
            delegate_to: str | None = None,
            impersonation_chain: str | Sequence[str] | None = None,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.spreadsheet_id = spreadsheet_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: "Context") -> str:
        hook = GSheetsHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        spreadsheets_metadata = hook.get_spreadsheet(spreadsheet_id=self.spreadsheet_id)

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
            spreadsheet_id=self.spreadsheet_id,
            ranges=sheets_range,
        )
        self.log.info(f"SPREADSHEETS: {json.dumps(spreadsheets, indent=4)}")
        spreadsheets_data = [
            {
                'name': sheet['range'].split('!')[0][1:-1],
                'headers': sheet.get('values', [[]])[0],
                'values': sheet.get('values', [])[1:],
            }
            for sheet in spreadsheets.get('valueRanges', [])
        ]

        return json.dumps(spreadsheets_data)
