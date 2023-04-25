from __future__ import annotations

from pandas import DataFrame
from typing import Any, Sequence


from airflow.models import BaseOperator
from airflow.providers.google.suite.hooks.sheets import GSheetsHook


class GoogleSheetsReadSpreadsheetOperator(BaseOperator):
    template_fields: Sequence[str] = (
        "impersonation_chain",
        "spreadsheet_id",
        "range",
    )

    def __init__(
        self,
        *,
        range_: str,
        spreadsheet_id: str,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.range = range_
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.spreadsheet_id = spreadsheet_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Any) -> str:
        hook = GSheetsHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        spreadsheet_value = hook.get_values(spreadsheet_id=self.spreadsheet_id, range_=self.range)

        headers, data = spreadsheet_value[0], spreadsheet_value[1:]

        df = DataFrame(data, columns=headers)

        return df.to_json(orient='table')
