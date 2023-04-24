from typing import Any, Dict, Sequence, Optional, List

from airflow.models import BaseOperator
from json import loads


class AskCreateInsertQueryOperator(BaseOperator):
    template_fields: Sequence[str] = ("data",)

    def __init__(
            self,
            *,
            table_name: str,
            data: str,
            unique_fields: Optional[List[str]] = None,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.table_name = table_name
        self.data = data
        self.unique_fields = unique_fields

    def execute(self, context: Any) -> str:

        raw_data = loads(self.data)

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
        INSERT INTO {self.table_name} ({str_fields})
        VALUES
        ({values})
        """

        if self.unique_fields:
            conflict_fields = ",\n                ".join([
                f"COALESCE(EXCLUDED.{field}, {self.table_name}.{field})"
                for field in fields
            ])
            query += f"""
            ON CONFLICT ({', '.join(self.unique_fields)}) DO UPDATE SET 
            ({str_fields}) = (
                {conflict_fields}
            )
            """

        return query
