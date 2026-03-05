from __future__ import annotations

import csv
import io
from dataclasses import asdict

import yaml

from .models import (
    DEFAULT_TOTAL_ROWS,
    ColumnSpec,
    GenerationRequest,
    JoinSpec,
    TableSpec,
)


def _default_rows_for_table(name: str, index: int) -> int:
    if index == 0:
        return DEFAULT_TOTAL_ROWS
    if "dim" in name.lower():
        return 10_000
    return 25_000


def build_request_from_description(description: str) -> GenerationRequest:
    request = GenerationRequest.default_single_table(description=description)
    lower = description.lower()

    if "join" in lower:
        customer = TableSpec(
            name="dim_customer",
            rows=10_000,
            primary_key="customer_id",
            columns=[
                ColumnSpec("customer_id", "int", distinct_values=10_000, nullable=False),
                ColumnSpec("customer_name", "string"),
                ColumnSpec("segment", "string", distinct_values=12),
            ],
        )
        request.tables.append(customer)
        request.tables[0].columns.append(ColumnSpec("customer_id", "int", distinct_values=10_000))
        request.joins.append(
            JoinSpec(
                left_table="fact_sales",
                right_table="dim_customer",
                left_key="customer_id",
                right_key="customer_id",
                join_type="inner",
            )
        )

    if "500 distinct" in lower or "500个distinct" in description:
        request.tables[0].columns.append(ColumnSpec("sku_code", "string", distinct_values=500))

    return request


def build_request_from_yaml(yaml_text: str) -> GenerationRequest:
    data = yaml.safe_load(yaml_text) or {}
    tables: list[TableSpec] = []
    joins: list[JoinSpec] = []

    for idx, item in enumerate(data.get("tables", [])):
        t_name = item.get("name", f"table_{idx+1}")
        rows = int(item.get("rows", _default_rows_for_table(t_name, idx)))
        columns = [
            ColumnSpec(
                name=c.get("name"),
                dtype=c.get("dtype", "string"),
                distinct_values=c.get("distinct_values"),
                nullable=bool(c.get("nullable", True)),
                trend_rule=c.get("trend_rule"),
            )
            for c in item.get("columns", [])
            if c.get("name")
        ]
        tables.append(
            TableSpec(
                name=t_name,
                rows=rows,
                columns=columns,
                primary_key=item.get("primary_key"),
            )
        )

    for j in data.get("joins", []):
        joins.append(
            JoinSpec(
                left_table=j["left_table"],
                right_table=j["right_table"],
                left_key=j["left_key"],
                right_key=j["right_key"],
                join_type=j.get("join_type", "inner"),
            )
        )

    if not tables:
        return GenerationRequest.default_single_table(description="empty yaml fallback")

    return GenerationRequest(description=data.get("description"), tables=tables, joins=joins)


def request_as_dict(request: GenerationRequest) -> dict:
    return {
        "description": request.description,
        "tables": [asdict(t) for t in request.tables],
        "joins": [asdict(j) for j in request.joins],
    }


def generate_csv_bundle(request: GenerationRequest, preview_rows: int = 20) -> dict[str, str]:
    files: dict[str, str] = {}
    for table in request.tables:
        cols = table.columns if table.columns else [ColumnSpec("id", "int")]
        header = [c.name for c in cols]
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(header)

        row_count = min(preview_rows, table.rows)
        for i in range(row_count):
            row = []
            for col in cols:
                if col.dtype in {"int", "long"}:
                    row.append(i + 1)
                elif col.dtype in {"double", "float", "decimal"}:
                    row.append(round((i + 1) * 10.5, 2))
                elif col.dtype == "date":
                    row.append(f"2025-01-{(i % 28) + 1:02d}")
                else:
                    row.append(f"{col.name}_{i+1}")
            writer.writerow(row)

        files[f"{table.name}.csv"] = output.getvalue()

    return files
