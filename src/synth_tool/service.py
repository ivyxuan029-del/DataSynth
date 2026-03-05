from __future__ import annotations

import csv
import io
import re
from datetime import date, timedelta
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
    if _is_dimension_table(name):
        return 10_000
    if index == 0:
        return DEFAULT_TOTAL_ROWS
    return 25_000


def _is_dimension_table(name: str) -> bool:
    lowered = name.lower()
    return any(token in lowered for token in ["dim", "dimension", "lookup", "字典", "维表"])


def _mk_order_table(rows: int | None = None) -> TableSpec:
    row_count = rows if rows is not None else DEFAULT_TOTAL_ROWS
    return TableSpec(
        name="fact_order",
        rows=row_count,
        primary_key="order_id",
        columns=[
            ColumnSpec("order_id", "int", distinct_values=row_count, nullable=False),
            ColumnSpec("customer_id", "int", distinct_values=min(10_000, row_count), nullable=False),
            ColumnSpec("order_date", "date"),
            ColumnSpec("order_amount", "double"),
        ],
    )


def _mk_customer_table(rows: int | None = None) -> TableSpec:
    row_count = rows if rows is not None else 10_000
    return TableSpec(
        name="dim_customer",
        rows=row_count,
        primary_key="customer_id",
        columns=[
            ColumnSpec("customer_id", "int", distinct_values=row_count, nullable=False),
            ColumnSpec("customer_name", "string"),
            ColumnSpec("segment", "string", distinct_values=12),
        ],
    )


def _parse_row_count(description: str) -> int | None:
    # Examples: "10万行", "5000 rows", "20000 row"
    cn_match = re.search(r"(\d+)\s*万\s*行", description)
    if cn_match:
        return int(cn_match.group(1)) * 10_000

    num_match = re.search(r"(\d[\d,]*)\s*(行|rows?|条)", description.lower())
    if num_match:
        return int(num_match.group(1).replace(",", ""))
    return None


def _parse_distinct_count(description: str) -> int | None:
    match = re.search(r"(\d+)\s*(个)?\s*distinct", description.lower())
    if match:
        return int(match.group(1))
    return None


def _has_join_intent(description: str) -> bool:
    lower = description.lower()
    return any(token in lower for token in ["join", "关联", "连接", "可join"])


def build_request_from_description(description: str) -> GenerationRequest:
    lower = description.lower()
    row_count = _parse_row_count(description)

    wants_order = any(token in description for token in ["订单"]) or any(token in lower for token in ["order", "orders"])
    wants_customer = any(token in description for token in ["客户"]) or any(
        token in lower for token in ["customer", "customers"]
    )
    wants_join = _has_join_intent(description)

    tables: list[TableSpec] = []
    joins: list[JoinSpec] = []

    if wants_order:
        tables.append(_mk_order_table(rows=row_count))
    if wants_customer:
        tables.append(_mk_customer_table())

    if not tables:
        request = GenerationRequest.default_single_table(description=description)
        if row_count is not None:
            request.tables[0].rows = row_count
            request.tables[0].columns[0].distinct_values = row_count
    else:
        request = GenerationRequest(description=description, tables=tables, joins=joins)

    # join rule: if user表达了join意图并且有订单和客户表，则配置 customer_id 连接
    table_names = {table.name for table in request.tables}
    if wants_join and "fact_order" in table_names and "dim_customer" in table_names:
        order_table = next(t for t in request.tables if t.name == "fact_order")
        has_customer_id = any(c.name == "customer_id" for c in order_table.columns)
        if not has_customer_id:
            order_table.columns.append(ColumnSpec("customer_id", "int", distinct_values=min(10_000, order_table.rows)))

        request.joins.append(
            JoinSpec(
                left_table="fact_order",
                right_table="dim_customer",
                left_key="customer_id",
                right_key="customer_id",
                join_type="inner",
            )
        )

    elif wants_join and len(request.tables) >= 2 and not request.joins:
        # fallback: use id-style join for first two tables
        left = request.tables[0]
        right = request.tables[1]
        left_key = "id"
        right_key = "id"
        if not any(c.name == "id" for c in left.columns):
            left.columns.append(ColumnSpec("id", "int", distinct_values=left.rows, nullable=False))
        if not any(c.name == "id" for c in right.columns):
            right.columns.append(ColumnSpec("id", "int", distinct_values=right.rows, nullable=False))

        request.joins.append(
            JoinSpec(
                left_table=left.name,
                right_table=right.name,
                left_key=left_key,
                right_key=right_key,
                join_type="inner",
            )
        )

    distinct_count = _parse_distinct_count(description)
    if distinct_count is not None and request.tables:
        first_table = request.tables[0]
        if not any(c.name == "category_code" for c in first_table.columns):
            first_table.columns.append(ColumnSpec("category_code", "string", distinct_values=distinct_count))
        else:
            for col in first_table.columns:
                if col.name == "category_code":
                    col.distinct_values = distinct_count
                    break

    return request


def build_request_from_yaml(yaml_text: str) -> GenerationRequest:
    data = yaml.safe_load(yaml_text) or {}
    tables: list[TableSpec] = []
    joins: list[JoinSpec] = []
    raw_tables = data.get("tables", [])

    main_table_index = 0
    for idx, item in enumerate(raw_tables):
        t_name = item.get("name", f"table_{idx+1}")
        if not _is_dimension_table(t_name):
            main_table_index = idx
            break

    for idx, item in enumerate(raw_tables):
        t_name = item.get("name", f"table_{idx+1}")
        relative_idx = 0 if idx == main_table_index else 1
        rows = int(item.get("rows", _default_rows_for_table(t_name, relative_idx)))
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


def _effective_row_count(table: TableSpec, preview_rows: int) -> int:
    return max(1, min(preview_rows, max(1, table.rows)))


def _int_value(index: int, distinct_values: int | None) -> int:
    if distinct_values is not None and distinct_values > 0:
        return (index % distinct_values) + 1
    return index + 1


def _double_value(index: int, distinct_values: int | None) -> float:
    if distinct_values is not None and distinct_values > 0:
        offset = float(index % distinct_values)
    else:
        offset = float(index)
    return round(100.0 + offset * 1.25, 2)


def _date_value(index: int, distinct_values: int | None) -> str:
    anchor = date(2025, 1, 1)
    cycle = distinct_values if distinct_values is not None and distinct_values > 0 else 365
    return (anchor + timedelta(days=index % cycle)).isoformat()


def _string_value(col_name: str, index: int, distinct_values: int | None) -> str:
    if distinct_values is not None and distinct_values > 0:
        token = (index % distinct_values) + 1
    else:
        token = index + 1
    return f"{col_name}_{token}"


def _value_for_column(col: ColumnSpec, index: int):
    dtype = col.dtype.lower()
    if dtype in {"int", "integer", "long", "bigint"}:
        return _int_value(index, col.distinct_values)
    if dtype in {"double", "float", "decimal", "numeric"}:
        return _double_value(index, col.distinct_values)
    if dtype in {"date", "timestamp", "datetime"}:
        return _date_value(index, col.distinct_values)
    return _string_value(col.name, index, col.distinct_values)


def _generate_table_records(table: TableSpec, preview_rows: int) -> list[dict]:
    columns = table.columns if table.columns else [ColumnSpec("id", "int", nullable=False)]
    row_count = _effective_row_count(table, preview_rows)

    records: list[dict] = []
    for row_idx in range(row_count):
        row = {}
        for col in columns:
            row[col.name] = _value_for_column(col, row_idx)
        records.append(row)
    return records


def generate_dataframe_bundle(request: GenerationRequest, preview_rows: int = 20) -> dict[str, list[dict]]:
    """
    Generate table data in a DataFrame-like representation.
    Returns dict[table_name, list[dict]] to keep runtime dependencies minimal.
    """
    return {table.name: _generate_table_records(table, preview_rows) for table in request.tables}


def generate_csv_bundle(request: GenerationRequest, preview_rows: int = 20) -> dict[str, str]:
    files: dict[str, str] = {}
    table_records = generate_dataframe_bundle(request, preview_rows=preview_rows)

    for table in request.tables:
        cols = table.columns if table.columns else [ColumnSpec("id", "int", nullable=False)]
        header = [col.name for col in cols]
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=header)
        writer.writeheader()
        for row in table_records.get(table.name, []):
            writer.writerow({k: row.get(k, "") for k in header})

        files[f"{table.name}.csv"] = output.getvalue()

    return files
