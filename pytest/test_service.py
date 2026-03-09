from synth_tool.models import DEFAULT_TOTAL_ROWS
from synth_tool.service import (
    build_request_from_description,
    build_request_from_yaml,
    generate_csv_bundle,
    generate_dataframe_bundle,
)


def test_description_default_request():
    req = build_request_from_description("生成销售数据")
    assert len(req.tables) == 1
    assert req.tables[0].rows == DEFAULT_TOTAL_ROWS


def test_description_join_request():
    req = build_request_from_description("请创建能 join 的订单客户数据")
    assert len(req.tables) >= 2
    assert len(req.joins) == 1
    assert req.joins[0].left_key == "customer_id"


def test_description_cn_two_tables_join_acceptance():
    req = build_request_from_description("订单和客户两表可join")
    table_names = {t.name for t in req.tables}
    assert "fact_order" in table_names
    assert "dim_customer" in table_names
    assert len(req.joins) == 1
    assert req.joins[0].left_table == "fact_order"
    assert req.joins[0].right_table == "dim_customer"
    assert req.joins[0].left_key == "customer_id"
    assert req.joins[0].right_key == "customer_id"


def test_yaml_parse_with_defaults():
    yaml_text = """
description: test

tables:
  - name: fact_sales
    columns:
      - name: sale_id
        dtype: int
  - name: dim_region
    columns:
      - name: region_id
        dtype: int
joins:
  - left_table: fact_sales
    right_table: dim_region
    left_key: region_id
    right_key: region_id
"""
    req = build_request_from_yaml(yaml_text)
    assert len(req.tables) == 2
    assert req.tables[0].rows == DEFAULT_TOTAL_ROWS
    assert req.tables[1].rows == 10000
    assert len(req.joins) == 1


def test_yaml_default_row_strategy_main_and_dimension():
    yaml_text = """
tables:
  - name: dim_customer
    columns:
      - name: customer_id
        dtype: int
  - name: fact_order
    columns:
      - name: order_id
        dtype: int
  - name: bridge_order_item
    columns:
      - name: order_id
        dtype: int
"""
    req = build_request_from_yaml(yaml_text)
    rows_by_name = {t.name: t.rows for t in req.tables}
    assert rows_by_name["fact_order"] == DEFAULT_TOTAL_ROWS
    assert rows_by_name["dim_customer"] == 10000
    assert rows_by_name["bridge_order_item"] == 25000


def test_generate_csv_bundle():
    req = build_request_from_description("生成能 join 的销售数据，并且 sku 大概有500个distinct")
    files = generate_csv_bundle(req, preview_rows=5)
    assert len(files) >= 1
    first_content = next(iter(files.values()))
    lines = first_content.strip().splitlines()
    assert len(lines) == 6


def test_generate_csv_with_missing_rows_in_yaml():
    yaml_text = """
tables:
  - name: fact_sales
    columns:
      - name: id
        dtype: int
  - name: dim_region
    columns:
      - name: region_id
        dtype: int
"""
    req = build_request_from_yaml(yaml_text)
    files = generate_csv_bundle(req, preview_rows=3)
    assert "fact_sales.csv" in files
    assert "dim_region.csv" in files
    assert len(files["fact_sales.csv"].strip().splitlines()) == 4


def test_mvp_engine_generates_typed_columns_and_non_empty_csv():
    yaml_text = """
tables:
  - name: fact_metrics
    rows: 12
    columns:
      - name: metric_id
        dtype: int
      - name: metric_name
        dtype: string
      - name: metric_day
        dtype: date
      - name: metric_value
        dtype: double
"""
    req = build_request_from_yaml(yaml_text)
    files = generate_csv_bundle(req, preview_rows=5)
    content = files["fact_metrics.csv"].strip().splitlines()
    assert len(content) == 6
    assert content[0] == "metric_id,metric_name,metric_day,metric_value"
    # First data line should have four non-empty comma-separated fields
    first_row = content[1].split(",")
    assert len(first_row) == 4
    assert all(cell != "" for cell in first_row)


def test_generate_dataframe_bundle_schema_matches_column_definitions():
    yaml_text = """
tables:
  - name: dim_item
    columns:
      - name: item_id
        dtype: int
      - name: item_name
        dtype: string
"""
    req = build_request_from_yaml(yaml_text)
    tables = generate_dataframe_bundle(req, preview_rows=3)
    assert "dim_item" in tables
    assert len(tables["dim_item"]) == 3
    assert list(tables["dim_item"][0].keys()) == ["item_id", "item_name"]
