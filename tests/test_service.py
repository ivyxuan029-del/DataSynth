from synth_tool.models import DEFAULT_TOTAL_ROWS
from synth_tool.service import (
    build_request_from_description,
    build_request_from_yaml,
    generate_csv_bundle,
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


def test_generate_csv_bundle():
    req = build_request_from_description("生成能 join 的销售数据，并且 sku 大概有500个distinct")
    files = generate_csv_bundle(req, preview_rows=5)
    assert len(files) >= 1
    first_content = next(iter(files.values()))
    lines = first_content.strip().splitlines()
    assert len(lines) == 6
