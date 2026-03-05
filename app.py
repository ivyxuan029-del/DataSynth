from __future__ import annotations

import streamlit as st

from synth_tool.service import (
    build_request_from_description,
    build_request_from_yaml,
    generate_csv_bundle,
    request_as_dict,
)

st.set_page_config(page_title="Synthetic Data Studio", layout="wide")
st.title("Synthetic Data Studio")
st.caption("支持业务描述或 Semantic YAML，生成可 Join 的多表 CSV 数据")

mode = st.radio("输入模式", ["业务描述", "Semantic YAML"], horizontal=True)

if "request" not in st.session_state:
    st.session_state.request = None

if mode == "业务描述":
    desc = st.text_area(
        "输入业务描述",
        placeholder="例如：生成订单和客户两张表，可 join；订单金额 2025 年高于 2023 年；sku 约 500 个 distinct。",
        height=140,
    )
    if st.button("解析描述", use_container_width=True):
        st.session_state.request = build_request_from_description(desc.strip())
else:
    yaml_input = st.text_area(
        "输入 Semantic View YAML",
        placeholder="description: demo\ntables:\n  - name: fact_sales\n    rows: 100000\n    columns:\n      - name: id\n        dtype: int\njoins: []",
        height=220,
    )
    if st.button("解析 YAML", use_container_width=True):
        st.session_state.request = build_request_from_yaml(yaml_input)

request = st.session_state.request

if request is not None:
    st.subheader("解析结果")
    st.json(request_as_dict(request))

    col1, col2 = st.columns([1, 1])
    with col1:
        preview_rows = st.number_input("每表预览行数", min_value=1, max_value=200, value=20)
    with col2:
        run_gen = st.button("生成预览 CSV", type="primary", use_container_width=True)

    if run_gen:
        files = generate_csv_bundle(request, preview_rows=int(preview_rows))
        st.success(f"已生成 {len(files)} 个 CSV 文件")
        for filename, content in files.items():
            st.markdown(f"**{filename}**")
            st.download_button(
                label=f"下载 {filename}",
                data=content,
                file_name=filename,
                mime="text/csv",
                key=f"dl_{filename}",
            )
            st.code("\n".join(content.splitlines()[:8]), language="csv")
else:
    st.info("请先输入描述或 YAML，再点击解析。")
