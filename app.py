from __future__ import annotations

import csv
import io
import zipfile

import streamlit as st

from synth_tool.service import build_request_from_description, build_request_from_yaml, generate_csv_bundle

st.set_page_config(page_title="Synthetic Data Studio", layout="wide")

st.markdown(
    """
    <style>
    .block-container {
      padding-top: 1.2rem;
      padding-bottom: 8rem;
      max-width: 980px;
    }

    .section-card {
      border: 1px solid #e5e7eb;
      border-radius: 14px;
      padding: 16px 18px;
      background: #ffffff;
      box-shadow: 0 4px 18px rgba(15, 23, 42, 0.04);
      margin-bottom: 14px;
    }

    .section-title {
      font-size: 20px;
      font-weight: 700;
      margin-bottom: 10px;
      color: #0f172a;
    }

    .st-key-bottom_bar {
      position: fixed;
      left: 0;
      right: 0;
      bottom: 0;
      background: #ffffff;
      border-top: 1px solid #e5e7eb;
      padding: 10px 20px;
      z-index: 9999;
    }

    .st-key-bottom_bar [data-testid="stHorizontalBlock"] {
      max-width: 980px;
      margin: 0 auto;
      align-items: center;
    }

    .st-key-bottom_bar button {
      height: 42px;
    }

    .st-key-bottom_bar input {
      height: 42px;
    }

    .status-ok {
      color: #166534;
      font-weight: 600;
      margin-top: 6px;
    }

    .status-warn {
      color: #92400e;
      font-weight: 600;
      margin-top: 6px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Synthetic Data Studio")

if "yaml_text" not in st.session_state:
    st.session_state.yaml_text = ""
if "uploaded_file_name" not in st.session_state:
    st.session_state.uploaded_file_name = ""
if "request" not in st.session_state:
    st.session_state.request = None
if "generated_files" not in st.session_state:
    st.session_state.generated_files = {}
if "preview_rows" not in st.session_state:
    st.session_state.preview_rows = 20
if "bottom_prompt" not in st.session_state:
    st.session_state.bottom_prompt = ""
if "parse_error" not in st.session_state:
    st.session_state.parse_error = ""
if "description_text" not in st.session_state:
    st.session_state.description_text = ""


def parse_yaml_to_request() -> None:
    """Parse YAML from session_state and update request + parse status."""
    yaml_text = st.session_state.yaml_text.strip()
    if not yaml_text:
        st.session_state.request = None
        st.session_state.parse_error = "YAML 内容为空，请粘贴或上传后再解析。"
        return

    try:
        st.session_state.request = build_request_from_yaml(yaml_text)
        st.session_state.parse_error = ""
    except Exception as exc:  # pylint: disable=broad-exception-caught
        st.session_state.request = None
        st.session_state.parse_error = f"YAML 解析失败：{exc}"


def parse_description_to_request() -> None:
    """Parse natural-language description into the same request object."""
    description = st.session_state.description_text.strip()
    if not description:
        st.session_state.request = None
        st.session_state.parse_error = "描述内容为空，请输入业务描述后再解析。"
        return

    try:
        st.session_state.request = build_request_from_description(description)
        st.session_state.parse_error = ""
    except Exception as exc:  # pylint: disable=broad-exception-caught
        st.session_state.request = None
        st.session_state.parse_error = f"描述解析失败：{exc}"

# 1. 上传YAML
st.markdown('<div class="section-card">', unsafe_allow_html=True)
st.markdown('<div class="section-title">1. 上传配置文件（YAML）</div>', unsafe_allow_html=True)
st.caption("支持两种输入：底部“+”上传 .yaml/.yml 文件，或直接在下方粘贴 YAML 文本。")

if st.session_state.uploaded_file_name:
    st.markdown(
        f'<div class="status-ok">已上传：{st.session_state.uploaded_file_name}</div>',
        unsafe_allow_html=True,
    )
else:
    st.markdown('<div class="status-warn">暂未上传 YAML 文件</div>', unsafe_allow_html=True)

st.session_state.yaml_text = st.text_area(
    "YAML 输入",
    value=st.session_state.yaml_text,
    height=190,
    placeholder=(
        "description: demo\n"
        "tables:\n"
        "  - name: fact_sales\n"
        "    rows: 100000\n"
        "    primary_key: id\n"
        "    columns:\n"
        "      - name: id\n"
        "        dtype: int\n"
        "joins: []"
    ),
)

parse_clicked = st.button("解析配置", use_container_width=True)
if parse_clicked:
    parse_yaml_to_request()

st.markdown("---")
st.session_state.description_text = st.text_area(
    "模糊描述输入（自然语言）",
    value=st.session_state.description_text,
    height=100,
    placeholder="例如：订单和客户两表可join，订单表 5000 行，某列大概 500 distinct。",
)
parse_desc_clicked = st.button("解析描述", use_container_width=True)
if parse_desc_clicked:
    parse_description_to_request()

if st.session_state.parse_error:
    st.error(st.session_state.parse_error)
elif st.session_state.request is not None:
    st.success("YAML 解析成功")
    table_rows = []
    column_rows = []
    for table in st.session_state.request.tables:
        table_rows.append(
            {
                "table_name": table.name,
                "rows": table.rows,
                "primary_key": table.primary_key or "",
                "column_count": len(table.columns),
            }
        )
        for col in table.columns:
            column_rows.append(
                {
                    "table_name": table.name,
                    "column_name": col.name,
                    "dtype": col.dtype,
                    "distinct_values": col.distinct_values,
                    "nullable": col.nullable,
                }
            )

    st.markdown("**表信息**")
    st.dataframe(table_rows, use_container_width=True, height=170)
    st.markdown("**列信息**")
    st.dataframe(column_rows, use_container_width=True, height=220)

st.markdown("</div>", unsafe_allow_html=True)

# 2. 生成数据
st.markdown('<div class="section-card">', unsafe_allow_html=True)
st.markdown('<div class="section-title">2. 生成模拟数据</div>', unsafe_allow_html=True)
left, right = st.columns([1, 1])
with left:
    st.session_state.preview_rows = st.number_input(
        "行数输入（每表预览行数）", min_value=1, max_value=2000, value=int(st.session_state.preview_rows)
    )
with right:
    run_generation = st.button("生成数据", use_container_width=True, type="primary")

if run_generation:
    if st.session_state.request is None:
        if st.session_state.yaml_text.strip():
            parse_yaml_to_request()
        elif st.session_state.description_text.strip():
            parse_description_to_request()

    if st.session_state.request is None:
        st.warning("请先输入合法 YAML 或业务描述并解析成功")
    else:
        st.session_state.generated_files = generate_csv_bundle(st.session_state.request, preview_rows=int(st.session_state.preview_rows))
        st.success(f"已生成 {len(st.session_state.generated_files)} 个 CSV 文件")

if st.session_state.generated_files:
    first_file = next(iter(st.session_state.generated_files.values()))
    reader = csv.DictReader(io.StringIO(first_file))
    preview_data = list(reader)
    st.dataframe(preview_data, use_container_width=True, height=260)
else:
    st.info("生成后将在这里显示数据预览")

st.markdown("</div>", unsafe_allow_html=True)

# 3. 下载数据
st.markdown('<div class="section-card">', unsafe_allow_html=True)
st.markdown('<div class="section-title">3. 下载生成结果</div>', unsafe_allow_html=True)

d1, d2 = st.columns([1, 1])

with d1:
    if st.session_state.generated_files:
        first_name, first_content = next(iter(st.session_state.generated_files.items()))
        st.download_button(
            "下载CSV",
            data=first_content,
            file_name=first_name,
            mime="text/csv",
            use_container_width=True,
        )
    else:
        st.button("下载CSV", disabled=True, use_container_width=True)

with d2:
    if st.session_state.generated_files:
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            for name, content in st.session_state.generated_files.items():
                zf.writestr(name, content)
        zip_buffer.seek(0)
        st.download_button(
            "下载zip",
            data=zip_buffer.getvalue(),
            file_name="generated_data.zip",
            mime="application/zip",
            use_container_width=True,
        )
    else:
        st.button("下载zip", disabled=True, use_container_width=True)

st.markdown("</div>", unsafe_allow_html=True)

# 底部固定输入区
bottom_bar = st.container(key="bottom_bar")
with bottom_bar:
    b1, b2, b3 = st.columns([1, 8, 1])

    with b1:
        with st.popover("+"):
            uploaded = st.file_uploader("上传 YAML", type=["yaml", "yml"], label_visibility="collapsed")
            if uploaded is not None:
                st.session_state.uploaded_file_name = uploaded.name
                st.session_state.yaml_text = uploaded.getvalue().decode("utf-8")
                st.success("上传成功，点击“解析配置”完成解析")

    with b2:
        st.session_state.bottom_prompt = st.text_input(
            "输入提示",
            value=st.session_state.bottom_prompt,
            placeholder="输入提示（例如：解析并生成 20 行数据）",
            label_visibility="collapsed",
        )

    with b3:
        if st.button("发送", use_container_width=True):
            if st.session_state.bottom_prompt.strip():
                st.session_state.description_text = st.session_state.bottom_prompt
                parse_description_to_request()
                if st.session_state.request is not None:
                    st.toast("描述已解析", icon="✅")
                else:
                    st.toast("描述解析失败", icon="⚠️")
            else:
                st.toast("请输入提示后再发送", icon="⚠️")
