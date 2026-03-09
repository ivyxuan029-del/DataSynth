from __future__ import annotations

from pathlib import Path

import streamlit as st
import yaml

from synth_tool.agent_config import DeepagentConfig, OPENAI_API_URL, OPENAI_MODEL, load_deepagent_config
from synth_tool.agents import CsvGenerationPipelineAgent, PythonCodeGenAgent, PythonExecutionAgent
from synth_tool.tools import ExecutePythonToCsvTool

st.set_page_config(page_title="DataSynth", layout="wide")

st.title("DataSynth")
st.caption("数据需求支持两种输入：模糊描述 / YAML 配置，统一由 AI Agent 生成 CSV。")

if "input_mode" not in st.session_state:
    st.session_state.input_mode = "fuzzy"
if "fuzzy_requirement" not in st.session_state:
    st.session_state.fuzzy_requirement = (
        "生成电商订单明细，包含订单号、客户名、城市、商品类目、金额、下单日期，"
        "数据要像真实业务，不要只有12345这类简单重复值，约200行。"
    )
if "yaml_requirement" not in st.session_state:
    st.session_state.yaml_requirement = (
        "description: 电商订单\n"
        "tables:\n"
        "  - name: orders\n"
        "    rows: 200\n"
        "    columns:\n"
        "      - name: order_id\n"
        "        dtype: int\n"
        "      - name: customer_name\n"
        "        dtype: string\n"
        "      - name: city\n"
        "        dtype: string\n"
        "      - name: category\n"
        "        dtype: string\n"
        "      - name: amount\n"
        "        dtype: double\n"
        "      - name: order_date\n"
        "        dtype: date\n"
    )
if "file_name" not in st.session_state:
    st.session_state.file_name = "orders.csv"
if "generated_code" not in st.session_state:
    st.session_state.generated_code = ""
if "pipeline_message" not in st.session_state:
    st.session_state.pipeline_message = ""
if "output_path" not in st.session_state:
    st.session_state.output_path = ""
if "ui_api_key" not in st.session_state:
    st.session_state.ui_api_key = ""
if "ui_model" not in st.session_state:
    st.session_state.ui_model = OPENAI_MODEL
if "ui_base_url" not in st.session_state:
    st.session_state.ui_base_url = OPENAI_API_URL

st.markdown("**API 配置（网页端）**")
st.session_state.ui_api_key = st.text_input(
    "API Key",
    value=st.session_state.ui_api_key,
    type="password",
    placeholder="在此输入可覆盖环境变量",
)
c_model, c_base = st.columns([1, 1])
with c_model:
    st.session_state.ui_model = st.text_input("Model", value=st.session_state.ui_model)
with c_base:
    st.session_state.ui_base_url = st.text_input("Base URL", value=st.session_state.ui_base_url)

try:
    if st.session_state.ui_api_key.strip():
        deepagent_cfg = DeepagentConfig(
            provider="deepagent",
            model=st.session_state.ui_model.strip() or OPENAI_MODEL,
            api_key_env="UI_INPUT",
            api_key=st.session_state.ui_api_key.strip(),
            base_url=st.session_state.ui_base_url.strip() or OPENAI_API_URL,
        )
    else:
        deepagent_cfg = load_deepagent_config()
except ValueError as exc:
    st.error(str(exc))
    st.info("可在上方输入 API Key，或在终端设置环境变量：export DEEPAGENT_API_KEY='your-key'")
    st.stop()

st.success(
    f"Deepagent 已就绪: provider={deepagent_cfg.provider}, model={deepagent_cfg.model}, key_source={deepagent_cfg.api_key_env}"
)

st.session_state.input_mode = st.radio(
    "需求输入方式",
    options=["fuzzy", "yaml"],
    format_func=lambda x: "模糊描述" if x == "fuzzy" else "YAML 配置",
    horizontal=True,
)

if st.session_state.input_mode == "fuzzy":
    st.session_state.fuzzy_requirement = st.text_area(
        "模糊需求描述",
        value=st.session_state.fuzzy_requirement,
        height=150,
    )
else:
    uploaded = st.file_uploader("上传 YAML 文件", type=["yaml", "yml"])
    if uploaded is not None:
        st.session_state.yaml_requirement = uploaded.getvalue().decode("utf-8")

    st.session_state.yaml_requirement = st.text_area(
        "YAML 配置",
        value=st.session_state.yaml_requirement,
        height=220,
    )
    if st.session_state.yaml_requirement.strip():
        try:
            yaml.safe_load(st.session_state.yaml_requirement)
            st.success("YAML 格式校验通过")
        except Exception as exc:  # pylint: disable=broad-exception-caught
            st.warning(f"YAML 格式可能有问题：{exc}")

st.session_state.file_name = st.text_input(
    "CSV 文件名",
    value=st.session_state.file_name,
    placeholder="例如：orders.csv",
)


def _build_requirement_text() -> str:
    if st.session_state.input_mode == "yaml":
        yaml_text = st.session_state.yaml_requirement.strip()
        return (
            "Use the following YAML schema/config to generate realistic synthetic table data.\n"
            "Return Python code that creates `rows` as list[dict] for the final CSV output.\n"
            "YAML:\n"
            f"{yaml_text}"
        )
    return st.session_state.fuzzy_requirement.strip()


if st.button("AI 生成并导出 CSV", use_container_width=True, type="primary"):
    codegen_agent = PythonCodeGenAgent(deepagent_cfg)
    execution_agent = PythonExecutionAgent(ExecutePythonToCsvTool(output_dir="generated_tools_output"))
    pipeline_agent = CsvGenerationPipelineAgent(codegen_agent=codegen_agent, execution_agent=execution_agent)

    result = pipeline_agent.run(
        requirement=_build_requirement_text(),
        file_name=st.session_state.file_name,
    )
    st.session_state.pipeline_message = result.message
    st.session_state.generated_code = result.code
    st.session_state.output_path = result.tool_result.output_path if result.tool_result and result.tool_result.output_path else ""

if st.session_state.pipeline_message:
    if st.session_state.output_path:
        st.success(st.session_state.pipeline_message)
    else:
        st.error(st.session_state.pipeline_message)

if st.session_state.generated_code:
    with st.expander("查看 Agent1 生成的 Python 代码", expanded=False):
        st.code(st.session_state.generated_code, language="python")

if st.session_state.output_path:
    output_path = Path(st.session_state.output_path)
    if output_path.exists():
        csv_content = output_path.read_text(encoding="utf-8")
        preview_lines = csv_content.strip().splitlines()[:40]
        st.markdown("**CSV 预览（前 40 行）**")
        st.code("\n".join(preview_lines), language="csv")
        st.download_button(
            "下载 CSV",
            data=csv_content,
            file_name=output_path.name,
            mime="text/csv",
            use_container_width=True,
        )
    else:
        st.error("输出文件不存在，请重新执行。")
