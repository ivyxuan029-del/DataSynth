from __future__ import annotations

from pathlib import Path
import json
import time
import threading
import uuid

import streamlit as st
import yaml

from synth_tool.agent_config import DeepagentConfig, OPENAI_API_URL, OPENAI_MODEL, load_deepagent_config
from synth_tool.agents import CsvGenerationPipelineAgent, PythonCodeGenAgent, PythonExecutionAgent
from synth_tool.tools import ExecutePythonToCsvTool

st.set_page_config(page_title="DataSynth", layout="wide")

st.title("DataSynth")
st.caption("数据需求支持两种输入：模糊描述 / YAML 配置，统一由 AI Agent 生成 CSV。")

# In-memory async job registry (per process).
_JOBS: dict[str, dict[str, object]] = {}
_JOBS_LOCK = threading.Lock()
_JOB_DIR = Path("generated_tools_output") / "jobs"

if "input_mode" not in st.session_state:
    st.session_state.input_mode = "fuzzy"
if "last_input_mode" not in st.session_state:
    st.session_state.last_input_mode = st.session_state.input_mode
if "fuzzy_requirement" not in st.session_state:
    st.session_state.fuzzy_requirement = (
        "生成电商订单表和客户表，还有join后的表，两个表要能join，joinkey是*ID，"
        "客户表中客户id不可以重复，订单表中的用户信息从用户表中抽取，"
        "电商订单表包括订单明细，包含订单号、客户名、城市、商品类目、金额、下单日期，"
        "数据要像真实业务，约200行。"
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
if "generated_code" not in st.session_state:
    st.session_state.generated_code = ""
if "pipeline_message" not in st.session_state:
    st.session_state.pipeline_message = ""
if "output_path" not in st.session_state:
    st.session_state.output_path = ""
if "generated_files_map" not in st.session_state:
    st.session_state.generated_files_map = {}
if "generation_time_sec" not in st.session_state:
    st.session_state.generation_time_sec = None

PREVIEW_LINES = 40
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

if st.session_state.input_mode != st.session_state.last_input_mode:
    st.session_state.pipeline_message = ""
    st.session_state.generated_code = ""
    st.session_state.output_path = ""
    st.session_state.generated_files_map = {}
    st.session_state.generation_time_sec = None
    st.session_state.current_job_id = ""
    st.session_state.last_input_mode = st.session_state.input_mode

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

def _build_requirement_text() -> str:
    if st.session_state.input_mode == "yaml":
        yaml_text = st.session_state.yaml_requirement.strip()
        return (
            "Use the following YAML schema/config to generate realistic synthetic table data.\n"
            "If YAML includes multiple tables or join relationships, return Python code with `tables` dict output.\n"
            "For joins, include joined tables named `join_<left_table>__<right_table>__<join_key>`.\n"
            "Return Python code that creates `rows` (single table) or `tables` (multi-table).\n"
            "YAML:\n"
            f"{yaml_text}"
        )
    return st.session_state.fuzzy_requirement.strip()


def _normalize_generated_files_map(output_path: str, files_map: dict[str, str]) -> dict[str, str]:
    normalized = dict(files_map or {})
    if not normalized and output_path:
        p = Path(output_path)
        if p.exists() and p.suffix.lower() == ".csv":
            normalized[p.name] = str(p)
    return normalized


def _describe_table_name(name: str) -> str:
    return name


def _run_ai_job(job_id: str, requirement_text: str, output_file: str, cfg: DeepagentConfig) -> None:
    _JOB_DIR.mkdir(parents=True, exist_ok=True)
    existing = _load_job(job_id)
    start_ts = existing.get("start_ts") if isinstance(existing, dict) else None
    if not isinstance(start_ts, (int, float)):
        start_ts = time.time()
    with _JOBS_LOCK:
        _JOBS[job_id] = {"status": "running", "start_ts": start_ts}
    _save_job(job_id, {"status": "running", "start_ts": start_ts})
    try:
        codegen_agent = PythonCodeGenAgent(cfg)
        execution_agent = PythonExecutionAgent(ExecutePythonToCsvTool(output_dir="generated_tools_output"))
        pipeline_agent = CsvGenerationPipelineAgent(codegen_agent=codegen_agent, execution_agent=execution_agent)
        result = pipeline_agent.run(requirement=requirement_text, file_name=output_file)
        output_path = result.tool_result.output_path if result.tool_result and result.tool_result.output_path else ""
        raw_map = result.tool_result.metadata.get("generated_files", {}) if result.tool_result and result.tool_result.metadata else {}
        files_map = _normalize_generated_files_map(output_path, raw_map if isinstance(raw_map, dict) else {})
        duration_sec = round(time.time() - start_ts, 2)
        payload = {
            "status": "done" if output_path else "failed",
            "message": result.message,
            "code": result.code,
            "output_path": output_path,
            "files_map": files_map,
            "duration_sec": duration_sec,
            "start_ts": start_ts,
        }
        with _JOBS_LOCK:
            _JOBS[job_id] = payload
        _save_job(job_id, payload)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        payload = {
            "status": "failed",
            "message": f"Job failed: {exc}",
            "duration_sec": None,
            "start_ts": start_ts,
        }
        with _JOBS_LOCK:
            _JOBS[job_id] = payload
        _save_job(job_id, payload)


def _job_path(job_id: str) -> Path:
    return _JOB_DIR / f"{job_id}.json"


def _save_job(job_id: str, payload: dict[str, object]) -> None:
    _JOB_DIR.mkdir(parents=True, exist_ok=True)
    _job_path(job_id).write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _load_job(job_id: str) -> dict[str, object]:
    path = _job_path(job_id)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _rerun() -> None:
    rerun_fn = getattr(st, "rerun", None)
    if callable(rerun_fn):
        rerun_fn()
        return
    rerun_fn = getattr(st, "experimental_rerun", None)
    if callable(rerun_fn):
        rerun_fn()


if st.button("AI 生成数据", use_container_width=True, type="primary"):
    job_id = uuid.uuid4().hex[:10]
    st.session_state.current_job_id = job_id
    requirement_text = _build_requirement_text()
    output_stem = "datasynth_output"
    with _JOBS_LOCK:
        _JOBS[job_id] = {"status": "queued", "start_ts": time.time()}
    _save_job(job_id, {"status": "queued", "start_ts": time.time()})
    worker = threading.Thread(
        target=_run_ai_job,
        args=(job_id, requirement_text, f"{output_stem}.csv", deepagent_cfg),
        daemon=True,
    )
    worker.start()
    st.session_state.pipeline_message = ""
    st.session_state.generated_code = ""
    st.session_state.output_path = ""
    st.session_state.generated_files_map = {}

if "current_job_id" in st.session_state and st.session_state.current_job_id:
    job_id = st.session_state.current_job_id
    with _JOBS_LOCK:
        job = _JOBS.get(job_id, {})
    if not job:
        job = _load_job(job_id)
    status = job.get("status")
    if status not in {"done", "failed"}:
        elapsed_placeholder = st.empty()
        with st.spinner("AI 正在生成数据..."):
            while True:
                latest = _load_job(job_id)
                if latest:
                    job = latest
                status = job.get("status")
                start_ts = job.get("start_ts")
                if isinstance(start_ts, (int, float)):
                    elapsed = max(0.0, time.time() - start_ts)
                    elapsed_placeholder.caption(f"已用时 {elapsed:.1f} 秒")
                if status in {"done", "failed"}:
                    break
                time.sleep(1.0)
        elapsed_placeholder.empty()

    if status in {"done", "failed"}:
        st.session_state.pipeline_message = str(job.get("message", ""))
        st.session_state.generated_code = str(job.get("code", ""))
        st.session_state.output_path = str(job.get("output_path", ""))
        st.session_state.generated_files_map = job.get("files_map", {}) if isinstance(job.get("files_map"), dict) else {}
        st.session_state.generation_time_sec = job.get("duration_sec")

if st.session_state.pipeline_message:
    if st.session_state.output_path:
        st.success(st.session_state.pipeline_message)
    else:
        st.error(st.session_state.pipeline_message)
    if st.session_state.generation_time_sec is not None:
        st.caption(f"AI 生成耗时：{st.session_state.generation_time_sec} 秒")

if st.session_state.generated_code:
    with st.expander("查看 Agent1 生成的 Python 代码", expanded=False):
        st.code(st.session_state.generated_code, language="python")

    if st.session_state.generated_files_map:
        st.markdown("**表预览（基础表与 Join 表）**")
        st.caption(f"仅预览前 {PREVIEW_LINES} 行，完整数据请下载 ZIP。")
        label_map = {}
        for fname in sorted(st.session_state.generated_files_map.keys()):
            label = _describe_table_name(fname)
            label_map[label] = fname
        selected_label = st.selectbox("选择预览表", options=list(label_map.keys()))
        selected_file = label_map[selected_label]
        selected_path = Path(st.session_state.generated_files_map[selected_file])
        if selected_path.exists():
            content = selected_path.read_text(encoding="utf-8")
            st.code("\n".join(content.strip().splitlines()[:PREVIEW_LINES]), language="csv")
        else:
            st.warning("所选表文件不存在，请重新生成。")

    if st.session_state.output_path:
        output_path = Path(st.session_state.output_path)
        if output_path.exists():
            if output_path.suffix.lower() == ".zip":
                st.download_button(
                    "下载 ZIP",
                    data=output_path.read_bytes(),
                    file_name=output_path.name,
                    mime="application/zip",
                    use_container_width=True,
                )
            else:
                csv_content = output_path.read_text(encoding="utf-8")
                st.download_button(
                    "下载 CSV",
                    data=csv_content,
                    file_name=output_path.name,
                    mime="text/csv",
                    use_container_width=True,
                )
        else:
            st.error("输出文件不存在，请重新执行。")
