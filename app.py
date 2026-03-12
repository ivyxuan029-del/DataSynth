from __future__ import annotations

from pathlib import Path
import csv
import json
import time
import threading
import uuid
import shutil
import subprocess
import shlex
from urllib import request, error

import streamlit as st
import yaml

from synth_tool.agent_config import DeepagentConfig, OPENAI_API_URL, OPENAI_MODEL, load_deepagent_config
from synth_tool.agents import CsvGenerationPipelineAgent, PythonCodeGenAgent, PythonExecutionAgent, KeywordExtractAgent
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
if "source_notice" not in st.session_state:
    st.session_state.source_notice = ""
if "source_link" not in st.session_state:
    st.session_state.source_link = ""
if "source_title" not in st.session_state:
    st.session_state.source_title = ""
if "source_subtitle" not in st.session_state:
    st.session_state.source_subtitle = ""
if "source_description" not in st.session_state:
    st.session_state.source_description = ""
if "table_desc_cache" not in st.session_state:
    st.session_state.table_desc_cache = {}
if "status_note" not in st.session_state:
    st.session_state.status_note = ""

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
    st.session_state.source_notice = ""
    st.session_state.source_link = ""
    st.session_state.source_title = ""
    st.session_state.source_subtitle = ""
    st.session_state.source_description = ""
    st.session_state.status_note = ""
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
    st.caption("会自动尝试匹配 Kaggle 数据集；或显式指定：source: kaggle, dataset: owner/slug, file: your_file.csv")
    if st.session_state.yaml_requirement.strip():
        try:
            yaml.safe_load(st.session_state.yaml_requirement)
            st.success("YAML 格式校验通过")
        except Exception as exc:  # pylint: disable=broad-exception-caught
            st.warning(f"YAML 格式可能有问题：{exc}")

def _build_requirement_text() -> str:
    if st.session_state.input_mode == "yaml":
        yaml_text = st.session_state.yaml_requirement.strip()
        return _build_yaml_requirement_text(yaml_text)
    return st.session_state.fuzzy_requirement.strip()


def _build_yaml_requirement_text(yaml_text: str) -> str:
    return (
        "Use the following YAML schema/config to generate realistic synthetic table data.\n"
        "If YAML includes multiple tables or join relationships, return Python code with `tables` dict output.\n"
        "For joins, include joined tables named `join_<left_table>__<right_table>__<join_key>`.\n"
        "Also accept `relationships` format with relationship_columns.left_column/right_column.\n"
        "Return Python code that creates `rows` (single table) or `tables` (multi-table).\n"
        "YAML:\n"
        f"{yaml_text}"
    )


def _normalize_generated_files_map(output_path: str, files_map: dict[str, str]) -> dict[str, str]:
    normalized = dict(files_map or {})
    if not normalized and output_path:
        p = Path(output_path)
        if p.exists() and p.suffix.lower() == ".csv":
            normalized[p.name] = str(p)
    return normalized


def _describe_table_name(name: str) -> str:
    return name


def _parse_kaggle_spec(yaml_text: str) -> dict[str, str]:
    try:
        data = yaml.safe_load(yaml_text) or {}
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    if str(data.get("source", "")).strip().lower() != "kaggle":
        return {}
    dataset = str(data.get("dataset", "")).strip()
    if not dataset:
        return {}
    file_name = str(data.get("file", "")).strip()
    return {"dataset": dataset, "file": file_name}


def _parse_yaml_tables_and_joins(yaml_text: str) -> tuple[list[dict], list[dict]]:
    try:
        data = yaml.safe_load(yaml_text) or {}
    except Exception:
        return [], []
    if not isinstance(data, dict):
        return [], []
    tables = data.get("tables", [])
    joins = data.get("joins", [])
    relationships = data.get("relationships", [])
    if not isinstance(tables, list):
        tables = []
    if not isinstance(joins, list):
        joins = []
    if not isinstance(relationships, list):
        relationships = []

    # Normalize relationships -> joins
    for rel in relationships:
        if not isinstance(rel, dict):
            continue
        left_table = rel.get("left_table")
        right_table = rel.get("right_table")
        rel_cols = rel.get("relationship_columns", [])
        if not left_table or not right_table or not isinstance(rel_cols, list) or not rel_cols:
            continue
        first = rel_cols[0] if isinstance(rel_cols[0], dict) else {}
        left_key = first.get("left_column") or first.get("left_key")
        right_key = first.get("right_column") or first.get("right_key")
        if not left_key or not right_key:
            continue
        joins.append(
            {
                "left_table": left_table,
                "right_table": right_table,
                "left_key": left_key,
                "right_key": right_key,
                "join_type": rel.get("join_type", "inner"),
            }
        )

    return tables, joins


def _sanitize_slug(text: str) -> str:
    safe = []
    for ch in text:
        if ch.isalnum() or ch in {"-", "_"}:
            safe.append(ch)
        else:
            safe.append("_")
    return "".join(safe)


def _zip_files(files_map: dict[str, str], output_dir: Path, output_name: str) -> str:
    import zipfile

    zip_path = output_dir / output_name
    with zipfile.ZipFile(zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for fname, fpath in files_map.items():
            zf.write(fpath, arcname=fname)
    return str(zip_path)


def _is_kaggle_data_file(path: Path) -> bool:
    if not path.is_file():
        return False
    name = path.name
    if name.startswith("."):
        return False
    if name in {".cache_complete", "dataset-metadata.json", "kaggle_dataset.zip"}:
        return False
    if name.lower().endswith(".zip"):
        return False
    return True


def _read_text_safe(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        try:
            return path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            return path.read_text(encoding="latin-1", errors="replace")


def _csv_basic_stats(path: Path) -> tuple[int | None, int | None]:
    try:
        with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if header is None:
                return 0, 0
            col_count = len(header)
            row_count = 0
            for _ in reader:
                row_count += 1
            return row_count, col_count
    except Exception:
        return None, None


def _chat_completions_url(cfg: DeepagentConfig) -> str:
    if cfg.base_url:
        base = cfg.base_url.rstrip("/")
        if base.endswith("/chat/completions"):
            return base
        return f"{base}/chat/completions"
    return "https://api.openai.com/v1/chat/completions"


def _ai_describe_table(sample_text: str, cfg: DeepagentConfig) -> str:
    system_prompt = (
        "You are a data analyst. Summarize what this table is about.\n"
        "Return 1-2 concise sentences in Chinese.\n"
        "Do not mention that you are an AI.\n"
    )
    user_prompt = f"Sample table content (header + few rows):\n{sample_text}"
    payload = {
        "model": cfg.model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.2,
    }
    data = json.dumps(payload).encode("utf-8")
    req = request.Request(
        _chat_completions_url(cfg),
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {cfg.api_key}",
        },
    )
    try:
        with request.urlopen(req, timeout=45) as resp:
            body = json.loads(resp.read().decode("utf-8"))
        content = body["choices"][0]["message"]["content"]
        return str(content).strip()
    except (error.HTTPError, error.URLError, KeyError, ValueError, UnicodeDecodeError):
        return ""


def _extract_kaggle_archives(out_dir: Path) -> None:
    import zipfile

    for z in sorted(out_dir.rglob("*.zip")):
        if z.name in {"kaggle_dataset.zip"}:
            continue
        if z.name.startswith("."):
            continue
        try:
            with zipfile.ZipFile(z, "r") as zf:
                zf.extractall(z.parent)
        except Exception:
            continue


def _extract_kaggle_archives_fallback(out_dir: Path) -> None:
    # Fallback to system unzip when python zipfile fails silently.
    for z in sorted(out_dir.rglob("*.zip")):
        if z.name in {"kaggle_dataset.zip"} or z.name.startswith("."):
            continue
        cmd = f"unzip -o {shlex.quote(str(z))} -d {shlex.quote(str(z.parent))}"
        subprocess.run(cmd, shell=True, capture_output=True, text=True)


def _collect_kaggle_csv_files(out_dir: Path) -> dict[str, str]:
    csv_files: dict[str, str] = {}
    for p in sorted(out_dir.rglob("*.csv")):
        if _is_kaggle_data_file(p):
            rel = p.relative_to(out_dir).as_posix()
            csv_files[rel] = str(p)
    return csv_files


def _convert_kaggle_files_to_csv(out_dir: Path) -> dict[str, str]:
    try:
        import pandas as pd  # type: ignore
    except Exception as exc:  # pylint: disable=broad-exception-caught
        raise RuntimeError("需要安装 pandas 以将 Kaggle 文件转换为 CSV。") from exc

    converted_dir = out_dir / "converted_csv"
    converted_dir.mkdir(parents=True, exist_ok=True)

    files_map: dict[str, str] = {}
    for p in sorted(out_dir.rglob("*")):
        if not _is_kaggle_data_file(p):
            continue
        suffix = p.suffix.lower()
        try:
            if suffix == ".csv":
                files_map[p.name] = str(p)
                continue
            if suffix in {".tsv", ".txt"}:
                df = pd.read_csv(p, sep="\t")
            elif suffix in {".xls", ".xlsx"}:
                df = pd.read_excel(p)
            elif suffix in {".json"}:
                df = pd.read_json(p)
            elif suffix in {".parquet"}:
                df = pd.read_parquet(p)
            else:
                # Skip unknown binary formats.
                continue
            rel = p.relative_to(out_dir)
            out_name = f"{rel.stem}.csv"
            out_path = converted_dir / out_name
            df.to_csv(out_path, index=False, encoding="utf-8")
            files_map[out_path.relative_to(converted_dir).as_posix()] = str(out_path)
        except Exception:
            # Skip files that cannot be converted.
            continue

    return files_map


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
        kaggle_payload = _try_kaggle_from_requirement(requirement_text, cfg, start_ts, job_id)
        if kaggle_payload:
            with _JOBS_LOCK:
                _JOBS[job_id] = kaggle_payload
            _save_job(job_id, kaggle_payload)
            return
        # If YAML is too long, split by table and generate in parts (no joins).
        yaml_text = ""
        if "YAML:\n" in requirement_text:
            yaml_text = requirement_text.split("YAML:\n", 1)[1]
        tables, joins = _parse_yaml_tables_and_joins(yaml_text) if yaml_text else ([], [])
        if yaml_text and len(yaml_text) > 500 and tables:
            codegen_agent = PythonCodeGenAgent(cfg)
            execution_agent = PythonExecutionAgent(ExecutePythonToCsvTool(output_dir="generated_tools_output"))
            pipeline_agent = CsvGenerationPipelineAgent(codegen_agent=codegen_agent, execution_agent=execution_agent)
            files_map: dict[str, str] = {}
            last_code = ""
            for idx, table in enumerate(tables, start=1):
                one_yaml = yaml.safe_dump({"tables": [table]}, allow_unicode=True)
                one_req = _build_yaml_requirement_text(one_yaml) + "\nSingle table only; output rows."
                file_name = f"{table.get('name','table_'+str(idx))}.csv"
                result = pipeline_agent.run(requirement=one_req, file_name=file_name)
                last_code = result.code
                out_path = result.tool_result.output_path if result.tool_result and result.tool_result.output_path else ""
                raw_map = result.tool_result.metadata.get("generated_files", {}) if result.tool_result and result.tool_result.metadata else {}
                part_map = _normalize_generated_files_map(out_path, raw_map if isinstance(raw_map, dict) else {})
                if not part_map:
                    raise RuntimeError(result.message or "Chunk generation failed")
                files_map.update(part_map)

            # If joins exist, generate join tables only in a second step.
            if joins:
                join_yaml = yaml.safe_dump({"tables": tables, "joins": joins}, allow_unicode=True)
                join_req = (
                    _build_yaml_requirement_text(join_yaml)
                    + "\nGenerate ONLY join tables based on the joins. Do NOT output base tables."
                    + "\nPreserve join key values exactly (no renumbering). If IDs are strings with prefixes (e.g., c000001), keep the same format."
                )
                join_result = pipeline_agent.run(requirement=join_req, file_name="joins.csv")
                last_code = join_result.code or last_code
                join_out = (
                    join_result.tool_result.output_path
                    if join_result.tool_result and join_result.tool_result.output_path
                    else ""
                )
                join_raw = (
                    join_result.tool_result.metadata.get("generated_files", {})
                    if join_result.tool_result and join_result.tool_result.metadata
                    else {}
                )
                join_map = _normalize_generated_files_map(join_out, join_raw if isinstance(join_raw, dict) else {})
                # Keep only join tables (name starts with join_)
                join_map = {k: v for k, v in join_map.items() if k.startswith("join_")}
                files_map.update(join_map)

            output_dir = Path("generated_tools_output")
            if len(files_map) == 1:
                output_path = next(iter(files_map.values()))
            else:
                output_path = _zip_files(files_map, output_dir, "datasynth_output.zip")
            duration_sec = round(time.time() - start_ts, 2)
            payload = {
                "status": "done",
                "message": "AI agents generated CSV successfully",
                "code": last_code,
                "output_path": output_path,
                "files_map": files_map,
                "duration_sec": duration_sec,
                "start_ts": start_ts,
            }
            with _JOBS_LOCK:
                _JOBS[job_id] = payload
            _save_job(job_id, payload)
            return

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


def _kaggle_cli_available() -> bool:
    return shutil.which("kaggle") is not None


def _kaggle_search(keywords: list[str]) -> dict[str, str]:
    if not keywords:
        return {}
    query = " ".join(keywords)
    cmd = f"kaggle datasets list -s {shlex.quote(query)} --csv"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        return {}
    rows = list(csv.DictReader(result.stdout.splitlines()))
    if not rows:
        return {}
    row = rows[0]
    ref = row.get("ref") or row.get("dataset") or row.get("datasetSlug") or ""
    title = row.get("title") or ""
    if not ref:
        return {}
    return {"ref": ref, "title": title}


def _kaggle_metadata(dataset_ref: str, out_dir: Path) -> dict[str, str]:
    try:
        cmd = f"kaggle datasets metadata -d {shlex.quote(dataset_ref)} -p {shlex.quote(str(out_dir))}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            return {}
        meta_path = out_dir / "dataset-metadata.json"
        if not meta_path.exists():
            return {}
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        return {
            "title": str(meta.get("title", "")).strip(),
            "subtitle": str(meta.get("subtitle", "")).strip(),
            "description": str(meta.get("description", "")).strip(),
        }
    except Exception:
        return {}


def _kaggle_download(dataset_ref: str) -> tuple[str, dict[str, str]]:
    out_dir = Path("generated_tools_output") / "kaggle" / _sanitize_slug(dataset_ref)
    out_dir.mkdir(parents=True, exist_ok=True)
    cache_marker = out_dir / ".cache_complete"
    if not cache_marker.exists():
        cmd = f"kaggle datasets download -d {shlex.quote(dataset_ref)} -p {shlex.quote(str(out_dir))} --unzip"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            # If files are already present, treat as success to avoid false negatives.
            has_files = any(p.is_file() for p in out_dir.iterdir())
            if not has_files:
                msg = result.stderr.strip() or result.stdout.strip() or "Kaggle download failed"
                raise RuntimeError(msg)
        cache_marker.write_text("ok", encoding="utf-8")
    _extract_kaggle_archives(out_dir)
    # If still no CSVs, try a fallback unzip.
    if not _collect_kaggle_csv_files(out_dir):
        _extract_kaggle_archives_fallback(out_dir)

    files_map = _collect_kaggle_csv_files(out_dir)
    if not files_map:
        files_map = _convert_kaggle_files_to_csv(out_dir)
    if not files_map:
        raise RuntimeError("未找到可用文件")

    if len(files_map) == 1:
        output_path = next(iter(files_map.values()))
    else:
        output_path = _zip_files(files_map, out_dir, "kaggle_dataset.zip")
    return output_path, files_map


def _try_kaggle_from_requirement(
    requirement_text: str,
    cfg: DeepagentConfig,
    start_ts: float,
    job_id: str,
) -> dict[str, object] | None:
    if not _kaggle_cli_available():
        _update_job_status_note(job_id, "未检测到 Kaggle CLI，自动回退 AI 生成")
        return None
    extractor = KeywordExtractAgent(cfg)
    kw_result = extractor.run(requirement=requirement_text)
    if not kw_result.success or not kw_result.keywords:
        _update_job_status_note(job_id, "未能从需求中提取 Kaggle 关键词，自动回退 AI 生成")
        return None
    match = _kaggle_search(kw_result.keywords)
    if not match:
        _update_job_status_note(job_id, "未在 Kaggle 找到相关数据集，AI 正在生成数据")
        return None
    out_dir = Path("generated_tools_output") / "kaggle" / _sanitize_slug(match["ref"])
    meta = _kaggle_metadata(match["ref"], out_dir)
    output_path, files_map = _kaggle_download(match["ref"])
    duration_sec = round(time.time() - start_ts, 2)
    return {
        "status": "done",
        "message": "Kaggle 数据集已下载",
        "code": "",
        "output_path": output_path,
        "files_map": files_map,
        "duration_sec": duration_sec,
        "start_ts": start_ts,
        "source_notice": "来源：Kaggle",
        "source_link": f"https://www.kaggle.com/datasets/{match['ref']}",
        "source_title": meta.get("title", "") or match.get("title", ""),
        "source_subtitle": meta.get("subtitle", ""),
        "source_description": meta.get("description", ""),
    }

def _run_kaggle_job(job_id: str, spec: dict[str, str]) -> None:
    _JOB_DIR.mkdir(parents=True, exist_ok=True)
    start_ts = time.time()
    with _JOBS_LOCK:
        _JOBS[job_id] = {"status": "running", "start_ts": start_ts}
    _save_job(job_id, {"status": "running", "start_ts": start_ts})

    dataset = spec["dataset"]
    file_name = spec.get("file", "")
    out_dir = Path("generated_tools_output") / "kaggle" / _sanitize_slug(dataset)
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        cache_marker = out_dir / ".cache_complete"
        if not cache_marker.exists():
            cmd = f"kaggle datasets download -d {shlex.quote(dataset)} -p {shlex.quote(str(out_dir))} --unzip"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            if result.returncode != 0:
                has_files = any(p.is_file() for p in out_dir.iterdir())
                if not has_files:
                    msg = result.stderr.strip() or result.stdout.strip() or "Kaggle download failed"
                    raise RuntimeError(msg)
            cache_marker.write_text("ok", encoding="utf-8")
        _extract_kaggle_archives(out_dir)
        if not _collect_kaggle_csv_files(out_dir):
            _extract_kaggle_archives_fallback(out_dir)

        files_map: dict[str, str] = {}
        if file_name:
            target = out_dir / file_name
            if not target.exists():
                raise RuntimeError(f"指定文件不存在: {file_name}")
            if target.suffix.lower() == ".csv":
                files_map[target.name] = str(target)
            else:
                files_map = _convert_kaggle_files_to_csv(out_dir)
        else:
            files_map = _collect_kaggle_csv_files(out_dir)
            if not files_map:
                files_map = _convert_kaggle_files_to_csv(out_dir)
            if not files_map:
                raise RuntimeError("未找到可用文件")

        meta = _kaggle_metadata(dataset, out_dir)
        if len(files_map) == 1:
            output_path = next(iter(files_map.values()))
        else:
            output_path = _zip_files(files_map, out_dir, "kaggle_dataset.zip")

        duration_sec = round(time.time() - start_ts, 2)
        payload = {
            "status": "done",
            "message": "Kaggle 数据集已下载",
            "code": "",
            "output_path": output_path,
            "files_map": files_map,
            "duration_sec": duration_sec,
            "start_ts": start_ts,
            "source_notice": "来源：Kaggle",
            "source_link": f"https://www.kaggle.com/datasets/{dataset}",
            "source_title": meta.get("title", "") or dataset,
            "source_subtitle": meta.get("subtitle", ""),
            "source_description": meta.get("description", ""),
        }
        with _JOBS_LOCK:
            _JOBS[job_id] = payload
        _save_job(job_id, payload)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        payload = {
            "status": "failed",
            "message": f"Kaggle 下载失败：{exc}",
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


def _update_job_status_note(job_id: str, note: str) -> None:
    payload = _load_job(job_id)
    if not isinstance(payload, dict):
        payload = {}
    payload["status_note"] = note
    _save_job(job_id, payload)


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
    kaggle_spec = {}
    if st.session_state.input_mode == "yaml":
        kaggle_spec = _parse_kaggle_spec(st.session_state.yaml_requirement)
    output_stem = "datasynth_output"
    with _JOBS_LOCK:
        _JOBS[job_id] = {"status": "queued", "start_ts": time.time()}
    _save_job(job_id, {"status": "queued", "start_ts": time.time()})
    if kaggle_spec:
        worker = threading.Thread(
            target=_run_kaggle_job,
            args=(job_id, kaggle_spec),
            daemon=True,
        )
    else:
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
    st.session_state.source_notice = ""
    st.session_state.source_link = ""
    st.session_state.source_title = ""
    st.session_state.source_subtitle = ""
    st.session_state.source_description = ""
    st.session_state.status_note = ""

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
        st.session_state.source_notice = str(job.get("source_notice", ""))
        st.session_state.source_link = str(job.get("source_link", ""))
        st.session_state.source_title = str(job.get("source_title", ""))
        st.session_state.source_subtitle = str(job.get("source_subtitle", ""))
        st.session_state.source_description = str(job.get("source_description", ""))
        st.session_state.status_note = str(job.get("status_note", ""))

if st.session_state.status_note:
    st.info(st.session_state.status_note)

if st.session_state.pipeline_message:
    if st.session_state.output_path:
        st.success(st.session_state.pipeline_message)
    else:
        st.error(st.session_state.pipeline_message)
    if st.session_state.generation_time_sec is not None:
        st.caption(f"AI 生成耗时：{st.session_state.generation_time_sec} 秒")
    if st.session_state.source_notice:
        st.caption(st.session_state.source_notice)
    if st.session_state.source_title or st.session_state.source_subtitle:
        title_line = st.session_state.source_title
        if st.session_state.source_subtitle:
            title_line = f"{title_line} — {st.session_state.source_subtitle}".strip(" —")
        if title_line:
            st.caption(title_line)
    if st.session_state.source_description:
        desc = st.session_state.source_description.strip()
        if desc:
            parts = desc.replace("\n", " ").split(". ")
            short_desc = ". ".join(parts[:2]).strip()
            if short_desc and not short_desc.endswith("."):
                short_desc += "."
            st.caption(short_desc)
    if st.session_state.source_link:
        st.code(st.session_state.source_link, language="text")

if st.session_state.generated_code:
    with st.expander("查看 Agent1 生成的 Python 代码", expanded=False):
        st.code(st.session_state.generated_code, language="python")

if st.session_state.generated_files_map:
    st.markdown("**表预览（基础表与 Join 表）**")
    st.caption(f"仅预览前 {PREVIEW_LINES} 行，完整数据请下载 ZIP。")
    if st.session_state.source_notice:
        desc = st.session_state.source_description.strip()
        if desc:
            parts = desc.replace("\n", " ").split(". ")
            short_desc = ". ".join(parts[:2]).strip()
            if short_desc and not short_desc.endswith("."):
                short_desc += "."
            st.caption(f"Kaggle 数据集简介：{short_desc}")
        elif st.session_state.source_title:
            st.caption(f"Kaggle 数据集：{st.session_state.source_title}")
        st.caption("数据集简介位置：")
    label_map = {}
    for fname in sorted(st.session_state.generated_files_map.keys()):
        label = _describe_table_name(fname)
        label_map[label] = fname
    selected_label = st.selectbox("选择预览表", options=list(label_map.keys()))
    selected_file = label_map[selected_label]
    selected_path = Path(st.session_state.generated_files_map[selected_file])
    if selected_path.exists():
        row_count, col_count = _csv_basic_stats(selected_path)
        if row_count is not None and col_count is not None:
            st.caption(f"行数：{row_count}，列数：{col_count}")
        cache_key = str(selected_path)
        if cache_key not in st.session_state.table_desc_cache:
            sample_lines = _read_text_safe(selected_path).strip().splitlines()[: min(PREVIEW_LINES, 20)]
            sample_text = "\n".join(sample_lines)
            if sample_text:
                desc = _ai_describe_table(sample_text, deepagent_cfg)
                if desc:
                    st.session_state.table_desc_cache[cache_key] = desc
        desc = st.session_state.table_desc_cache.get(cache_key, "")
        if desc:
            st.caption(f"表简介：{desc}")
        content = _read_text_safe(selected_path)
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
