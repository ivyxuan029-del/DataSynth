# DataSynth

A Streamlit framework for defining multi-table synthetic data requirements from either natural-language descriptions or semantic YAML.

## Two-Agent Flow

The app uses two agents end-to-end:

- Agent 1 (`PythonCodeGenAgent`): natural-language requirement -> Python code (`rows = list[dict]`)
- Agent 2 (`PythonExecutionAgent`): execute code via `ExecutePythonToCsvTool` -> CSV file

You only provide requirement text + file name in the UI.

## Tool Interface

The project includes a common tool interface and a concrete execution tool:

- `ToolRequest`: `{"tool_name": str, "inputs": dict}`
- `ToolResult`: `{"success": bool, "message": str, "output_path": str | None, "metadata": dict}`
- `ExecutePythonToCsvTool` (`tool_name="execute_python_to_csv"`)

Input for `execute_python_to_csv`:

- `code`: Python code string
- `file_name`: target CSV file name

The code should define one of:

- `df`: object that has `to_csv(path, index=False)`
- `rows`: `list[dict]`
- `csv_content`: CSV string

Then the tool executes the code and writes a CSV file, returning `output_path`.

## Deepagent Key From Environment

This app treats Deepagent as a separate AI provider and requires key from environment variables.

- Default key variable: `DEEPAGENT_API_KEY`
- Optional override for key variable name: `DEEPAGENT_API_KEY_ENV`
- Optional provider/model/base URL:
  - `DEEPAGENT_PROVIDER`
  - `DEEPAGENT_MODEL`
  - `DEEPAGENT_BASE_URL`

Example:

```bash
export DEEPAGENT_API_KEY="your-key"
export DEEPAGENT_PROVIDER="deepagent"
export DEEPAGENT_MODEL="deepagent-default"
streamlit run app.py
```

Example:

```python
from synth_tool.tools import ExecutePythonToCsvTool, ToolRequest

tool = ExecutePythonToCsvTool(output_dir="generated_tools_output")
result = tool.run(
    ToolRequest(
        tool_name="execute_python_to_csv",
        inputs={
            "code": "rows = [{'id': 1, 'name': 'alice'}, {'id': 2, 'name': 'bob'}]",
            "file_name": "users.csv",
        },
    )
)
print(result.success, result.output_path)
```

## Run app

```bash
streamlit run app.py
```

## Run tests

```bash
pytest -q
```
