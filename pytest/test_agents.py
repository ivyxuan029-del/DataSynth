from __future__ import annotations

import json

from synth_tool.agent_config import DeepagentConfig
from synth_tool.agents import CsvGenerationPipelineAgent, PythonCodeGenAgent, PythonExecutionAgent
from synth_tool.tools import ExecutePythonToCsvTool


class _FakeResponse:
    def __init__(self, payload: dict):
        self._payload = payload

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_codegen_agent_extracts_python_code(monkeypatch):
    cfg = DeepagentConfig(
        provider="deepagent",
        model="demo-model",
        api_key_env="DEEPAGENT_API_KEY",
        api_key="dummy-key",
        base_url="https://example.com/v1",
    )
    agent = PythonCodeGenAgent(cfg)

    def fake_urlopen(req, timeout=60):  # noqa: ARG001
        payload = {
            "choices": [
                {
                    "message": {
                        "content": "```python\nrows = [{'id': 1, 'name': 'alice'}]\n```",
                    }
                }
            ]
        }
        return _FakeResponse(payload)

    monkeypatch.setattr("synth_tool.agents.request.urlopen", fake_urlopen)

    result = agent.run("生成用户数据100行", "users.csv")
    assert result.success is True
    assert "rows =" in result.code


def test_pipeline_agent_runs_codegen_and_execution(monkeypatch, tmp_path):
    cfg = DeepagentConfig(
        provider="deepagent",
        model="demo-model",
        api_key_env="DEEPAGENT_API_KEY",
        api_key="dummy-key",
        base_url="https://example.com/v1",
    )
    codegen_agent = PythonCodeGenAgent(cfg)

    def fake_urlopen(req, timeout=60):  # noqa: ARG001
        payload = {
            "choices": [
                {
                    "message": {
                        "content": "rows = [{'id': 1, 'city': 'Shanghai'}, {'id': 2, 'city': 'Shenzhen'}]",
                    }
                }
            ]
        }
        return _FakeResponse(payload)

    monkeypatch.setattr("synth_tool.agents.request.urlopen", fake_urlopen)

    execution_agent = PythonExecutionAgent(ExecutePythonToCsvTool(output_dir=str(tmp_path)))
    pipeline = CsvGenerationPipelineAgent(codegen_agent=codegen_agent, execution_agent=execution_agent)

    result = pipeline.run("生成城市样例数据", "cities.csv")
    assert result.success is True
    assert result.tool_result is not None
    assert result.tool_result.output_path is not None
    assert (tmp_path / "cities.csv").exists()
