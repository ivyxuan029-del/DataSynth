from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any
from urllib import error, request

from .agent_config import DeepagentConfig
from .tools import ExecutePythonToCsvTool, ToolRequest, ToolResult


@dataclass
class CodeGenResult:
    success: bool
    message: str
    code: str = ""


@dataclass
class AgentPipelineResult:
    success: bool
    message: str
    code: str = ""
    tool_result: ToolResult | None = None


def _extract_code_block(text: str) -> str:
    match = re.search(r"```(?:python)?\s*(.*?)```", text, flags=re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return text.strip()


class PythonCodeGenAgent:
    """
    Agent 1: requirement -> python code
    """

    def __init__(self, cfg: DeepagentConfig):
        self.cfg = cfg

    def _chat_completions_url(self) -> str:
        if self.cfg.base_url:
            base = self.cfg.base_url.rstrip("/")
            if base.endswith("/chat/completions"):
                return base
            return f"{base}/chat/completions"
        # OpenAI-compatible default endpoint.
        return "https://api.openai.com/v1/chat/completions"

    def _build_prompt(self, requirement: str, file_name: str) -> list[dict[str, str]]:
        system_prompt = (
            "You generate Python code for realistic synthetic tabular data.\n"
            "Return only Python code. No markdown.\n"
            "Code must produce variable `rows` as list[dict].\n"
            "Use varied and realistic values, avoid trivial repeated values like 1,2,3,4,5 for all fields.\n"
            "IDs can be sequential, but names, categories, amounts, dates should be diverse.\n"
            "Use only Python stdlib.\n"
            "Do NOT use file I/O: no `open`, no writing files, no reading files.\n"
            "Do NOT use network/system calls.\n"
            "Do NOT print CSV text; only construct `rows`.\n"
        )
        user_prompt = (
            f"Requirement: {requirement}\n"
            f"Target CSV file name: {file_name}\n"
            "Generate around 100 rows unless requirement specifies row count.\n"
        )
        return [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}]

    def run(self, requirement: str, file_name: str) -> CodeGenResult:
        messages = self._build_prompt(requirement=requirement, file_name=file_name)
        payload = {"model": self.cfg.model, "messages": messages, "temperature": 0.3}
        data = json.dumps(payload).encode("utf-8")

        req = request.Request(
            self._chat_completions_url(),
            data=data,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.cfg.api_key}",
            },
        )

        try:
            with request.urlopen(req, timeout=60) as resp:
                body = json.loads(resp.read().decode("utf-8"))
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore")
            return CodeGenResult(success=False, message=f"CodeGen API HTTP error: {exc.code}, {detail}")
        except Exception as exc:  # pylint: disable=broad-exception-caught
            return CodeGenResult(success=False, message=f"CodeGen API request failed: {exc}")

        try:
            content = body["choices"][0]["message"]["content"]
        except Exception:  # pylint: disable=broad-exception-caught
            return CodeGenResult(success=False, message=f"Unexpected CodeGen API response: {body}")

        code = _extract_code_block(content)
        if not code:
            return CodeGenResult(success=False, message="CodeGen API returned empty code")
        return CodeGenResult(success=True, message="Code generated", code=code)


class PythonExecutionAgent:
    """
    Agent 2: python code -> csv file (via tool)
    """

    def __init__(self, tool: ExecutePythonToCsvTool):
        self.tool = tool

    def run(self, code: str, file_name: str) -> ToolResult:
        return self.tool.run(
            ToolRequest(
                tool_name="execute_python_to_csv",
                inputs={"code": code, "file_name": file_name},
            )
        )


class CsvGenerationPipelineAgent:
    """
    Orchestrator: requirement -> codegen agent -> execution agent
    """

    def __init__(self, codegen_agent: PythonCodeGenAgent, execution_agent: PythonExecutionAgent):
        self.codegen_agent = codegen_agent
        self.execution_agent = execution_agent

    def run(self, requirement: str, file_name: str) -> AgentPipelineResult:
        requirement = requirement.strip()
        if not requirement:
            return AgentPipelineResult(success=False, message="Missing requirement text")
        if not file_name.strip():
            return AgentPipelineResult(success=False, message="Missing file_name")

        codegen = self.codegen_agent.run(requirement=requirement, file_name=file_name)
        if not codegen.success:
            return AgentPipelineResult(success=False, message=codegen.message)

        exec_result = self.execution_agent.run(code=codegen.code, file_name=file_name)
        if not exec_result.success:
            return AgentPipelineResult(
                success=False,
                message=exec_result.message,
                code=codegen.code,
                tool_result=exec_result,
            )

        return AgentPipelineResult(
            success=True,
            message="AI agents generated CSV successfully",
            code=codegen.code,
            tool_result=exec_result,
        )
