"""Core package for synth-tool."""

from .models import ColumnSpec, JoinSpec, TableSpec, GenerationRequest
from .service import (
    build_request_from_description,
    build_request_from_yaml,
    generate_csv_bundle,
    generate_dataframe_bundle,
)
from .tools import ExecutePythonToCsvTool, ToolRegistry, ToolRequest, ToolResult
from .agent_config import DeepagentConfig, OPENAI_API_URL, OPENAI_MODEL, load_deepagent_config
from .agents import (
    AgentPipelineResult,
    CodeGenResult,
    CsvGenerationPipelineAgent,
    PythonCodeGenAgent,
    PythonExecutionAgent,
)

__all__ = [
    "ColumnSpec",
    "JoinSpec",
    "TableSpec",
    "GenerationRequest",
    "build_request_from_description",
    "build_request_from_yaml",
    "generate_csv_bundle",
    "generate_dataframe_bundle",
    "ToolRequest",
    "ToolResult",
    "ToolRegistry",
    "ExecutePythonToCsvTool",
    "DeepagentConfig",
    "OPENAI_API_URL",
    "OPENAI_MODEL",
    "load_deepagent_config",
    "CodeGenResult",
    "AgentPipelineResult",
    "PythonCodeGenAgent",
    "PythonExecutionAgent",
    "CsvGenerationPipelineAgent",
]
