from __future__ import annotations

import csv
import io
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol
import zipfile


@dataclass
class ToolRequest:
    tool_name: str
    inputs: dict[str, Any] = field(default_factory=dict)


@dataclass
class ToolResult:
    success: bool
    message: str
    output_path: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class Tool(Protocol):
    name: str

    def run(self, request: ToolRequest) -> ToolResult:
        ...


class ToolRegistry:
    def __init__(self) -> None:
        self._tools: dict[str, Tool] = {}

    def register(self, tool: Tool) -> None:
        self._tools[tool.name] = tool

    def execute(self, request: ToolRequest) -> ToolResult:
        tool = self._tools.get(request.tool_name)
        if tool is None:
            return ToolResult(success=False, message=f"Tool not found: {request.tool_name}")
        return tool.run(request)


class ExecutePythonToCsvTool:
    """
    Common-interface tool:
    input: {"code": "<python>", "file_name": "x.csv"}
    output: write CSV file and return its path.
    """

    name = "execute_python_to_csv"

    def __init__(self, output_dir: str = "generated_tools_output") -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._allowed_import_roots = {
            "random",
            "time",
            "datetime",
            "math",
            "string",
            "uuid",
            "itertools",
            "statistics",
            "decimal",
            "collections",
            "csv",
            "io",
            "json",
            "re",
        }

    def _safe_import(self, name: str, globals_=None, locals_=None, fromlist=(), level: int = 0):
        root = name.split(".")[0]
        if root not in self._allowed_import_roots:
            raise ImportError(f"import not allowed: {name}")
        return __import__(name, globals_, locals_, fromlist, level)

    def _write_rows_csv(self, output_path: Path, rows: list[dict]) -> tuple[int, list[str]]:
        fieldnames = list(rows[0].keys()) if rows else []
        with output_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if fieldnames:
                writer.writeheader()
                writer.writerows(rows)
        return len(rows), fieldnames

    def run(self, request: ToolRequest) -> ToolResult:
        code = str(request.inputs.get("code", "")).strip()
        file_name = str(request.inputs.get("file_name", "")).strip()

        if not code:
            return ToolResult(success=False, message="Missing input: code")
        if not file_name:
            return ToolResult(success=False, message="Missing input: file_name")

        safe_name = Path(file_name).name
        if not safe_name.endswith(".csv"):
            safe_name = f"{safe_name}.csv"
        output_path = self.output_dir / safe_name

        # Keep execution surface small; allow common builtins for data construction.
        exec_namespace: dict[str, Any] = {
            "__builtins__": {
                "__import__": self._safe_import,
                "abs": abs,
                "all": all,
                "any": any,
                "bool": bool,
                "chr": chr,
                "dict": dict,
                "enumerate": enumerate,
                "float": float,
                "int": int,
                "len": len,
                "list": list,
                "max": max,
                "min": min,
                "next": next,
                "range": range,
                "round": round,
                "set": set,
                "str": str,
                "sum": sum,
                "zip": zip,
            }
        }

        try:
            exec(code, exec_namespace, exec_namespace)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            return ToolResult(success=False, message=f"Python execution failed: {exc}")

        if "tables" in exec_namespace:
            tables = exec_namespace["tables"]
            if isinstance(tables, dict):
                table_files: dict[str, str] = {}
                try:
                    for table_name, rows in tables.items():
                        if not isinstance(table_name, str):
                            continue
                        if not isinstance(rows, list) or (rows and not isinstance(rows[0], dict)):
                            continue
                        safe_table_name = Path(table_name).name
                        table_csv_path = self.output_dir / f"{safe_table_name}.csv"
                        self._write_rows_csv(table_csv_path, rows)
                        table_files[f"{safe_table_name}.csv"] = str(table_csv_path)

                    if not table_files:
                        return ToolResult(success=False, message="`tables` is present but no valid table rows were found.")

                    if len(table_files) == 1:
                        only_name = next(iter(table_files.keys()))
                        return ToolResult(
                            success=True,
                            message="CSV generated from variable `tables` (single table).",
                            output_path=table_files[only_name],
                            metadata={"generated_files": table_files},
                        )

                    zip_base = Path(safe_name).stem
                    zip_path = self.output_dir / f"{zip_base}.zip"
                    zip_buffer = io.BytesIO()
                    with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
                        for filename, full_path in table_files.items():
                            zf.writestr(filename, Path(full_path).read_text(encoding="utf-8"))
                    zip_path.write_bytes(zip_buffer.getvalue())
                    return ToolResult(
                        success=True,
                        message="Multiple CSV files generated from variable `tables`.",
                        output_path=str(zip_path),
                        metadata={"generated_files": table_files, "bundle_type": "zip"},
                    )
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    return ToolResult(success=False, message=f"Failed writing tables to CSV bundle: {exc}")

        if "df" in exec_namespace and hasattr(exec_namespace["df"], "to_csv"):
            try:
                exec_namespace["df"].to_csv(output_path, index=False)
                return ToolResult(
                    success=True,
                    message="CSV generated from variable `df`.",
                    output_path=str(output_path),
                    metadata={"generated_files": {output_path.name: str(output_path)}},
                )
            except Exception as exc:  # pylint: disable=broad-exception-caught
                return ToolResult(success=False, message=f"Failed writing df to CSV: {exc}")

        if "rows" in exec_namespace:
            rows = exec_namespace["rows"]
            if isinstance(rows, list) and (not rows or isinstance(rows[0], dict)):
                try:
                    row_count, fieldnames = self._write_rows_csv(output_path, rows)
                    return ToolResult(
                        success=True,
                        message="CSV generated from variable `rows`.",
                        output_path=str(output_path),
                        metadata={
                            "row_count": row_count,
                            "columns": fieldnames,
                            "generated_files": {output_path.name: str(output_path)},
                        },
                    )
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    return ToolResult(success=False, message=f"Failed writing rows to CSV: {exc}")

        if "csv_content" in exec_namespace and isinstance(exec_namespace["csv_content"], str):
            try:
                output_path.write_text(exec_namespace["csv_content"], encoding="utf-8")
                return ToolResult(
                    success=True,
                    message="CSV generated from variable `csv_content`.",
                    output_path=str(output_path),
                    metadata={"generated_files": {output_path.name: str(output_path)}},
                )
            except Exception as exc:  # pylint: disable=broad-exception-caught
                return ToolResult(success=False, message=f"Failed writing csv_content to CSV: {exc}")

        return ToolResult(
            success=False,
            message="Code must define `tables` (dict[str, list[dict]]), or `df`, or `rows`, or `csv_content`.",
        )
