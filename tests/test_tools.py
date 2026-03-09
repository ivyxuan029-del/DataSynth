from synth_tool.tools import ExecutePythonToCsvTool, ToolRegistry, ToolRequest


def test_execute_python_to_csv_from_rows(tmp_path):
    tool = ExecutePythonToCsvTool(output_dir=str(tmp_path))
    request = ToolRequest(
        tool_name="execute_python_to_csv",
        inputs={
            "code": "rows = [{'id': 1, 'name': 'a'}, {'id': 2, 'name': 'b'}]",
            "file_name": "demo.csv",
        },
    )

    result = tool.run(request)
    assert result.success is True
    assert result.output_path is not None

    content = (tmp_path / "demo.csv").read_text(encoding="utf-8").strip().splitlines()
    assert content[0] == "id,name"
    assert content[1] == "1,a"
    assert content[2] == "2,b"


def test_execute_python_to_csv_from_csv_content(tmp_path):
    tool = ExecutePythonToCsvTool(output_dir=str(tmp_path))
    request = ToolRequest(
        tool_name="execute_python_to_csv",
        inputs={
            "code": "csv_content = 'id,value\\n1,10\\n2,20\\n'",
            "file_name": "metrics",
        },
    )

    result = tool.run(request)
    assert result.success is True
    assert (tmp_path / "metrics.csv").exists()


def test_tool_registry_dispatch(tmp_path):
    registry = ToolRegistry()
    registry.register(ExecutePythonToCsvTool(output_dir=str(tmp_path)))

    result = registry.execute(
        ToolRequest(
            tool_name="execute_python_to_csv",
            inputs={"code": "rows = [{'x': 1}]", "file_name": "x.csv"},
        )
    )
    assert result.success is True
