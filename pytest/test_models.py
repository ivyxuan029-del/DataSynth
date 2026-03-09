from synth_tool.models import GenerationRequest


def test_default_single_table_shape():
    req = GenerationRequest.default_single_table("demo")
    assert req.description == "demo"
    assert req.tables[0].name == "fact_sales"
    assert req.tables[0].primary_key == "id"
