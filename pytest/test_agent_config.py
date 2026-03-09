import pytest

from synth_tool.agent_config import load_deepagent_config


def test_load_deepagent_config_requires_api_key(monkeypatch):
    monkeypatch.delenv("DEEPAGENT_API_KEY", raising=False)
    monkeypatch.delenv("DEEPAGENT_API_KEY_ENV", raising=False)

    with pytest.raises(ValueError):
        load_deepagent_config()


def test_load_deepagent_config_custom_env_name(monkeypatch):
    monkeypatch.setenv("DEEPAGENT_API_KEY_ENV", "CUSTOM_KEY")
    monkeypatch.setenv("CUSTOM_KEY", "abc123")
    monkeypatch.setenv("DEEPAGENT_MODEL", "my-model")

    cfg = load_deepagent_config()
    assert cfg.api_key_env == "CUSTOM_KEY"
    assert cfg.api_key == "abc123"
    assert cfg.model == "my-model"
