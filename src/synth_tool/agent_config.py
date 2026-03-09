from __future__ import annotations

import os
from dataclasses import dataclass

OPENAI_API_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions"
OPENAI_MODEL = "qwen3-next-80b-a3b-instruct"


@dataclass
class DeepagentConfig:
    provider: str
    model: str
    api_key_env: str
    api_key: str
    base_url: str | None = None


def load_deepagent_config() -> DeepagentConfig:
    api_key_env = os.getenv("DEEPAGENT_API_KEY_ENV", "DEEPAGENT_API_KEY")
    api_key = os.getenv(api_key_env, "").strip()
    if not api_key:
        raise ValueError(f"Missing API key: set environment variable `{api_key_env}`")

    return DeepagentConfig(
        provider=os.getenv("DEEPAGENT_PROVIDER", "deepagent"),
        model=os.getenv("DEEPAGENT_MODEL", OPENAI_MODEL),
        api_key_env=api_key_env,
        api_key=api_key,
        base_url=os.getenv("DEEPAGENT_BASE_URL", OPENAI_API_URL),
    )
