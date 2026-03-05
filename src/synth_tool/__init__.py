"""Core package for synth-tool."""

from .models import ColumnSpec, JoinSpec, TableSpec, GenerationRequest
from .service import build_request_from_description, build_request_from_yaml, generate_csv_bundle

__all__ = [
    "ColumnSpec",
    "JoinSpec",
    "TableSpec",
    "GenerationRequest",
    "build_request_from_description",
    "build_request_from_yaml",
    "generate_csv_bundle",
]
