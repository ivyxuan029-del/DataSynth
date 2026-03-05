from __future__ import annotations

from dataclasses import dataclass, field


DEFAULT_TOTAL_ROWS = 100_000


@dataclass
class ColumnSpec:
    name: str
    dtype: str = "string"
    distinct_values: int | None = None
    nullable: bool = True
    trend_rule: str | None = None


@dataclass
class TableSpec:
    name: str
    rows: int
    columns: list[ColumnSpec] = field(default_factory=list)
    primary_key: str | None = None


@dataclass
class JoinSpec:
    left_table: str
    right_table: str
    left_key: str
    right_key: str
    join_type: str = "inner"


@dataclass
class GenerationRequest:
    description: str | None = None
    tables: list[TableSpec] = field(default_factory=list)
    joins: list[JoinSpec] = field(default_factory=list)

    @staticmethod
    def default_single_table(description: str | None = None) -> "GenerationRequest":
        cols = [
            ColumnSpec(name="id", dtype="int", distinct_values=DEFAULT_TOTAL_ROWS, nullable=False),
            ColumnSpec(name="event_date", dtype="date"),
            ColumnSpec(name="amount", dtype="double", trend_rule="2025>2023"),
        ]
        table = TableSpec(name="fact_sales", rows=DEFAULT_TOTAL_ROWS, columns=cols, primary_key="id")
        return GenerationRequest(description=description, tables=[table], joins=[])
