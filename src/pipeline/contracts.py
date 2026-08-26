"""Loading and enforcing data contracts.

The YAML files in `contracts/` are the single source of truth. This module
turns one into an object that can validate a batch of parsed rows and report
*why* a row was rejected, which is what the quarantine table stores.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

_PY_TYPES: dict[str, tuple[type, ...]] = {
    "string": (str,),
    "integer": (int,),
    "float": (float, int),  # an int is an acceptable float
    "boolean": (bool,),
    "timestamp": (datetime,),
}


class ContractError(RuntimeError):
    """Raised when a contract file itself is malformed or missing."""


@dataclass(frozen=True)
class FieldSpec:
    name: str
    type: str
    required: bool = False
    min: float | None = None
    max: float | None = None
    description: str | None = None

    def violations(self, value: Any) -> list[str]:
        if value is None:
            return [f"{self.name}: required field is null"] if self.required else []

        expected = _PY_TYPES.get(self.type)
        if expected is None:
            raise ContractError(f"unknown type '{self.type}' for field '{self.name}'")
        if isinstance(value, bool) and self.type != "boolean":
            return [f"{self.name}: expected {self.type}, got boolean"]
        if not isinstance(value, expected):
            return [f"{self.name}: expected {self.type}, got {type(value).__name__}"]

        problems: list[str] = []
        if self.min is not None and float(value) < self.min:
            problems.append(f"{self.name}: {value} below contract minimum {self.min}")
        if self.max is not None and float(value) > self.max:
            problems.append(f"{self.name}: {value} above contract maximum {self.max}")
        return problems


@dataclass(frozen=True)
class Contract:
    name: str
    version: int
    grain: str
    primary_key: tuple[str, ...]
    partition_key: str
    source_lag_days: int
    fields: tuple[FieldSpec, ...]
    unknown_field_action: str = "warn_and_keep"
    raw: dict[str, Any] = field(default_factory=dict, repr=False)

    @property
    def field_names(self) -> set[str]:
        return {f.name for f in self.fields}

    def validate_row(self, row: dict[str, Any]) -> list[str]:
        """Return a list of human readable violations; empty means valid."""
        problems: list[str] = []
        for spec in self.fields:
            problems.extend(spec.violations(row.get(spec.name)))

        unknown = set(row) - self.field_names - {"_", ""}
        unknown = {k for k in unknown if not k.startswith("_")}
        if unknown and self.unknown_field_action == "reject":
            problems.append(f"unknown fields not allowed by contract: {sorted(unknown)}")
        return problems

    def split_valid(
        self, rows: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], list[tuple[dict[str, Any], list[str]]]]:
        """Partition rows into (accepted, [(rejected_row, reasons), ...])."""
        accepted: list[dict[str, Any]] = []
        rejected: list[tuple[dict[str, Any], list[str]]] = []
        for row in rows:
            problems = self.validate_row(row)
            if problems:
                rejected.append((row, problems))
            else:
                accepted.append(row)
        return accepted, rejected

    def unknown_fields(self, row: dict[str, Any]) -> set[str]:
        """Fields the producer sent that the contract does not describe."""
        return {k for k in row if not k.startswith("_")} - self.field_names


def load_contract(name: str, version: int, contracts_dir: Path | str = "contracts") -> Contract:
    path = Path(contracts_dir) / f"{name}.v{version}.yml"
    if not path.exists():
        raise ContractError(f"contract file not found: {path}")

    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    try:
        return Contract(
            name=doc["name"],
            version=int(doc["version"]),
            grain=doc["grain"],
            primary_key=tuple(doc["primary_key"]),
            partition_key=doc["partition_key"],
            source_lag_days=int(doc["sla"]["source_lag_days"]),
            fields=tuple(FieldSpec(**f) for f in doc["fields"]),
            unknown_field_action=doc.get("compatibility", {}).get(
                "unknown_field_action", "warn_and_keep"
            ),
            raw=doc,
        )
    except (KeyError, TypeError) as exc:  # pragma: no cover - config error path
        raise ContractError(f"malformed contract {path}: {exc}") from exc
