"""Data quality gate that runs *before* anything reaches Snowflake.

Policy (see docs/failure_modes.md):

* rows that violate the contract go to a quarantine file, they do not stop the
  batch - one bad hour should not block a whole day of history;
* if the share of rejected rows crosses `max_reject_ratio` the batch is treated
  as broken and the task fails, because that usually means the producer changed
  something rather than one sensor glitching;
* unknown fields are logged loudly - that is the early warning for a schema
  change that has not been negotiated yet.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from pipeline.contracts import Contract
from pipeline.logging_conf import get_logger

log = get_logger(__name__)

DEFAULT_MAX_REJECT_RATIO = 0.02


class DataQualityError(RuntimeError):
    """Raised when a batch fails the quality gate and must not be loaded."""


@dataclass(frozen=True)
class QualityReport:
    city_id: str
    observation_date: date
    total_rows: int
    accepted_rows: int
    rejected_rows: int
    unknown_fields: tuple[str, ...]
    quarantine_path: Path | None

    @property
    def reject_ratio(self) -> float:
        return self.rejected_rows / self.total_rows if self.total_rows else 0.0


def check_batch(
    contract: Contract,
    rows: list[dict[str, Any]],
    city_id: str,
    observation_date: date,
    quarantine_root: Path | str,
    max_reject_ratio: float = DEFAULT_MAX_REJECT_RATIO,
) -> tuple[list[dict[str, Any]], QualityReport]:
    accepted, rejected = contract.split_valid(rows)

    unknown: set[str] = set()
    for row in rows[:50]:  # sampling is enough to spot a new producer field
        unknown |= contract.unknown_fields(row)
    if unknown:
        log.warning(
            "quality.unknown_fields",
            city_id=city_id,
            fields=sorted(unknown),
            action=contract.unknown_field_action,
        )

    quarantine_path = None
    if rejected:
        quarantine_path = _write_quarantine(
            quarantine_root, contract, city_id, observation_date, rejected
        )

    report = QualityReport(
        city_id=city_id,
        observation_date=observation_date,
        total_rows=len(rows),
        accepted_rows=len(accepted),
        rejected_rows=len(rejected),
        unknown_fields=tuple(sorted(unknown)),
        quarantine_path=quarantine_path,
    )

    if report.reject_ratio > max_reject_ratio:
        raise DataQualityError(
            f"{city_id} {observation_date}: {report.rejected_rows}/{report.total_rows} rows "
            f"({report.reject_ratio:.1%}) violate contract "
            f"{contract.name} v{contract.version}, above the {max_reject_ratio:.1%} threshold"
        )

    log.info(
        "quality.checked",
        city_id=city_id,
        observation_date=observation_date.isoformat(),
        accepted=report.accepted_rows,
        rejected=report.rejected_rows,
    )
    return accepted, report


def _write_quarantine(
    root: Path | str,
    contract: Contract,
    city_id: str,
    observation_date: date,
    rejected: list[tuple[dict[str, Any], list[str]]],
) -> Path:
    """Quarantine is newline-delimited JSON: schemaless on purpose.

    The rows are rejected precisely because they do not fit the schema, so
    storing them as Parquet would need a second, looser schema to maintain.
    """
    directory = Path(root) / contract.name / f"v{contract.version}" / observation_date.isoformat()
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{city_id}.jsonl"

    with path.open("w", encoding="utf-8") as handle:
        for row, reasons in rejected:
            handle.write(
                json.dumps(
                    {
                        "_quarantined_at_utc": datetime.now(tz=UTC).isoformat(),
                        "_contract": f"{contract.name}.v{contract.version}",
                        "_reasons": reasons,
                        "row": row,
                    },
                    default=str,
                )
                + "\n"
            )

    log.warning("quality.quarantined", path=str(path), rows=len(rejected))
    return path
