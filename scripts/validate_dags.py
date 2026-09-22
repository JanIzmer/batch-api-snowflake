#!/usr/bin/env python
"""Validate the DAGs without a scheduler, a database or Snowflake.

Three levels, in increasing order of what they catch:

1. **Import.** An ImportError in a DAG file fails nothing at runtime - the DAG
   just silently stops existing in the scheduler, and the first symptom is
   missing data days later.
2. **Graph.** The task ids and the dependency edges are asserted, so a
   refactor that accidentally detaches a task from the chain is caught here
   rather than by a run that quietly skips a step.
3. **Templates.** Every Bash command and every env value is rendered against a
   realistic context. This is the level that matters most, because Airflow only
   renders templates at *execution* - a bad filter in a task at the end of a
   backfill fails after every other task has already done its work.

Run locally with:  python scripts/validate_dags.py airflow/dags
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

EXPECTED_DAGS = {
    "weather_batch_daily": {
        "start",
        "target_date",
        "preflight",
        "ingest.ingest_city",
        "transform.dbt_deps",
        "transform.dbt_seed",
        "transform.dbt_source_freshness",
        "transform.dbt_run",
        "transform.dbt_test",
        "report",
        "finish",
    },
    "weather_backfill": {
        "plan",
        "ingest_unit",
        "restatement_window_days",
        "rebuild_marts",
        "verify",
    },
}

# Edges that carry a correctness guarantee, so they are asserted explicitly.
EXPECTED_EDGES = [
    # Freshness runs before the transform, or we spend warehouse credits
    # turning yesterday's data into today's marts.
    ("weather_batch_daily", "transform.dbt_source_freshness", "transform.dbt_run"),
    ("weather_batch_daily", "transform.dbt_run", "transform.dbt_test"),
    # One dbt build at the end of a backfill, after every unit has landed.
    ("weather_backfill", "ingest_unit", "rebuild_marts"),
    ("weather_backfill", "restatement_window_days", "rebuild_marts"),
]


class _FakeVars(dict):
    """Stands in for Airflow Variables, which exist only in a real deployment."""

    def __getattr__(self, name: str) -> str:
        return f"<{name}>"

    def get(self, key: str, default: Any = None) -> Any:
        return f"<{key}>" if default is None else default


class _FakeTi:
    def xcom_pull(self, task_ids: Any = None, **kwargs: Any) -> int:
        return 7


def render_context() -> dict[str, Any]:
    return {
        "params": {
            "start_date": "2026-07-01",
            "end_date": "2026-07-31",
            "cities": [],
            "full_refresh": False,
        },
        "run_id": "scheduled__2026-09-10T05:00:00+00:00",
        "ds": "2026-09-10",
        "ti": _FakeTi(),
        "var": type("V", (), {"value": _FakeVars(), "json": _FakeVars()})(),
    }


def main(dag_folder: str = "airflow/dags") -> int:
    from airflow.models import DagBag
    from airflow.operators.bash import BashOperator

    bag = DagBag(dag_folder, include_examples=False)
    problems: list[str] = []

    for path, error in bag.import_errors.items():
        problems.append(f"import error in {path}:\n{error}")

    for dag_id, expected_tasks in EXPECTED_DAGS.items():
        dag = bag.dags.get(dag_id)
        if dag is None:
            problems.append(f"DAG '{dag_id}' is missing; found {sorted(bag.dag_ids)}")
            continue
        missing = expected_tasks - set(dag.task_ids)
        if missing:
            problems.append(f"{dag_id}: missing tasks {sorted(missing)}")

    for dag_id, upstream, downstream in EXPECTED_EDGES:
        dag = bag.dags.get(dag_id)
        if dag is None:
            continue
        try:
            task = dag.get_task(upstream)
        except Exception:
            problems.append(f"{dag_id}: no task '{upstream}'")
            continue
        if downstream not in task.downstream_task_ids:
            problems.append(f"{dag_id}: '{upstream}' -> '{downstream}' edge is missing")

    context = render_context()
    rendered = 0
    for dag_id, dag in sorted(bag.dags.items()):
        env = dag.get_template_env()
        for task in dag.tasks:
            if not isinstance(task, BashOperator):
                continue
            targets = [("bash_command", task.bash_command)]
            targets += [(f"env[{k}]", v) for k, v in (task.env or {}).items()]
            for label, text in targets:
                if not isinstance(text, str):
                    continue
                try:
                    env.from_string(text).render(**context)
                    rendered += 1
                except Exception as exc:
                    problems.append(
                        f"{dag_id}.{task.task_id} {label} failed to render: "
                        f"{type(exc).__name__}: {exc}"
                    )

    if problems:
        print("DAG validation FAILED:\n")
        for problem in problems:
            print(f"  - {problem}")
        return 1

    print(f"DAGs OK: {sorted(bag.dag_ids)}")
    print(f"  tasks checked     : {sum(len(d.tasks) for d in bag.dags.values())}")
    print(f"  templates rendered: {rendered}")
    return 0


if __name__ == "__main__":
    folder = sys.argv[1] if len(sys.argv) > 1 else "airflow/dags"
    if not Path(folder).is_dir():
        print(f"no such folder: {folder}")
        raise SystemExit(2)
    raise SystemExit(main(folder))
