"""Alert routing for the weather DAGs.

Two channels on purpose (see docs/runbook.md):

* **page** - the pipeline is broken and a human has to act tonight. Sent to the
  on-call webhook.
* **notify** - something needs a data owner tomorrow morning: a quality test
  warned, a day is incomplete. Sent to the team channel only.

The distinction is the whole point. An alert that fires for things nobody will
act on at 03:00 trains people to ignore the channel, and then the real one is
missed too.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any

PAGE_WEBHOOK_ENV = "ALERT_PAGE_WEBHOOK_URL"
NOTIFY_WEBHOOK_ENV = "ALERT_NOTIFY_WEBHOOK_URL"
RUNBOOK_URL = "https://github.com/JanIzmer/batch-api-snowflake/blob/main/docs/runbook.md"


def _post(webhook_env: str, payload: dict[str, Any]) -> None:
    url = os.environ.get(webhook_env)
    if not url:
        # Local and CI runs have no webhook. Printing keeps the DAG usable
        # without pretending an alert was delivered.
        print(f"[alerting:{webhook_env} not set] {json.dumps(payload)}")
        return

    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            response.read()
    except (urllib.error.URLError, TimeoutError) as exc:
        # Never let a broken webhook turn a warning into a task failure.
        print(f"[alerting] failed to deliver alert: {exc}")


def _context_fields(context: dict[str, Any]) -> dict[str, Any]:
    task_instance = context.get("task_instance")
    return {
        "dag_id": context.get("dag").dag_id if context.get("dag") else "unknown",
        "task_id": getattr(task_instance, "task_id", "unknown"),
        "run_id": context.get("run_id"),
        "logical_date": str(context.get("logical_date") or context.get("execution_date")),
        "try_number": getattr(task_instance, "try_number", None),
        "log_url": getattr(task_instance, "log_url", None),
    }


def page_on_failure(context: dict[str, Any]) -> None:
    """Task-level failure callback. Only fires after the last retry."""
    fields = _context_fields(context)
    exception = context.get("exception")
    _post(
        PAGE_WEBHOOK_ENV,
        {
            "severity": "page",
            "title": f"[weather] {fields['dag_id']}.{fields['task_id']} failed",
            "error": str(exception)[:1000] if exception else "unknown",
            "runbook": RUNBOOK_URL,
            **fields,
        },
    )


def notify_on_retry(context: dict[str, Any]) -> None:
    """Retries are normal; they are worth seeing but never worth waking anyone."""
    fields = _context_fields(context)
    _post(
        NOTIFY_WEBHOOK_ENV,
        {
            "severity": "info",
            "title": f"[weather] retrying {fields['dag_id']}.{fields['task_id']}",
            **fields,
        },
    )


def notify_on_sla_miss(dag: Any, task_list: str, blocking_task_list: str, *args: Any) -> None:
    """SLA miss means the data will be late for the 07:00 dashboard refresh."""
    _post(
        NOTIFY_WEBHOOK_ENV,
        {
            "severity": "warning",
            "title": f"[weather] SLA missed in {getattr(dag, 'dag_id', 'unknown')}",
            "late_tasks": task_list,
            "blocking_tasks": blocking_task_list,
            "runbook": RUNBOOK_URL,
        },
    )


def page_on_dag_failure(context: dict[str, Any]) -> None:
    """DAG-level callback: one message per failed run, not one per failed task."""
    fields = _context_fields(context)
    _post(
        PAGE_WEBHOOK_ENV,
        {
            "severity": "page",
            "title": f"[weather] dag run failed: {fields['dag_id']}",
            "runbook": RUNBOOK_URL,
            **fields,
        },
    )
