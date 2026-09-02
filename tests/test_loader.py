from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from pipeline.landing import LandedBatch
from pipeline.warehouse.connection import split_statements
from pipeline.warehouse.loader import already_loaded, load_batch


class FakeCursor:
    def __init__(self, connection: "FakeConnection") -> None:
        self._connection = connection
        self._result: list[tuple] = []

    def execute(self, statement: str, params=None):
        self._connection.statements.append((" ".join(statement.split()), params))
        if statement.lstrip().upper().startswith("MERGE"):
            self._result = [self._connection.merge_result]
        elif "FROM OPS.LOAD_AUDIT" in statement:
            self._result = [(1,)] if self._connection.audit_hit else []
        else:
            self._result = []
        return self

    def fetchone(self):
        return self._result[0] if self._result else None

    def fetchall(self):
        return self._result

    def close(self):
        return None


class FakeConnection:
    def __init__(self, merge_result=(24, 0), audit_hit=False) -> None:
        self.statements: list[tuple[str, object]] = []
        self.merge_result = merge_result
        self.audit_hit = audit_hit
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        return None

    def sql_of_kind(self, keyword: str) -> list[str]:
        return [sql for sql, _ in self.statements if sql.upper().startswith(keyword)]


@pytest.fixture
def batch(tmp_path) -> LandedBatch:
    path = Path(tmp_path) / "abc123.parquet"
    path.write_bytes(b"parquet")
    return LandedBatch(
        path=path,
        batch_id="abc123",
        city_id="berlin",
        observation_date=date(2026, 8, 20),
        row_count=24,
        payload_hash="hash-a",
    )


def run_load(connection, batch, **kwargs):
    return load_batch(
        connection=connection,
        batch=batch,
        target="WEATHER.RAW.WEATHER_OBSERVATION",
        stage="WEATHER.RAW.STG_WEATHER_OBSERVATION",
        run_id="run-1",
        rows_fetched=kwargs.get("rows_fetched", 24),
        rows_rejected=kwargs.get("rows_rejected", 0),
    )


def test_load_puts_merges_and_cleans_up(batch):
    connection = FakeConnection(merge_result=(24, 0))

    result = run_load(connection, batch)

    assert result.rows_inserted == 24
    assert result.status == "loaded"
    assert connection.sql_of_kind("PUT")
    assert connection.sql_of_kind("MERGE")
    assert connection.sql_of_kind("REMOVE")


def test_put_overwrites_so_a_retry_cannot_stage_twice(batch):
    connection = FakeConnection()

    run_load(connection, batch)

    put = connection.sql_of_kind("PUT")[0]
    assert "OVERWRITE = TRUE" in put
    assert "2026-08-20/berlin" in put


def test_merge_matches_on_the_contract_primary_key(batch):
    connection = FakeConnection()

    run_load(connection, batch)

    merge = connection.sql_of_kind("MERGE")[0]
    assert "tgt.CITY_ID = src.city_id" in merge
    assert "tgt.OBSERVED_AT_UTC = src.observed_at_utc" in merge
    # An unchanged payload must not rewrite rows - that is what keeps a retry free.
    assert "tgt._SOURCE_PAYLOAD_HASH <> src._source_payload_hash" in merge


def test_second_load_of_unchanged_data_updates_nothing(batch):
    connection = FakeConnection(merge_result=(0, 0))

    result = run_load(connection, batch)

    assert (result.rows_inserted, result.rows_updated) == (0, 0)
    assert result.status == "loaded"


def test_every_load_writes_an_audit_row(batch):
    connection = FakeConnection()

    run_load(connection, batch)

    inserts = [sql for sql in connection.sql_of_kind("INSERT") if "OPS.LOAD_AUDIT" in sql]
    assert len(inserts) == 1


def test_failed_load_rolls_back_and_still_audits(batch, monkeypatch):
    connection = FakeConnection()
    original = FakeCursor.execute

    def explode(self, statement, params=None):
        if statement.lstrip().upper().startswith("MERGE"):
            raise RuntimeError("stage file not found")
        return original(self, statement, params)

    monkeypatch.setattr(FakeCursor, "execute", explode)

    with pytest.raises(RuntimeError, match="stage file not found"):
        run_load(connection, batch)

    assert connection.rollbacks == 1
    audits = [sql for sql in connection.sql_of_kind("INSERT") if "OPS.LOAD_AUDIT" in sql]
    assert len(audits) == 1


def test_already_loaded_uses_the_payload_hash(batch):
    connection = FakeConnection(audit_hit=True)

    assert already_loaded(connection, "berlin", date(2026, 8, 20), "hash-a") is True
    sql, params = connection.statements[-1]
    assert "STATUS = 'loaded'" in sql
    assert params == ("berlin", date(2026, 8, 20), "hash-a")


def test_already_loaded_is_false_for_a_new_payload():
    connection = FakeConnection(audit_hit=False)

    assert already_loaded(connection, "berlin", date(2026, 8, 20), "hash-b") is False


def test_split_statements_drops_comments_and_blanks():
    script = """
    -- a comment
    CREATE TABLE a (id INT);

    -- another
    CREATE TABLE b (id INT);
    """

    statements = split_statements(script)

    assert len(statements) == 2
    assert all(s.startswith("CREATE TABLE") for s in statements)
