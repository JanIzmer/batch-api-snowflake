from __future__ import annotations

from datetime import date

from pipeline.landing import (
    cleanup_temp_files,
    clear_partition,
    existing_payload_hash,
    partition_dir,
    read_landed_file,
    write_batch,
)
from pipeline.models import flatten_hourly
from tests.conftest import hourly_payload


def land(root, berlin, day, payload_hash="hash-a", hours=24):
    observations = flatten_hourly(berlin, hourly_payload(hours=hours))
    return write_batch(root, berlin.city_id, day, observations, payload_hash)


def test_write_batch_lands_one_file_per_partition(tmp_path, berlin, day):
    batch = land(tmp_path, berlin, day)

    assert batch.row_count == 24
    assert batch.path.exists()
    assert batch.path.parent == partition_dir(tmp_path, day, "berlin")
    assert list(batch.path.parent.glob("*.parquet")) == [batch.path]


def test_rerunning_a_day_overwrites_instead_of_appending(tmp_path, berlin, day):
    """The whole point of a deterministic filename: a retry cannot duplicate."""
    first = land(tmp_path, berlin, day)
    second = land(tmp_path, berlin, day)

    assert first.path == second.path
    assert len(list(first.path.parent.glob("*.parquet"))) == 1
    assert read_landed_file(second.path).num_rows == 24


def test_partitions_are_isolated_per_city_and_day(tmp_path, berlin, day):
    land(tmp_path, berlin, day)
    land(tmp_path, berlin, date(2026, 8, 21))

    assert partition_dir(tmp_path, day, "berlin").exists()
    assert partition_dir(tmp_path, date(2026, 8, 21), "berlin").exists()


def test_metadata_columns_are_attached(tmp_path, berlin, day):
    batch = land(tmp_path, berlin, day, payload_hash="hash-xyz")

    table = read_landed_file(batch.path)
    assert set(table.column_names) >= {
        "_ingested_at_utc",
        "_batch_id",
        "_source_payload_hash",
        "_contract_version",
    }
    assert table.column("_source_payload_hash")[0].as_py() == "hash-xyz"
    assert table.column("_batch_id")[0].as_py() == batch.batch_id


def test_existing_payload_hash_round_trips(tmp_path, berlin, day):
    assert existing_payload_hash(tmp_path, "berlin", day) is None

    land(tmp_path, berlin, day, payload_hash="hash-a")

    assert existing_payload_hash(tmp_path, "berlin", day) == "hash-a"


def test_clear_partition_removes_only_that_partition(tmp_path, berlin, day):
    land(tmp_path, berlin, day)
    land(tmp_path, berlin, date(2026, 8, 21))

    clear_partition(tmp_path, day, "berlin")

    assert not partition_dir(tmp_path, day, "berlin").exists()
    assert partition_dir(tmp_path, date(2026, 8, 21), "berlin").exists()


def test_cleanup_removes_leftovers_from_killed_tasks(tmp_path, berlin, day):
    batch = land(tmp_path, berlin, day)
    (batch.path.parent / ".abandoned.parquet.tmp").write_bytes(b"partial")

    removed = cleanup_temp_files(tmp_path)

    assert removed == 1
    assert batch.path.exists()


def test_the_tree_can_still_be_scanned_as_a_dataset(tmp_path, berlin, day):
    """The partition keys live in the path AND in the file, on purpose.

    The path is what makes one (city, day) a directory that can be dropped and
    rebuilt; the columns are what Snowflake's COPY reads. A dataset scan has to
    be told not to infer partitioning from the directories, or it tries to
    merge a dictionary-typed `city_id` from the path with the string column of
    the same name inside the file.
    """
    import pyarrow.parquet as pq

    land(tmp_path, berlin, day)
    land(tmp_path, berlin, date(2026, 8, 21))

    table = pq.read_table(tmp_path / "weather_observation" / "v1", partitioning=None)

    assert table.num_rows == 48
    assert set(table.column("city_id").to_pylist()) == {"berlin"}
    assert "observation_date" not in table.column_names  # it is in the path only
