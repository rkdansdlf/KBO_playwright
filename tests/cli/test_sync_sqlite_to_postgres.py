from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime
from datetime import time as dtime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    JSON,
    LargeBinary,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
    Time,
    create_engine,
)

from src.cli.sync.sync_sqlite_to_postgres import (
    SyncOptions,
    TableTransferRequest,
    _dry_run_table,
    _is_resume_safe,
    _order_metas,
    _sqlite_path,
    _wrap_pg_params,
    convert_value,
    main,
    parse_args,
    plan_action,
    run_sync,
    sync_table,
    verify_counts,
)
from src.sync.table_dag import TableMeta


def _probe_table(metadata: MetaData) -> Table:
    return Table(
        "probe_table",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("flag", Boolean, nullable=True),
        Column("created", DateTime, nullable=True),
        Column("birthday", Date, nullable=True),
        Column("alarm", Time, nullable=True),
        Column("payload", JSON, nullable=True),
        Column("score", Float, nullable=True),
        Column("amount", Numeric, nullable=True),
        Column("name", String(50), nullable=True),
        Column("blob", LargeBinary, nullable=True),
        Column("note", Text, nullable=True),
        Column("big", BigInteger, nullable=True),
    )


def test_parse_args_defaults():
    args = parse_args([])
    assert args.apply is False
    assert args.verify is False
    assert args.batch_size == 5000
    assert args.level is None


def test_sqlite_path_rejects_non_sqlite():
    try:
        _sqlite_path("postgresql://host/db")
    except ValueError:
        pass
    else:  # pragma: no cover
        raise AssertionError("expected ValueError")


def test_plan_action_matrix():
    assert plan_action(10, 10) == "SKIP"
    assert plan_action(0, 0) == "SKIP"
    assert plan_action(10, 0) == "LOAD"
    assert plan_action(10, 4) == "RELOAD"


def test_convert_value_branches():
    assert convert_value(None, Boolean()) is None
    assert convert_value(1, Boolean()) is True
    assert convert_value(0, Boolean()) is False
    assert convert_value("true", Boolean()) is True
    assert convert_value("2024-01-02 03:04:05", DateTime()) == datetime(2024, 1, 2, 3, 4, 5)
    assert convert_value("2024-01-02", Date()) == date(2024, 1, 2)
    assert convert_value("03:04:05", Time()) == dtime(3, 4, 5)
    assert convert_value('{"a": 1}', JSON()) == {"a": 1}
    assert convert_value("", JSON()) is None
    assert convert_value("12", Integer()) == 12
    assert convert_value(12.0, Integer()) == 12
    assert convert_value("12.5", Numeric()) == Decimal("12.5")
    assert convert_value("3.5", Float()) == 3.5
    assert convert_value(b"abc", String(10)) == "abc"
    assert convert_value("abc", LargeBinary()) == b"abc"
    assert convert_value("x", Text()) == "x"


def test_sync_table_sqlite_to_sqlite(tmp_path):
    source_path = tmp_path / "source.db"
    target_path = tmp_path / "target.db"
    metadata = MetaData()
    probe = _probe_table(metadata)
    source_engine = create_engine(f"sqlite:///{source_path.as_posix()}")
    target_engine = create_engine(f"sqlite:///{target_path.as_posix()}")
    try:
        metadata.create_all(source_engine)
        metadata.create_all(target_engine)
        with sqlite3.connect(source_path) as connection:
            connection.execute(
                "INSERT INTO probe_table (id, flag, created, birthday, alarm, payload, score, amount, name, blob, note, big)"
                " VALUES (1, 1, '2024-01-02 03:04:05', '2000-05-06', '07:08:09', '{\"a\": 1}', '3.5', 12.5, '한화', X'4142', 'note-1', 99)",
            )
            connection.execute(
                "INSERT INTO probe_table (id, flag, created, payload, name) VALUES (2, 0, '2024-06-07 08:09:10', '[1, 2]', '두산')",
            )
            connection.commit()
        first = sync_table(TableTransferRequest(str(source_path), target_engine, probe, 0, 500, apply=True))
        assert first.action == "LOAD"
        assert first.target_after == 2
        with target_engine.connect() as connection:
            rows = connection.exec_driver_sql(
                "SELECT id, flag, created, payload, name FROM probe_table ORDER BY id"
            ).fetchall()
        assert rows[0][1] in (1, True)
        assert "2024-01-02" in str(rows[0][2])
        assert rows[0][4] == "한화"
        second = sync_table(TableTransferRequest(str(source_path), target_engine, probe, 0, 500, apply=True))
        assert second.action == "SKIP"
    finally:
        source_engine.dispose()
        target_engine.dispose()


def test_sync_table_uppercase_source_columns(tmp_path):
    source_path = tmp_path / "source.db"
    target_path = tmp_path / "target.db"
    with sqlite3.connect(source_path) as connection:
        connection.execute("CREATE TABLE probe_upper (ID INTEGER PRIMARY KEY, FLAG INTEGER, NAME TEXT)")
        connection.execute("INSERT INTO probe_upper (ID, FLAG, NAME) VALUES (1, 1, 'LG'), (2, 0, '두산')")
        connection.commit()
    metadata = MetaData()
    target_table = Table(
        "probe_upper",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("flag", Boolean, nullable=True),
        Column("name", String(50), nullable=True),
    )
    target_engine = create_engine(f"sqlite:///{target_path.as_posix()}")
    try:
        metadata.create_all(target_engine)
        result = sync_table(TableTransferRequest(str(source_path), target_engine, target_table, 3, 500, apply=True))
        assert result.action == "LOAD"
        assert result.target_after == 2
        with target_engine.connect() as connection:
            names = connection.exec_driver_sql("SELECT name FROM probe_upper ORDER BY id").fetchall()
        assert [row[0] for row in names] == ["LG", "두산"]
    finally:
        target_engine.dispose()


def test_order_metas_parents_first():
    metadata = MetaData()
    parent = Table("z_parent", metadata, Column("id", Integer, primary_key=True))
    child = Table(
        "a_child",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("pid", Integer, ForeignKey("z_parent.id")),
    )
    metas = [TableMeta("a_child", level=3), TableMeta("z_parent", level=3)]
    ordered = _order_metas(metas, {"a_child": child, "z_parent": parent})
    assert [meta.name for meta in ordered] == ["z_parent", "a_child"]


def test_wrap_pg_params_array_passthrough():
    from sqlalchemy.dialects.postgresql import ARRAY as PG_ARRAY

    metadata = MetaData()
    table = Table(
        "probe_array",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("aliases", JSON().with_variant(PG_ARRAY(String), "postgresql"), nullable=True),
        Column("payload", JSON, nullable=True),
    )
    rows, names = _wrap_pg_params(
        [{"id": 1, "aliases": '["OB", "DB"]', "payload": '{"a": 1}'}],
        table,
        [("id", "id"), ("aliases", "aliases"), ("payload", "payload")],
    )
    assert names == ["id", "aliases", "payload"]
    assert rows[0][0] == 1
    assert rows[0][1] == ["OB", "DB"]
    assert type(rows[0][1]) is list
    from psycopg2.extras import Json

    assert isinstance(rows[0][2], Json)


def test_is_resume_safe():
    metadata = MetaData()
    assert _is_resume_safe(_probe_table(metadata)) is True
    heap = Table(
        "heap_table",
        metadata,
        Column("a", Integer, nullable=True),
        Column("b", Text, nullable=True),
    )
    assert _is_resume_safe(heap) is False


def test_run_sync_dry_run_empty_databases(tmp_path):
    source_path = tmp_path / "source.db"
    target_path = tmp_path / "target.db"
    sqlite3.connect(source_path).close()
    sqlite3.connect(target_path).close()
    source_url = f"sqlite:///{source_path.as_posix()}"
    target_url = f"sqlite:///{target_path.as_posix()}"
    report = run_sync(source_url, target_url, SyncOptions())
    assert report.tables_total == 87
    assert report.tables_failed == 0
    assert report.rows_synced == 0


def test_main_json_emit_empty_databases(tmp_path, capsys):
    source_path = tmp_path / "source.db"
    target_path = tmp_path / "target.db"
    sqlite3.connect(source_path).close()
    sqlite3.connect(target_path).close()
    source_url = f"sqlite:///{source_path.as_posix()}"
    target_url = f"sqlite:///{target_path.as_posix()}"
    code = main(["--source-url", source_url, "--target-url", target_url, "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["tables_total"] == 87


def test_dry_run_skip_places_message_field(tmp_path):
    source_path = tmp_path / "source.db"
    target_path = tmp_path / "target.db"
    with sqlite3.connect(source_path) as connection:
        connection.execute("CREATE TABLE game (id INTEGER PRIMARY KEY)")
        connection.commit()
    sqlite3.connect(target_path).close()
    target_engine = create_engine(f"sqlite:///{target_path.as_posix()}")
    try:
        missing_source = _dry_run_table(str(source_path), target_engine, "no_such_table", 1)
        assert missing_source.action == "SKIP"
        assert missing_source.deduped == 0
        assert missing_source.message == "missing in source"

        missing_target = _dry_run_table(str(source_path), target_engine, "game", 1)
        assert missing_target.action == "SKIP"
        assert missing_target.deduped == 0
        assert missing_target.message == "missing in target schema"
    finally:
        target_engine.dispose()


def test_verify_counts_places_message_field(tmp_path):
    source_path = tmp_path / "source.db"
    target_path = tmp_path / "target.db"
    with sqlite3.connect(source_path) as connection:
        connection.execute("CREATE TABLE game (id INTEGER PRIMARY KEY)")
        connection.execute("INSERT INTO game (id) VALUES (1)")
        connection.commit()
    sqlite3.connect(target_path).close()
    source_url = f"sqlite:///{source_path.as_posix()}"
    target_url = f"sqlite:///{target_path.as_posix()}"
    report = verify_counts(source_url, target_url, ["game"])
    assert report.tables_total == 1
    assert report.tables_failed == 1
    item = report.results[0]
    assert item.action == "FAILED"
    assert item.deduped == 0
    assert item.message == "mismatch: source=1 target=0"
