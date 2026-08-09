from __future__ import annotations

from datetime import date, datetime, time

from sqlalchemy import JSON, Boolean, Column, Date, DateTime, Integer, MetaData, Table, Time

from scripts.migrate_sqlite_to_postgres import _coerce_value, _table_payload


def test_coerce_json_and_temporal_values():
    assert _coerce_value('{"ok": true}', JSON()) == {"ok": True}
    assert _coerce_value("2026-08-09T12:30:00", DateTime()) == datetime(2026, 8, 9, 12, 30)
    assert _coerce_value("2026-08-09", Date()) == date(2026, 8, 9)
    assert _coerce_value("12:30:00", Time()) == time(12, 30)


def test_coerce_boolean_values():
    assert _coerce_value(1, Boolean()) is True
    assert _coerce_value("false", Boolean()) is False
    assert _coerce_value(0, Boolean()) is False


def test_table_payload_uses_target_columns_and_types():
    metadata = MetaData()
    source = Table("source", metadata, Column("id", Integer), Column("payload", JSON))
    target = Table("target", metadata, Column("id", Integer), Column("payload", JSON), Column("missing", Integer))
    row = {"id": 7, "payload": '{"source": "sqlite"}'}

    assert _table_payload(source, type("Row", (), {"_mapping": row})(), target) == {
        "id": 7,
        "payload": {"source": "sqlite"},
    }
