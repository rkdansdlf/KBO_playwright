from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from sqlalchemy import Column, ForeignKeyConstraint, Integer, MetaData, Table
from sqlalchemy.dialects import oracle
from sqlalchemy.schema import CreateTable

from src.db.engine import normalize_oracle_url


def test_normalize_oracle_url_encodes_special_password_characters() -> None:
    url = "oracle+oracledb://user:p%40ss%3Aword@db.example/service"

    assert normalize_oracle_url(url) == "oracle+oracledb://user:p%40ss%3Aword@db.example/service"


def test_normalize_oracle_url_preserves_non_oracle_urls() -> None:
    url = "postgresql://user:p%40ss@db.example/service"

    assert normalize_oracle_url(url) == url


def test_normalize_oracle_url_canonicalizes_encoded_password() -> None:
    url = "oracle+oracledb://user:p%2fss%3aword@db.example/service"

    assert normalize_oracle_url(url) == "oracle+oracledb://user:p%2Fss%3Aword@db.example/service"


def test_normalize_oracle_url_returns_malformed_url_unchanged() -> None:
    url = "oracle+oracledb://malformed"

    assert normalize_oracle_url(url) == url


def test_create_oracle_engine_uses_normalized_url_and_wallet_args(monkeypatch) -> None:
    from src.db import engine

    fake_engine = MagicMock()
    fake_engine.dialect = SimpleNamespace()
    monkeypatch.setenv("TNS_ADMIN", "/wallet")
    url = "oracle+oracledb://user:p%40ss+word@db/service"

    with patch.object(engine, "create_engine", return_value=fake_engine) as create:
        result = engine.create_engine_for_url(url)

    assert result is fake_engine
    create.assert_called_once_with(
        "oracle+oracledb://user:p%40ss%2Bword@db/service",
        pool_pre_ping=True,
        pool_size=2,
        max_overflow=2,
        echo=False,
        connect_args={
            "config_dir": "/wallet",
            "wallet_location": "/wallet",
            "wallet_password": "p@ss+word",
        },
    )
    assert fake_engine.dialect._json_deserializer is None


def test_install_oracle_json_compiler_patches_missing_visit_json(monkeypatch) -> None:
    from sqlalchemy.dialects.oracle.base import OracleTypeCompiler

    from src.db import engine

    monkeypatch.delattr(OracleTypeCompiler, "visit_JSON", raising=False)

    engine._install_oracle_json_compiler()

    assert OracleTypeCompiler.visit_JSON(object(), object()) == "CLOB"


def test_oracle_fk_restrict_is_omitted_and_constraint_is_restored() -> None:
    from src.db import engine

    metadata = MetaData()
    Table("parents", metadata, Column("id", Integer, primary_key=True))
    child = Table("children", metadata, Column("parent_id", Integer))
    constraint = ForeignKeyConstraint(
        ["parent_id"],
        ["parents.id"],
        ondelete="RESTRICT",
    )
    child.append_constraint(constraint)

    ddl = str(CreateTable(child).compile(dialect=oracle.dialect()))

    assert "ON DELETE RESTRICT" not in ddl.upper()
    assert constraint.ondelete == "RESTRICT"
    engine._install_oracle_fk_restrict_compiler()


def test_oracle_fk_cascade_is_preserved() -> None:
    metadata = MetaData()
    Table("parents", metadata, Column("id", Integer, primary_key=True))
    child = Table("children", metadata, Column("parent_id", Integer))
    child.append_constraint(
        ForeignKeyConstraint(["parent_id"], ["parents.id"], ondelete="CASCADE"),
    )

    ddl = str(CreateTable(child).compile(dialect=oracle.dialect()))

    assert "ON DELETE CASCADE" in ddl.upper()


def test_oracle_fk_compiler_installation_is_idempotent() -> None:
    from sqlalchemy.dialects.oracle.base import OracleDDLCompiler

    from src.db import engine

    patched = OracleDDLCompiler.visit_foreign_key_constraint

    engine._install_oracle_fk_restrict_compiler()

    assert OracleDDLCompiler.visit_foreign_key_constraint is patched
