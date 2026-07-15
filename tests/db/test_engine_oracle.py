from unittest.mock import MagicMock, patch

from src.db.engine import normalize_oracle_url


def test_normalize_oracle_url_encodes_special_password_characters() -> None:
    url = "oracle+oracledb://user:p%40ss%3Aword@db.example/service"

    assert normalize_oracle_url(url) == "oracle+oracledb://user:p%40ss%3Aword@db.example/service"


def test_normalize_oracle_url_preserves_non_oracle_urls() -> None:
    url = "postgresql://user:p%40ss@db.example/service"

    assert normalize_oracle_url(url) == url


def test_create_oracle_engine_uses_normalized_url_and_wallet_args(monkeypatch) -> None:
    from src.db import engine

    fake_engine = MagicMock()
    fake_engine.dialect = MagicMock()
    monkeypatch.setenv("TNS_ADMIN", "/wallet")

    with patch.object(engine, "create_engine", return_value=fake_engine) as create:
        engine.create_engine_for_url("oracle+oracledb://user:p%40ss@db/service")

    create.assert_called_once_with(
        "oracle+oracledb://user:p%40ss@db/service",
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20,
        echo=False,
        connect_args={
            "config_dir": "/wallet",
            "wallet_location": "/wallet",
            "wallet_password": "p@ss",
        },
    )
