from __future__ import annotations

from src.cli.common import RegenerationConfig


class TestRegenerationConfig:
    def test_default_values(self) -> None:
        config = RegenerationConfig()
        assert config.game_ids is None
        assert config.dates is None
        assert config.seasons is None
        assert config.apply is False
        assert config.sync_oci is False
        assert config.oci_url is None
        assert config.report_out is None
        assert config.backup_out is None

    def test_custom_values(self) -> None:
        config = RegenerationConfig(
            game_ids=["20260101LGSS0"],
            dates=["2026-01-01"],
            seasons=[2026],
            apply=True,
            sync_oci=True,
            oci_url="postgresql://host/db",
            report_out=None,
            backup_out=None,
        )
        assert config.game_ids == ["20260101LGSS0"]
        assert config.apply is True
        assert config.sync_oci is True
        assert config.oci_url == "postgresql://host/db"

    def test_frozen(self) -> None:
        config = RegenerationConfig()
        import dataclasses

        assert dataclasses.is_dataclass(config)
        assert config.__dataclass_params__.frozen is True
