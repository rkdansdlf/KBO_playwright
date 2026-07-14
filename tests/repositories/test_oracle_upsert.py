from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.models.team_stats import TeamSeasonPitching
from src.repositories.oracle_upsert import upsert_model_by_unique_keys


def test_upsert_adds_when_business_key_is_missing() -> None:
    session = MagicMock()
    session.execute.return_value.scalars.return_value.first.return_value = None

    upsert_model_by_unique_keys(
        session,
        TeamSeasonPitching,
        {"team_id": "LG", "season": 2025, "league": "REGULAR", "era": 3.5},
        ("team_id", "season", "league"),
    )

    session.add.assert_called_once()
    assert session.add.call_args.args[0].era == 3.5


def test_upsert_updates_existing_row_without_replacing_identity() -> None:
    session = MagicMock()
    existing = SimpleNamespace(team_id="LG", season=2025, league="REGULAR", era=1.0, id=10, created_at="created")
    session.execute.return_value.scalars.return_value.first.return_value = existing

    upsert_model_by_unique_keys(
        session,
        TeamSeasonPitching,
        {"team_id": "LG", "season": 2025, "league": "REGULAR", "era": 2.0, "id": 20},
        ("team_id", "season", "league"),
    )

    assert existing.era == 2.0
    assert existing.id == 10
    assert existing.created_at == "created"
    session.add.assert_not_called()


def test_upsert_requires_all_business_keys() -> None:
    with pytest.raises(ValueError, match="Missing unique-key values: season"):
        upsert_model_by_unique_keys(MagicMock(), TeamSeasonPitching, {"team_id": "LG"}, ("team_id", "season"))
