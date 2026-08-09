from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from scripts.maintenance.link_award_player_ids import AwardLinkReport, _summary_line, link_award_player_ids
from src.models.award import Award


@pytest.fixture
def session() -> MagicMock:
    return MagicMock()


def _award(
    *,
    year: int,
    award_type: str = "MVP",
    player_name: str = "홍길동",
    team_name: str = "두산",
    player_id: int | None = None,
    team_code: str | None = None,
) -> MagicMock:
    return MagicMock(
        year=year,
        award_type=award_type,
        player_name=player_name,
        team_name=team_name,
        player_id=player_id,
        team_code=team_code,
    )


def test_link_applies_id_on_resolved_row() -> None:
    session = MagicMock()
    award = _award(year=2024)
    session.scalars.return_value.all.return_value = [award]
    resolver = MagicMock()
    resolver.resolve_id.return_value = 42

    with patch("scripts.maintenance.link_award_player_ids.PlayerIdResolver", return_value=resolver):
        with patch("scripts.maintenance.link_award_player_ids.get_team_code", return_value="DB"):
            report = link_award_player_ids(session, apply=True)

    assert award.player_id == 42
    assert award.team_code == "DB"
    session.commit.assert_called_once()
    assert report == AwardLinkReport(total=1, resolved=1, applied=True)


def test_dry_run_does_not_mutate_or_commit() -> None:
    session = MagicMock()
    award = _award(year=2024)
    session.scalars.return_value.all.return_value = [award]
    resolver = MagicMock()
    resolver.resolve_id.return_value = 42

    with patch("scripts.maintenance.link_award_player_ids.PlayerIdResolver", return_value=resolver):
        with patch("scripts.maintenance.link_award_player_ids.get_team_code", return_value="DB"):
            report = link_award_player_ids(session, apply=False)

    assert award.player_id is None
    assert award.team_code is None
    session.commit.assert_not_called()
    assert report.resolved == 1
    assert not report.applied


def test_unresolved_team_counts_and_skips_resolve() -> None:
    session = MagicMock()
    award = _award(year=1985, team_name="고양이")
    session.scalars.return_value.all.return_value = [award]
    resolver = MagicMock()

    with patch("scripts.maintenance.link_award_player_ids.PlayerIdResolver", return_value=resolver):
        with patch("scripts.maintenance.link_award_player_ids.get_team_code", return_value=None):
            report = link_award_player_ids(session)

    resolver.resolve_id.assert_not_called()
    assert report.unresolved_team == 1
    assert report.resolved == 0


def test_unresolved_player_counts_separately() -> None:
    session = MagicMock()
    award = _award(year=2024)
    session.scalars.return_value.all.return_value = [award]
    resolver = MagicMock()
    resolver.resolve_id.return_value = None

    with patch("scripts.maintenance.link_award_player_ids.PlayerIdResolver", return_value=resolver):
        with patch("scripts.maintenance.link_award_player_ids.get_team_code", return_value="DB"):
            report = link_award_player_ids(session)

    assert report.unresolved_player == 1
    assert report.resolved == 0
    assert award.player_id is None


def test_year_and_type_filters_are_applied() -> None:
    engine = create_engine("sqlite://")
    Award.__table__.create(engine)
    session = sessionmaker(bind=engine)()
    session.add(Award(year=2024, award_type="MVP", player_name="가", team_name="두산"))
    session.add(Award(year=2024, award_type="골든글러브", player_name="나", team_name="두산"))
    session.add(Award(year=2023, award_type="MVP", player_name="다", team_name="삼성"))
    session.commit()
    resolver = MagicMock()
    resolver.resolve_id.return_value = 42

    with patch("scripts.maintenance.link_award_player_ids.PlayerIdResolver", return_value=resolver):
        with patch("scripts.maintenance.link_award_player_ids.get_team_code", return_value="DB"):
            report = link_award_player_ids(session, year=2024, award_type="골든글러브")

    assert report.total == 1
    assert report.resolved == 1
    session.close()
    engine.dispose()


def test_summary_line_renders_dry_run() -> None:
    report = AwardLinkReport(total=5, resolved=3, unresolved_team=1, unresolved_player=1)
    assert "dry-run" in _summary_line(report)
    assert "resolved=3" in _summary_line(report)


def test_summary_line_renders_applied() -> None:
    report = AwardLinkReport(total=1, resolved=1, applied=True)
    assert "applied" in _summary_line(report)


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
