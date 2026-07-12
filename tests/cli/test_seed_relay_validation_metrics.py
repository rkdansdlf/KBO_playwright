from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.cli.seed_relay_validation_metrics import (
    VALIDATION_SOURCE_INCOMPLETE,
    VALIDATION_SOURCE_UNAVAILABLE,
    VALIDATION_UNVERIFIED,
    VALIDATION_VERIFIED,
    main,
    seed_relay_validation_metrics,
)


def _query(*, rows=None, one=None):
    query = MagicMock()
    query.filter.return_value = query
    query.order_by.return_value = query
    query.distinct.return_value = query
    query.group_by.return_value = query
    query.all.return_value = rows or []
    query.one_or_none.return_value = one
    return query


class TestSeedRelayValidationMetrics:
    def test_default_run(self):
        with patch("src.cli.seed_relay_validation_metrics.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
            result = main([])
            assert result == 0

    def test_with_season(self):
        with patch("src.cli.seed_relay_validation_metrics.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
            result = main(["--season", "2025"])
            assert result == 0

    def test_no_mark_legacy(self):
        with patch("src.cli.seed_relay_validation_metrics.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
            result = main(["--no-mark-legacy-unavailable"])
            assert result == 0

    def test_seeds_each_relay_validation_state_and_preserves_previous_status(self):
        games = [
            SimpleNamespace(game_id="20090001"),
            SimpleNamespace(game_id="20250001"),
            SimpleNamespace(game_id="20250002"),
            SimpleNamespace(game_id="20250003"),
        ]
        events = [
            SimpleNamespace(game_id="20250002", has_wpa_state=False),
            SimpleNamespace(game_id="20250003", has_wpa_state=True),
        ]
        existing_metrics = SimpleNamespace(
            validation_status=VALIDATION_UNVERIFIED,
            source_used=None,
            last_successful_event_at=None,
        )
        session = MagicMock()
        session.query.side_effect = [
            _query(rows=games),
            _query(rows=[("20250002",), ("20250003",)]),
            _query(rows=events),
            _query(rows=[("20250002", 2), ("20250003", 4)]),
            _query(one=None),
            _query(one=None),
            _query(one=None),
            _query(one=existing_metrics),
        ]

        with (
            patch("src.cli.seed_relay_validation_metrics.SessionLocal") as session_local,
            patch(
                "src.cli.seed_relay_validation_metrics.event_has_wpa_state",
                side_effect=lambda event: event.has_wpa_state,
            ),
        ):
            session_local.return_value.__enter__.return_value = session
            counts = seed_relay_validation_metrics(season=2025)

        assert counts == {
            VALIDATION_SOURCE_UNAVAILABLE: 1,
            VALIDATION_UNVERIFIED: 1,
            VALIDATION_SOURCE_INCOMPLETE: 1,
            VALIDATION_VERIFIED: 1,
        }
        assert existing_metrics.previous_status == VALIDATION_UNVERIFIED
        assert existing_metrics.validation_status == VALIDATION_VERIFIED
        assert existing_metrics.last_successful_event_at is not None
        session.commit.assert_called_once()

    def test_can_leave_legacy_games_unverified(self):
        session = MagicMock()
        session.query.side_effect = [
            _query(rows=[SimpleNamespace(game_id="20090001")]),
            _query(),
            _query(),
            _query(),
            _query(one=None),
        ]

        with patch("src.cli.seed_relay_validation_metrics.SessionLocal") as session_local:
            session_local.return_value.__enter__.return_value = session
            counts = seed_relay_validation_metrics(mark_legacy_unavailable=False)

        assert counts == {VALIDATION_UNVERIFIED: 1}
