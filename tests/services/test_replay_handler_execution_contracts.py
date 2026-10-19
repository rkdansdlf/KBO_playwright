"""Replay handler registration and team source registry contracts.

The seat dead-letter failure (``1c40266e``) was invisible to the test suite
because no test checked that every registered handler can actually reach its
crawler. The handler was registered, the crawler was adopted, and the two
never met until a dead letter exhausted five retries on ``KeyError('seat')``.

These tests focus on the structural contracts that the AST-based signature
check cannot reliably verify:

1. Every registered handler is reachable in the dispatcher.
2. ``_TEAM_SOURCES`` keys match the types ``_replay_team_page`` accepts.
3. Seat does not go through ``_replay_team_page`` (it has its own path).
4. Every fully-adopted crawler has a replay handler.
"""

from __future__ import annotations

import pytest

from src.services import crawl_replay_dispatcher as dispatcher
from src.services.crawl_replay_dispatcher import build_default_dispatcher


class TestHandlerRegistration:
    """Every registered handler must be reachable in the dispatcher."""

    def test_all_expected_crawlers_are_registered(self) -> None:
        """All 16 crawlers that have a replay handler must be registered."""
        expected = {
            "awards",
            "roster_transactions",
            "schedule",
            "game_detail",
            "relay",
            "food",
            "parking",
            "seat",
            "kbo_event",
            "player_movement",
            "team_history",
            "realtime_issue",
            "preview",
            "pbp",
            "player_batting_all_series",
            "player_pitching_all_series",
        }
        registered = build_default_dispatcher().registered_crawlers()
        missing = expected - registered
        assert not missing, f"Missing handlers: {sorted(missing)}"

    def test_seat_is_registered(self) -> None:
        """Seat must have a replay handler."""
        assert "seat" in build_default_dispatcher().registered_crawlers()

    def test_food_is_registered(self) -> None:
        """Food must have a replay handler."""
        assert "food" in build_default_dispatcher().registered_crawlers()

    def test_parking_is_registered(self) -> None:
        """Parking must have a replay handler."""
        assert "parking" in build_default_dispatcher().registered_crawlers()


class TestTeamSourceRegistry:
    """``_TEAM_SOURCES`` must match the types ``_replay_team_page`` accepts."""

    def test_team_source_keys_match_replay_team_page_type(self) -> None:
        """Every key in ``_TEAM_SOURCES`` must be a crawler name that
        ``_replay_team_page`` is typed to accept (food, parking).
        """
        expected = {"food", "parking"}
        actual = set(dispatcher._TEAM_SOURCES)
        assert actual == expected, (
            f"_TEAM_SOURCES keys {sorted(actual)} != expected {sorted(expected)}. "
            f"A team crawler missing here will KeyError on replay."
        )

    def test_team_source_keys_match_registered_crawlers(self) -> None:
        """Every key in ``_TEAM_SOURCES`` must be a registered crawler."""
        registered = build_default_dispatcher().registered_crawlers()
        for key in dispatcher._TEAM_SOURCES:
            assert key in registered, f"_TEAM_SOURCES[{key!r}] is not a registered crawler"

    def test_food_and_parking_handlers_use_team_page_helper(self) -> None:
        """``_replay_food`` and ``_replay_parking`` must delegate to
        ``_replay_team_page`` -- that is the helper that validates the team
        code against ``_TEAM_SOURCES``.
        """
        import ast
        import inspect

        for handler_name in ("_replay_food", "_replay_parking"):
            handler_fn = getattr(dispatcher, handler_name)
            src = inspect.getsource(handler_fn)
            tree = ast.parse(src)
            called = set()
            for child in ast.walk(tree):
                if isinstance(child, ast.Call):
                    func = child.func
                    if isinstance(func, ast.Name):
                        called.add(func.id)
                    elif isinstance(func, ast.Attribute):
                        called.add(func.attr)
            assert "_replay_team_page" in called, (
                f"{handler_name} does not call _replay_team_page -- team codes will not be validated"
            )

    def test_seat_handler_does_not_use_team_page_helper(self) -> None:
        """``_replay_seat`` must NOT delegate to ``_replay_team_page`` --
        that helper is typed for FoodCrawler | ParkingCrawler and would
        raise ``KeyError('seat')`` on ``_TEAM_SOURCES``.
        """
        import ast
        import inspect

        handler_fn = dispatcher._replay_seat
        src = inspect.getsource(handler_fn)
        tree = ast.parse(src)
        called = set()
        for child in ast.walk(tree):
            if isinstance(child, ast.Call):
                func = child.func
                if isinstance(func, ast.Name):
                    called.add(func.id)
                elif isinstance(func, ast.Attribute):
                    called.add(func.attr)
        assert "_replay_team_page" not in called, (
            "_replay_seat calls _replay_team_page -- this is the bug that "
            "caused KeyError('seat') and exhausted 4 dead letters"
        )


class TestRegisteredCrawlersMatchAdoptionMatrix:
    """Every fully-adopted crawler must have a replay handler."""

    def test_all_fully_adopted_crawlers_are_registered(self) -> None:
        """A crawler that is fully adopted but has no replay handler means
        its dead letters can never be retried.
        """
        from src.crawlers.adoption_matrix import build_matrix

        matrix = build_matrix()
        # Module names do not always match crawler names
        # (e.g. award_crawler -> awards, roster_transaction_crawler -> roster_transactions).
        module_to_crawler = {
            "award_crawler": "awards",
            "roster_transaction_crawler": "roster_transactions",
        }
        fully_adopted = {
            module_to_crawler.get(row.module, row.module.removesuffix("_crawler")) for row in matrix.adopted()
        }
        registered = build_default_dispatcher().registered_crawlers()

        missing = fully_adopted - registered
        assert not missing, (
            f"Fully adopted crawlers without a replay handler: {sorted(missing)}. "
            f"Their dead letters can never be retried."
        )
