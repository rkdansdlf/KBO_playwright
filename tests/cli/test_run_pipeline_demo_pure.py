from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from src.cli import run_pipeline_demo as demo


def test_ingest_schedule_fixtures_parses_and_saves(tmp_path):
    fixtures = tmp_path / "schedule"
    fixtures.mkdir()
    (fixtures / "b.html").write_text("<html>b</html>", encoding="utf-8")
    (fixtures / "a.html").write_text("<html>a</html>", encoding="utf-8")

    with (
        patch("src.cli.run_pipeline_demo.parse_schedule_html", side_effect=[[{"game_id": "a"}], []]) as parse,
        patch("src.cli.run_pipeline_demo.save_schedule_games", return_value=MagicMock(saved=2, failed=0)) as save,
    ):
        assert demo.ingest_schedule_fixtures(fixtures, "regular", 2025) == 2

    assert parse.call_count == 2
    save.assert_called_once_with([{"game_id": "a"}])


def test_ingest_game_fixtures_counts_successes(tmp_path):
    fixtures = tmp_path / "games"
    fixtures.mkdir()
    (fixtures / "20250405LGSS0.html").write_text("<html>game</html>", encoding="utf-8")

    payload = {"game_id": "20250405LGSS0"}
    with (
        patch("src.cli.run_pipeline_demo.parse_game_detail_html", return_value=payload) as parse,
        patch("src.cli.run_pipeline_demo.save_game_detail", return_value=True) as save,
    ):
        assert demo.ingest_game_fixtures(fixtures) == 1

    parse.assert_called_once_with("<html>game</html>", "20250405LGSS0", "20250405")
    save.assert_called_once_with(payload)


def test_run_futures_builds_namespace() -> None:
    crawl_futures = AsyncMock()
    with patch.dict("sys.modules", {"src.cli.crawl_futures": MagicMock(crawl_futures=crawl_futures)}):
        asyncio.run(demo.run_futures(limit=5, season=2025, delay=0.25, concurrency=2))

    args = crawl_futures.await_args.args[0]
    assert args.limit == 5
    assert args.season == 2025
    assert args.delay == 0.25
    assert args.concurrency == 2


def test_count_games_by_season_id_groups_unknown() -> None:
    session = MagicMock()
    session.query.return_value.group_by.return_value.order_by.return_value.all.return_value = [(None, 1), (202501, 3)]

    with patch("src.cli.run_pipeline_demo.SessionLocal", return_value=_session_cm(session)):
        assert demo._count_games_by_season_id() == {"unknown": 1, "202501": 3}


def test_show_summary_queries_each_game() -> None:
    game = MagicMock(game_date="2025-04-05", season_id=202501, away_score=2, home_score=1)
    game_query = MagicMock()
    game_query.filter.return_value.one_or_none.return_value = game
    batting_query = MagicMock()
    batting_query.filter.return_value.count.return_value = 18
    pitching_query = MagicMock()
    pitching_query.filter.return_value.count.return_value = 9
    session = MagicMock()
    session.query.side_effect = [game_query, batting_query, pitching_query]

    with (
        patch("src.cli.run_pipeline_demo.show_schedule_totals") as totals,
        patch("src.cli.run_pipeline_demo.SessionLocal", return_value=_session_cm(session)),
    ):
        demo.show_summary(["20250405LGSS0"])

    totals.assert_called_once_with()
    assert session.query.call_count == 3


def test_build_arg_parser_parses_fixture_and_futures_options() -> None:
    args = demo.build_arg_parser().parse_args(
        [
            "--schedule-fixtures",
            "schedule",
            "--schedule-season-type",
            "postseason",
            "--schedule-year",
            "2025",
            "--game-fixtures",
            "games",
            "--report-game-id",
            "20250405LGSS0",
            "--run-futures",
            "--futures-limit",
            "10",
            "--futures-season",
            "2025",
            "--futures-delay",
            "0.5",
            "--futures-concurrency",
            "4",
        ],
    )

    assert args.schedule_season_type == "postseason"
    assert args.schedule_year == 2025
    assert args.report_game_id == ["20250405LGSS0"]
    assert args.run_futures is True
    assert args.futures_concurrency == 4


def test_main_routes_missing_dirs_and_futures(tmp_path) -> None:
    missing = tmp_path / "missing"
    try:
        demo.main(["--schedule-fixtures", str(missing)])
        raise AssertionError("missing schedule directory should exit")
    except SystemExit as exc:
        assert "Schedule fixtures directory not found" in str(exc)

    with (
        patch("src.cli.run_pipeline_demo.asyncio.run", side_effect=lambda coro: coro.close()) as run,
        patch("src.cli.run_pipeline_demo.run_futures", return_value="future") as futures,
        patch("src.cli.run_pipeline_demo.show_schedule_totals") as totals,
    ):
        demo.main(["--run-futures", "--futures-season", "2025"])

    futures.assert_called_once_with(None, 2025, 1.5, 3)
    run.assert_called_once()
    totals.assert_called_once_with()


def _session_cm(session: MagicMock) -> MagicMock:
    session_cm = MagicMock()
    session_cm.__enter__.return_value = session
    return session_cm
