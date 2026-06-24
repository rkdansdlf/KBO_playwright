from src.crawlers import player_pitching_all_series_crawler as module
from src.crawlers.player_pitching_all_series_crawler import (
    Basic2PageContext,
    PitcherStats,
    build_pitching_crawl_summary,
    parse_basic2_page,
)


class _Header:
    def __init__(self, text):
        self.text = text

    def inner_text(self):
        return self.text

    def text_content(self):
        return self.text


class _FakePage:
    def query_selector_all(self, selector):
        if selector == "table.tData01 thead th":
            return [_Header("순위"), _Header("선수명"), _Header("팀명"), _Header("NP")]
        return []


def test_parse_basic2_page_does_not_create_pitcher_without_basic1(monkeypatch):
    monkeypatch.setattr(module, "retry_wait_for_selector", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(module, "get_team_mapping_for_year", lambda _year: {"삼성": "SS"})
    monkeypatch.setattr(
        module,
        "extract_rows_fast",
        lambda _page, **kwargs: [
            {
                "cells": ["1", "Basic2전용", "삼성", "100"],
                "linkText": "Basic2전용",
                "linkHref": "x?playerId=2001",
            }
        ],
    )

    pitchers = {}
    processed = parse_basic2_page(Basic2PageContext(
        page=_FakePage(),
        season=2025,
        league="REGULAR",
        pitchers=pitchers,
        sort_key="NP",
    ))

    assert processed == 0
    assert pitchers == {}


def test_parse_basic2_page_enriches_existing_basic1_pitcher(monkeypatch):
    monkeypatch.setattr(module, "retry_wait_for_selector", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(module, "get_team_mapping_for_year", lambda _year: {"삼성": "SS"})
    monkeypatch.setattr(
        module,
        "extract_rows_fast",
        lambda _page, **kwargs: [
            {
                "cells": ["1", "원태인", "삼성", "100"],
                "linkText": "원태인",
                "linkHref": "x?playerId=2001",
            }
        ],
    )
    pitchers = {
        2001: PitcherStats(
            player_id=2001,
            player_name="원태인",
            season=2025,
            league="REGULAR",
            team_code="SS",
            games=10,
            innings_outs=90,
        )
    }

    processed = parse_basic2_page(Basic2PageContext(
        page=_FakePage(),
        season=2025,
        league="REGULAR",
        pitchers=pitchers,
        sort_key="NP",
    ))

    assert processed == 1
    assert pitchers[2001].extra_stats["metrics"]["np"] == 100


def test_pitching_crawl_summary_filters_rows_without_core_basic1_stats():
    summary, valid_stats = build_pitching_crawl_summary(
        [
            PitcherStats(
                player_id=2001,
                player_name="원태인",
                season=2025,
                league="REGULAR",
                team_code="SS",
                games=10,
                innings_outs=90,
            ),
            PitcherStats(
                player_id=2002,
                player_name="Basic2전용",
                season=2025,
                league="REGULAR",
                team_code="SS",
                extra_stats={"metrics": {"np": 100}},
            ),
        ]
    )

    assert summary == {
        "processed_rows": 2,
        "valid_rows": 1,
        "filtered_rows": 1,
        "failure_counts": {"empty_core_stats": 1},
    }
    assert [stat.player_id for stat in valid_stats] == [2001]
