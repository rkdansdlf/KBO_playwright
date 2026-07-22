from __future__ import annotations

import json

from src.cli.lookup_official_players import (
    LookupTarget,
    _deduplicate_targets,
    load_targets_from_csv,
    summarize_candidate,
    team_matches,
)


def test_team_matches_historical_doosan_alias() -> None:
    target = LookupTarget("최원준", "DB", 2020)

    assert team_matches(target, "두산") is True


def test_team_matches_samsung_code() -> None:
    target = LookupTarget("이승현", "SS", 2020)

    assert team_matches(target, "삼성 라이온즈") is True


def test_summarize_candidate_marks_team_and_season_evidence() -> None:
    target = LookupTarget("홍길동", "LG", 2020)
    candidate = {
        "player_id": 123,
        "name": "홍길동",
        "team": "LG",
        "career": "2018~2020",
        "position": "투수",
    }

    result = summarize_candidate(target, candidate)

    assert result["player_id"] == 123
    assert result["team_match"] is True
    assert result["career_mentions_season"] is True


def test_summarize_candidate_uses_career_team_history() -> None:
    target = LookupTarget("김민", "KT", 2020)
    candidate = {
        "player_id": 68043,
        "name": "김민",
        "team": "SSG",
        "career": "인천숭의초-평촌중-유신고-KT-상무-KT",
    }

    result = summarize_candidate(target, candidate)

    assert result["current_team_match"] is False
    assert result["career_team_match"] is True
    assert result["team_match"] is True


def test_load_targets_from_csv_uses_default_year(tmp_path) -> None:
    path = tmp_path / "targets.csv"
    path.write_text("name,team_code,season\n이승현,SS,\n", encoding="utf-8")

    assert load_targets_from_csv(path, 2020) == [LookupTarget("이승현", "SS", 2020)]


def test_deduplicate_targets_is_sorted() -> None:
    targets = [
        LookupTarget("최원준", "DB", 2020),
        LookupTarget("이승현", "SS", 2020),
        LookupTarget("최원준", "DB", 2020),
    ]

    assert _deduplicate_targets(targets) == [
        LookupTarget("이승현", "SS", 2020),
        LookupTarget("최원준", "DB", 2020),
    ]


def test_lookup_report_payload_is_json_serializable() -> None:
    target = LookupTarget("홍길동", "LG", 2020)
    payload = {
        "target": target.__dict__,
        "candidates": [summarize_candidate(target, {"player_id": 1, "team": "LG", "career": "2020"})],
    }

    assert json.loads(json.dumps(payload, ensure_ascii=False))["target"]["season"] == 2020
