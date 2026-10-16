"""Pin the team-code-agnostic play key and the three-way candidate split."""

from __future__ import annotations

from src.cli.rag.verify_pbp_candidates import classify, play_key

NEW_CODE = "경기일: 2026-08-16, 경기: DB vs KIA, 이닝: 1회top, 투수: 미상, 타자: 김대한, 내용: 김대한 : 삼진 아웃"
OLD_CODE = "경기일: 2026-08-16, 경기: OB vs HT, 이닝: 1회top, 투수: 미상, 타자: 김대한, 내용: 김대한 : 삼진 아웃"


def test_team_code_variants_share_one_play_key() -> None:
    """Only the team field differs; the play itself is the same."""
    assert play_key(NEW_CODE) == play_key(OLD_CODE)
    assert play_key(NEW_CODE) is not None


def test_a_different_batter_is_a_different_play() -> None:
    """Keep plays of different batters apart even in the same inning."""
    other = NEW_CODE.replace("김대한", "박준순")

    assert play_key(NEW_CODE) != play_key(other)


def test_unparsed_text_is_reported_not_guessed() -> None:
    """Text outside the template is counted as unparsed, never matched."""
    assert play_key("임의 문장") is None


def test_classify_splits_matched_new_and_unparsed() -> None:
    """Matched = same play already stored under another code; new = absent; unparsed = not classifiable."""
    candidates = [
        {"source_row_id": "20260816DBKIA0_7", "content": NEW_CODE},
        {"source_row_id": "20260816DBKIA0_9", "content": NEW_CODE.replace("김대한", "정수빈")},
        {"source_row_id": "bad", "content": "임의 문장"},
    ]
    dense = [("1680134", OLD_CODE)]

    result = classify(candidates, dense)

    assert result["candidates"] == 3
    assert result["matched"] == 1
    assert result["new"] == 1
    assert result["unparsed"] == 1
    assert result["samples"][0]["dense_source_row_ids"] == ["1680134"]


def test_ambiguous_match_is_flagged() -> None:
    """Two stored rows for one play make the match ambiguous, so it is reported separately."""
    result = classify(
        [{"source_row_id": "c", "content": NEW_CODE}],
        [("1680134", OLD_CODE), ("1680198", OLD_CODE)],
    )

    assert result["matched"] == 1
    assert result["ambiguous"] == 1


def test_team_pairs_show_whether_a_match_stays_inside_one_game() -> None:
    """A match should pair the same two teams under their own codes; any other pair is suspect."""
    result = classify(
        [{"source_row_id": "c", "content": NEW_CODE}],
        [("1680134", OLD_CODE)],
    )

    assert result["team_pairs"] == [
        {"candidate_teams": "DB vs KIA", "dense_teams": "OB vs HT", "count": 1},
    ]
