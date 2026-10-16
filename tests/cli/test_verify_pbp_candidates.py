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


def test_old_and_new_team_codes_give_the_same_game_key() -> None:
    """OB/HT and DB/KIA name the same game once the aliases are applied."""
    from src.cli.rag.verify_pbp_candidates import game_key

    assert game_key("20260816DBKIA0") == game_key("20260816OBHT0")
    assert game_key("20260816DBKIA0") is not None


def test_doubleheader_index_separates_two_games_of_one_day() -> None:
    """The trailing digit is the game of the day, so the two games of a doubleheader differ."""
    from src.cli.rag.verify_pbp_candidates import game_key

    assert game_key("20260816DBKIA0") != game_key("20260816DBKIA1")


def test_legacy_numeric_ids_have_no_game_key() -> None:
    """A legacy numeric key carries no game id, so it cannot confirm or refute a game."""
    from src.cli.rag.verify_pbp_candidates import game_key

    assert game_key("1680134") is None


def test_game_verdict_confirms_same_game_and_flags_other_games() -> None:
    """Confirmed: same game. Conflicting: stored under another game. Unverified: nothing to compare."""
    from src.cli.rag.verify_pbp_candidates import game_verdict

    assert game_verdict("20260816DBKIA0_7", ["20260816OBHT0_7"]) == "confirmed"
    assert game_verdict("20260816DBKIA0_7", ["20260816OBHT1_7"]) == "conflicting"
    assert game_verdict("20260816DBKIA0_7", ["1680134"]) == "unverified"


def test_legacy_key_is_confirmed_through_its_source_game() -> None:
    """A legacy key whose source row sits in the same game confirms the match."""
    from src.cli.rag.verify_pbp_candidates import classify

    result = classify(
        [{"source_row_id": "20260816DBKIA0_7", "content": NEW_CODE}],
        [("1680134", OLD_CODE)],
        {"1680134": ("20260816OBHT0", 1, "초", "김대한")},
    )

    assert result["game_identity"]["confirmed"] == 1
    assert result["legacy_mapping"]["legacy_mapping_agrees"] == 1


def test_legacy_key_from_another_game_is_conflicting() -> None:
    """The same text under a different doubleheader game must not be treated as the same play."""
    from src.cli.rag.verify_pbp_candidates import classify

    result = classify(
        [{"source_row_id": "20260816DBKIA0_7", "content": NEW_CODE}],
        [("1680134", OLD_CODE)],
        {"1680134": ("20260816OBHT1", 1, "초", "김대한")},
    )

    assert result["game_identity"]["conflicting"] == 1


def test_legacy_key_missing_from_source_stays_unverified() -> None:
    """Without a source row there is nothing to compare, so the match stays unverified."""
    from src.cli.rag.verify_pbp_candidates import classify

    result = classify([{"source_row_id": "20260816DBKIA0_7", "content": NEW_CODE}], [("1680134", OLD_CODE)], {})

    assert result["game_identity"]["unverified"] == 1
