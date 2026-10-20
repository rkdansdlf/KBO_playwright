"""Pin the team-code-agnostic play identity used by the presence filter."""

from __future__ import annotations

from src.services.rag_play_identity import play_identity

NEW_CODE = "경기일: 2026-08-16, 경기: DB vs KIA, 이닝: 1회top, 투수: 미상, 타자: 김대한, 내용: 김대한 : 삼진 아웃"
OLD_CODE = "경기일: 2026-08-16, 경기: OB vs HT, 이닝: 1회top, 투수: 미상, 타자: 김대한, 내용: 김대한 : 삼진 아웃"


def test_old_and_new_team_codes_are_the_same_play() -> None:
    """Only the team field differs between the two generations of the same play."""
    assert play_identity(NEW_CODE) == play_identity(OLD_CODE)
    assert play_identity(NEW_CODE) is not None


def test_the_order_of_the_teams_does_not_matter() -> None:
    """Away/home is sorted, so a swapped order names the same game."""
    swapped = NEW_CODE.replace("DB vs KIA", "KIA vs DB")

    assert play_identity(swapped) == play_identity(NEW_CODE)


def test_a_different_batter_is_a_different_play() -> None:
    """Keep the same inning's plays of different batters apart."""
    assert play_identity(NEW_CODE.replace("김대한", "박준순")) != play_identity(NEW_CODE)


def test_a_different_team_pair_is_a_different_game() -> None:
    """The same batter and event against other opponents must not match."""
    assert play_identity(NEW_CODE.replace("DB vs KIA", "LG vs KIA")) != play_identity(NEW_CODE)


def test_text_outside_the_template_has_no_identity() -> None:
    """Unparseable text is never matched, so it can never be skipped by mistake."""
    assert play_identity("임의 문장") is None
