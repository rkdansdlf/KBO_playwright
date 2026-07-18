"""Unit tests for play description parsing logic in KBOTextParser."""

from __future__ import annotations

import pytest

from src.utils.text_parser import KBOTextParser


class TestKBOTextParserParsedDetails:
    @pytest.mark.parametrize(
        "text,expected_outcome,expected_direction,expected_hit_type",
        [
            # Hits
            ("오지환 : 우익수 뒤 솔로 홈런 (홈런거리:120m)", "home_run", "right", "hit"),
            ("양의지 : 좌중간 1루타", "single", "left", "hit"),
            ("박해민 : 2루수 방면 내야안타", "single", "second_base", "hit"),
            ("최정 : 좌월 2루타", "double", "left", "hit"),
            ("구자욱 : 중전 3루타 (적시타)", "triple", "center", "hit"),
            # Walks & Strikeouts
            ("김재환 : 스트라이크아웃 (삼진아웃)", "strikeout", None, None),
            ("오스틴 : 헛스윙 삼진", "strikeout", None, None),
            ("소토 : 볼넷으로 진루", "walk", None, None),
            ("박병호 : 고의4구", "intentional_walk", None, None),
            ("최원준 : 몸에 맞는 볼 (사구)", "hit_by_pitch", None, None),
            # Sacrifices
            ("홍창기 : 희생번트 성공", "sacrifice_hit", None, None),
            ("신민재 : 중견수 희생플라이 아웃", "sacrifice_fly", "center", "flyout"),
            # Errors & Double Plays
            ("문보경 : 3루수 실책으로 출루", "error", "third_base", None),
            ("김혜성 : 유격수 땅볼 아웃 (병살타)", "double_play", "shortstop", "groundout"),
            # Common Outs & Play Types
            ("김도영 : 2루수 땅볼 아웃", "groundout", "second_base", "groundout"),
            ("정수빈 : 중견수 뜬공 (플라이)", "flyout", "center", "flyout"),
            ("강백호 : 좌익수 직선타 아웃", "lineout", "left", "lineout"),
            ("로하스 : 폭투로 1루 진루", "wild_pitch", None, None),
            # Runner Actions
            ("김주원 : 2루 도루 성공", "stolen_base", None, None),
            ("송성문 : 도루 실패 아웃", "caught_stealing", None, None),
            ("최형우 : 견제사 아웃", "runner_out", None, None),
            ("empty", None, None, None),  # fallback
            ("", None, None, None),
        ],
    )
    def test_parse_play_details(
        self,
        text: str,
        expected_outcome: str | None,
        expected_direction: str | None,
        expected_hit_type: str | None,
    ) -> None:
        result = KBOTextParser.parse_play_details(text)
        assert result["play_outcome"] == expected_outcome
        assert result["hit_direction"] == expected_direction
        assert result["hit_type"] == expected_hit_type
