from __future__ import annotations

from src.utils.stadium_normalizer import (
    STADIUM_CODE_MAP,
    STADIUM_MAP,
    get_stadium_code,
    normalize_stadium_name,
)


class TestNormalizeStadiumName:
    def test_잠실(self):
        assert normalize_stadium_name("잠실") == "잠실야구장"

    def test_문학(self):
        assert normalize_stadium_name("문학") == "인천SSG랜더스필드"

    def test_인천문학야구장(self):
        assert normalize_stadium_name("인천문학야구장") == "인천SSG랜더스필드"

    def test_고척(self):
        assert normalize_stadium_name("고척") == "고척스카이돔"

    def test_수원(self):
        assert normalize_stadium_name("수원") == "수원 kt wiz 파크"

    def test_사직(self):
        assert normalize_stadium_name("사직") == "부산 사직 야구장"

    def test_대구(self):
        assert normalize_stadium_name("대구") == "대구 삼성 라이온즈 파크"

    def test_창원(self):
        assert normalize_stadium_name("창원") == "창원NC파크"

    def test_광주(self):
        assert normalize_stadium_name("광주") == "광주-기아 챔피언스 필드"


class TestGetStadiumCode:
    def test_jamsil(self):
        assert get_stadium_code("잠실야구장") == "JAMSIL"

    def test_munhak(self):
        assert get_stadium_code("인천SSG랜더스필드") == "MUNHAK"


class TestStadiumMap:
    def test_major_stadiums_present(self):
        assert "잠실" in STADIUM_MAP
        assert "문학" in STADIUM_MAP
        assert "고척" in STADIUM_MAP
        assert "수원" in STADIUM_MAP
