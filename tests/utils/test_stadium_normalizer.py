from __future__ import annotations

from src.utils.stadium_normalizer import (
    STADIUM_CODE_MAP,
    STADIUM_MAP,
    get_stadium_code,
    normalize_stadium_name,
)


class TestNormalizeStadiumName:
    def test_jamsil(self):
        assert normalize_stadium_name("잠실") == "잠실야구장"

    def test_munhak(self):
        assert normalize_stadium_name("문학") == "인천SSG랜더스필드"

    def test_gocheok(self):
        assert normalize_stadium_name("고척") == "고척스카이돔"

    def test_suwon(self):
        assert normalize_stadium_name("수원") == "수원 kt wiz 파크"

    def test_sajik(self):
        assert normalize_stadium_name("사직") == "부산 사직 야구장"

    def test_daegu(self):
        assert normalize_stadium_name("대구") == "대구 삼성 라이온즈 파크"

    def test_changwon(self):
        assert normalize_stadium_name("창원") == "창원NC파크"

    def test_gwangju(self):
        assert normalize_stadium_name("광주") == "광주-기아 챔피언스 필드"

    def test_hanbat(self):
        assert normalize_stadium_name("한밭") == "대전 한화생명 이글스 파크"

    def test_daejeon(self):
        assert normalize_stadium_name("대전") == "대전 한화생명 이글스 파크"

    def test_unknown_passthrough(self):
        assert normalize_stadium_name("Unknown Stadium") == "Unknown Stadium"

    def test_empty_string(self):
        assert normalize_stadium_name("") == ""

    def test_none(self):
        assert normalize_stadium_name(None) is None

    def test_whitespace_stripped(self):
        assert normalize_stadium_name("  잠실  ") == "잠실야구장"

    def test_already_normalized(self):
        assert normalize_stadium_name("잠실야구장") == "잠실야구장"


class TestGetStadiumCode:
    def test_jamsil(self):
        assert get_stadium_code("잠실야구장") == "JAMSIL"

    def test_munhak(self):
        assert get_stadium_code("인천SSG랜더스필드") == "MUNHAK"

    def test_gocheok(self):
        assert get_stadium_code("고척스카이돔") == "GOCHEOK"

    def test_unknown(self):
        assert get_stadium_code("Unknown Stadium") is None

    def test_none(self):
        assert get_stadium_code(None) is None


class TestStadiumMap:
    def test_major_stadiums_present(self):
        assert "잠실" in STADIUM_MAP
        assert "문학" in STADIUM_MAP
        assert "고척" in STADIUM_MAP
        assert "수원" in STADIUM_MAP

    def test_regional_stadiums_present(self):
        assert "울산" in STADIUM_MAP
        assert "포항" in STADIUM_MAP
        assert "청주" in STADIUM_MAP

    def test_historical_stadiums_present(self):
        assert "시민" in STADIUM_MAP
        assert "무등" in STADIUM_MAP
        assert "목동" in STADIUM_MAP


class TestStadiumCodeMap:
    def test_all_major_codes(self):
        assert STADIUM_CODE_MAP["잠실야구장"] == "JAMSIL"
        assert STADIUM_CODE_MAP["고척스카이돔"] == "GOCHEOK"
        assert STADIUM_CODE_MAP["수원 kt wiz 파크"] == "SUWON"
        assert STADIUM_CODE_MAP["부산 사직 야구장"] == "SAJIK"
        assert STADIUM_CODE_MAP["대구 삼성 라이온즈 파크"] == "DAEGU"
        assert STADIUM_CODE_MAP["창원NC파크"] == "CHANGWON"
        assert STADIUM_CODE_MAP["광주-기아 챔피언스 필드"] == "GWANGJU"
        assert STADIUM_CODE_MAP["대전 한화생명 이글스 파크"] == "HANBAT"
