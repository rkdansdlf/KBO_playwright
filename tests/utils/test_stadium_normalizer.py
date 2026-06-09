"""Tests for stadium_normalizer — stadium name normalization."""


from src.utils.stadium_normalizer import get_stadium_code, normalize_stadium_name


class TestNormalizeStadiumName:
    def test_jamsil_shorthand(self):
        assert normalize_stadium_name("잠실") == "잠실야구장"

    def test_munhak_shorthand(self):
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

    def test_daejeon(self):
        assert normalize_stadium_name("대전") == "대전 한화생명 이글스 파크"

    def test_hanbat(self):
        assert normalize_stadium_name("한밭") == "대전 한화생명 이글스 파크"

    def test_full_name_passthrough(self):
        assert normalize_stadium_name("잠실야구장") == "잠실야구장"

    def test_unknown_name_passthrough(self):
        assert normalize_stadium_name("없는구장") == "없는구장"

    def test_empty_string(self):
        assert normalize_stadium_name("") == ""

    def test_none_input(self):
        assert normalize_stadium_name(None) is None


class TestGetStadiumCode:
    def test_jamsil(self):
        assert get_stadium_code("잠실야구장") == "JAMSIL"

    def test_munhak(self):
        assert get_stadium_code("인천SSG랜더스필드") == "MUNHAK"

    def test_gocheok(self):
        assert get_stadium_code("고척스카이돔") == "GOCHEOK"

    def test_unknown(self):
        assert get_stadium_code("없는구장") is None

    def test_none_input(self):
        assert get_stadium_code(None) is None
