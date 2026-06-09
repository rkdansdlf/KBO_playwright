from src.utils.team_codes import (
    KBO_GAME_ID_TEAM_CODES,
    KBO_LEGACY_TECHNICAL_CODE,
    STANDARD_TEAM_CODES,
    TEAM_NAME_TO_CODE,
    build_kbo_game_id,
    kbo_game_id_team_code,
    normalize_kbo_game_id,
    resolve_team_code,
    team_code_from_game_id_segment,
)


class TestTeamNameToCode:
    def test_active_teams(self):
        assert TEAM_NAME_TO_CODE["삼성"] == "SS"
        assert TEAM_NAME_TO_CODE["LG"] == "LG"
        assert TEAM_NAME_TO_CODE["두산"] == "DB"
        assert TEAM_NAME_TO_CODE["KIA"] == "KIA"
        assert TEAM_NAME_TO_CODE["NC"] == "NC"
        assert TEAM_NAME_TO_CODE["KT"] == "KT"

    def test_full_names(self):
        assert TEAM_NAME_TO_CODE["삼성 라이온즈"] == "SS"
        assert TEAM_NAME_TO_CODE["LG 트윈스"] == "LG"

    def test_historical_names(self):
        assert TEAM_NAME_TO_CODE["해태"] == "HT"
        assert TEAM_NAME_TO_CODE["현대"] == "HU"
        assert TEAM_NAME_TO_CODE["SK"] == "SK"


class TestResolveTeamCode:
    def test_none_returns_none(self):
        assert resolve_team_code(None) is None

    def test_known_name(self):
        assert resolve_team_code("LG") == "LG"
        assert resolve_team_code("두산 베어스") == "DB"


class TestKboGameIdTeamCode:
    def test_none_returns_none(self):
        assert kbo_game_id_team_code(None) is None

    def test_modern_code(self):
        assert kbo_game_id_team_code("SSG") == "SK"

    def test_legacy_code_stays(self):
        assert kbo_game_id_team_code("OB") == "OB"


class TestBuildKboGameId:
    def test_valid_build(self):
        game_id = build_kbo_game_id("20250415", "LG", "SS")
        assert game_id == "20250415LGSS0"

    def test_none_date_returns_none(self):
        assert build_kbo_game_id(None, "LG", "SS") is None

    def test_invalid_date_format(self):
        assert build_kbo_game_id("2025", "LG", "SS") is None

    def test_missing_teams(self):
        assert build_kbo_game_id("20250415", None, "SS") is None


class TestTeamCodeFromGameIdSegment:
    def test_known_segment(self):
        assert team_code_from_game_id_segment("OB") == "DB"
        assert team_code_from_game_id_segment("SK") == "SSG"
        assert team_code_from_game_id_segment("LG") == "LG"

    def test_none_returns_none(self):
        assert team_code_from_game_id_segment(None) is None

    def test_case_insensitive(self):
        assert team_code_from_game_id_segment("ob") == "DB"


class TestNormalizeKboGameId:
    def test_modern_to_legacy(self):
        assert normalize_kbo_game_id("20260418SSGNC0") == "20260418SKNC0"
        assert normalize_kbo_game_id("20260418KHLG0") == "20260418WOLG0"

    def test_short_id_unchanged(self):
        assert normalize_kbo_game_id("short") == "short"

    def test_old_year_unchanged(self):
        assert normalize_kbo_game_id("20230415SSGNC0") == "20230415SSGNC0"

    def test_already_legacy(self):
        assert normalize_kbo_game_id("20260415SKSG0") == "20260415SKSG0"


class TestLegacyTechnicalCode:
    def test_mappings(self):
        assert KBO_LEGACY_TECHNICAL_CODE["SSG"] == "SK"
        assert KBO_LEGACY_TECHNICAL_CODE["KH"] == "WO"
        assert KBO_LEGACY_TECHNICAL_CODE["DB"] == "OB"
        assert KBO_LEGACY_TECHNICAL_CODE["KIA"] == "HT"


class TestStandardTeamCodes:
    def test_ten_current_teams(self):
        assert len(STANDARD_TEAM_CODES) == 10

    def test_includes_active(self):
        for code in ("HH", "KIA", "KT", "LG", "LT", "NC", "DB", "SSG", "SS", "KH"):
            assert code in STANDARD_TEAM_CODES


class TestGameIdTeamCodes:
    def test_team_codes_contains_all_relevant(self):
        assert "LG" in KBO_GAME_ID_TEAM_CODES
        assert "SSG" in KBO_GAME_ID_TEAM_CODES
        assert "OB" in KBO_GAME_ID_TEAM_CODES
        assert "SK" in KBO_GAME_ID_TEAM_CODES
