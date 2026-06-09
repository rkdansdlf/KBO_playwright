
from src.parsers.player_profile_parser import (
    LABEL_REGEX,
    POS_MAP,
    TEAM_CODE_MAP,
    PlayerProfileParsed,
    _clean,
    _to_year,
    parse_back_number,
    parse_birth_date,
    parse_draft,
    parse_entry_year_team,
    parse_height_weight,
    parse_money,
    parse_path,
    parse_position_and_hands,
    parse_profile,
    tokenize_profile,
)


class TestClean:
    def test_whitespace_normalized(self):
        assert _clean("  Hello   World ") == "Hello World"

    def test_none_returns_empty(self):
        assert _clean(None) == ""

    def test_already_clean(self):
        assert _clean("Hello") == "Hello"


class TestToYear:
    def test_2000s(self):
        assert _to_year(0) == 2000
        assert _to_year(6) == 2006
        assert _to_year(24) == 2024
        assert _to_year(49) == 2049

    def test_1900s(self):
        assert _to_year(50) == 1950
        assert _to_year(98) == 1998
        assert _to_year(99) == 1999


class TestTokenizeProfile:
    def test_basic_tokenization(self):
        text = "선수명 : 홍길동 등번호 : No.25 포지션 : 투수(우투우타)"
        tokens = tokenize_profile(text)
        assert tokens["선수명"] == "홍길동"
        assert tokens["등번호"] == "No.25"
        assert tokens["포지션"] == "투수(우투우타)"

    def test_empty_input(self):
        assert tokenize_profile("") == {}

    def test_multiple_tokens(self):
        text = "선수명 : 홍길동 생년월일 : 1990년 05월 15일 신장/체중 : 180cm/95kg"
        tokens = tokenize_profile(text)
        assert "선수명" in tokens
        assert "생년월일" in tokens
        assert "신장/체중" in tokens


class TestParseBackNumber:
    def test_with_no_prefix(self):
        assert parse_back_number("25") == 25

    def test_with_no_dot_prefix(self):
        assert parse_back_number("No.25") == 25
        assert parse_back_number("No. 25") == 25

    def test_empty_or_none(self):
        assert parse_back_number("") is None
        assert parse_back_number(None) is None

    def test_no_number(self):
        assert parse_back_number("abc") is None


class TestParseBirthDate:
    def test_korean_format(self):
        assert parse_birth_date("1987년 06월 05일") == "1987-06-05"

    def test_korean_format_single_digit(self):
        assert parse_birth_date("1990년 1월 5일") == "1990-01-05"

    def test_iso_format(self):
        assert parse_birth_date("1990-05-15") == "1990-05-15"

    def test_dot_format(self):
        assert parse_birth_date("1990.05.15") == "1990-05-15"

    def test_slash_format(self):
        assert parse_birth_date("1990/05/15") == "1990-05-15"

    def test_empty_or_none(self):
        assert parse_birth_date("") is None
        assert parse_birth_date(None) is None

    def test_invalid_format(self):
        assert parse_birth_date("abc") is None


class TestParsePositionAndHands:
    def test_full_hands(self):
        result = parse_position_and_hands("투수(우투우타)")
        assert result["position"] == "P"
        assert result["throwing_hand"] == "R"
        assert result["batting_hand"] == "R"

    def test_left_hands(self):
        result = parse_position_and_hands("내야수(좌투좌타)")
        assert result["position"] == "IF"
        assert result["throwing_hand"] == "L"
        assert result["batting_hand"] == "L"

    def test_switch_hitter(self):
        result = parse_position_and_hands("포수(우투양타)")
        assert result["position"] == "C"
        assert result["throwing_hand"] == "R"
        assert result["batting_hand"] == "S"

    def test_no_hands(self):
        result = parse_position_and_hands("지명타자")
        assert result["position"] == "DH"
        assert result["throwing_hand"] is None
        assert result["batting_hand"] is None

    def test_empty_or_none(self):
        assert parse_position_and_hands("") == {"position": None, "throwing_hand": None, "batting_hand": None}
        assert parse_position_and_hands(None) == {"position": None, "throwing_hand": None, "batting_hand": None}

    def test_unknown_position(self):
        result = parse_position_and_hands("심판(우투우타)")
        assert result["position"] is None

    def test_no_parentheses(self):
        result = parse_position_and_hands("투수")
        assert result["position"] == "P"
        assert result["throwing_hand"] is None


class TestParseHeightWeight:
    def test_normal(self):
        assert parse_height_weight("180cm/95kg") == {"height_cm": 180, "weight_kg": 95}

    def test_with_spaces(self):
        assert parse_height_weight(" 180 cm / 95 kg ") == {"height_cm": 180, "weight_kg": 95}

    def test_empty_or_none(self):
        assert parse_height_weight("") == {"height_cm": None, "weight_kg": None}
        assert parse_height_weight(None) == {"height_cm": None, "weight_kg": None}

    def test_invalid_format(self):
        assert parse_height_weight("abc") == {"height_cm": None, "weight_kg": None}

    def test_case_insensitive(self):
        assert parse_height_weight("180CM/95KG") == {"height_cm": 180, "weight_kg": 95}


class TestParsePath:
    def test_hyphen_separated(self):
        assert parse_path("송정동초-무등중-진흥고") == ["송정동초", "무등중", "진흥고"]

    def test_arrow_separated(self):
        assert parse_path("초등학교 → 중학교 → 고등학교") == ["초등학교", "중학교", "고등학교"]

    def test_comma_separated(self):
        assert parse_path("대학교, 대학원") == ["대학교", "대학원"]

    def test_empty_or_none(self):
        assert parse_path("") == []
        assert parse_path(None) == []

    def test_single_item(self):
        assert parse_path("학교") == ["학교"]


class TestParseMoney:
    def test_krw_with_man(self):
        result = parse_money("160000만원")
        assert result["amount"] == 1600000000
        assert result["currency"] == "KRW"
        assert result["original"] == "160000만원"

    def test_usd(self):
        result = parse_money("200000달러")
        assert result["amount"] == 200000
        assert result["currency"] == "USD"

    def test_usd_symbol(self):
        result = parse_money("500000$")
        assert result["currency"] == "USD"

    def test_empty_or_none(self):
        assert parse_money("") == {"amount": None, "currency": None, "original": None}
        assert parse_money(None) == {"amount": None, "currency": None, "original": None}

    def test_dash_returns_none(self):
        result = parse_money("-")
        assert result["amount"] is None

    def test_plain_number_krw(self):
        result = parse_money("50000000")
        assert result["amount"] == 50000000
        assert result["currency"] == "KRW"

    def test_krw_with_won(self):
        result = parse_money("5000원")
        assert result["amount"] == 5000
        assert result["currency"] == "KRW"


class TestParseDraft:
    def test_full_draft_info(self):
        result = parse_draft("06 두산 2차 8라운드 59순위")
        assert result["draft_year"] == 2006
        assert result["draft_team_code"] == "OB"
        assert result["draft_round"] == 8
        assert result["draft_pick_overall"] == 59
        assert result["draft_type"] == "2차"

    def test_free_agent_draft(self):
        result = parse_draft("25 삼성 자유선발")
        assert result["draft_year"] == 2025
        assert result["draft_team_code"] == "SS"
        assert result["draft_type"] == "자유선발"

    def test_primary_draft(self):
        result = parse_draft("98 삼성 1차")
        assert result["draft_year"] == 1998
        assert result["draft_team_code"] == "SS"
        assert result["draft_type"] == "1차"

    def test_empty_or_none(self):
        default = {"draft_year": None, "draft_team_code": None, "draft_round": None, "draft_pick_overall": None, "draft_type": None}
        assert parse_draft("") == default
        assert parse_draft(None) == default
        assert parse_draft("-") == default

    def test_no_match(self):
        default = {"draft_year": None, "draft_team_code": None, "draft_round": None, "draft_pick_overall": None, "draft_type": None}
        assert parse_draft("abc") == default

    def test_historical_team_code(self):
        result = parse_draft("06 현대 1차")
        assert result["draft_team_code"] == "HD"


class TestParseEntryYearTeam:
    def test_normal(self):
        result = parse_entry_year_team("06두산")
        assert result["entry_year"] == 2006
        assert result["entry_team_code"] == "OB"

    def test_with_space(self):
        result = parse_entry_year_team("25 삼성")
        assert result["entry_year"] == 2025
        assert result["entry_team_code"] == "SS"

    def test_empty_or_none(self):
        assert parse_entry_year_team("") == {"entry_year": None, "entry_team_code": None}
        assert parse_entry_year_team(None) == {"entry_year": None, "entry_team_code": None}

    def test_no_match(self):
        assert parse_entry_year_team("abc") == {"entry_year": None, "entry_team_code": None}

    def test_historical_name(self):
        result = parse_entry_year_team("98 현대")
        assert result["entry_team_code"] == "HD"


class TestParseProfile:
    def test_full_profile(self):
        text = (
            "선수명 : 홍길동 등번호 : No.25\n"
            "생년월일 : 1990년 05월 15일\n"
            "포지션 : 투수(우투우타)\n"
            "신장/체중 : 180cm/95kg\n"
            "경력 : 송정동초-무등중-진흥고\n"
            "입단 계약금 : 200000달러\n"
            "연봉 : 160000만원\n"
            "지명순위 : 06 두산 2차 8라운드 59순위\n"
            "입단년도 : 06두산"
        )
        result = parse_profile(text, is_active=True, is_foreign=False)
        assert result.player_name == "홍길동"
        assert result.back_number == 25
        assert result.birth_date == "1990-05-15"
        assert result.position == "P"
        assert result.throwing_hand == "R"
        assert result.batting_hand == "R"
        assert result.height_cm == 180
        assert result.weight_kg == 95
        assert result.education_path == ["송정동초", "무등중", "진흥고"]
        assert result.signing_bonus_amount == 200000
        assert result.signing_bonus_currency == "USD"
        assert result.salary_amount == 1600000000
        assert result.salary_currency == "KRW"
        assert result.draft_year == 2006
        assert result.draft_team_code == "OB"
        assert result.draft_round == 8
        assert result.draft_pick_overall == 59
        assert result.draft_type == "2차"
        assert result.entry_year == 2006
        assert result.entry_team_code == "OB"
        assert result.is_active is True
        assert result.is_foreign is False

    def test_empty_profile(self):
        result = parse_profile("")
        assert result.player_id is None
        assert result.player_name is None

    def test_minimal_profile(self):
        text = "선수명 : 김철수"
        result = parse_profile(text)
        assert result.player_name == "김철수"

    def test_foreign_player(self):
        text = "선수명 : 외국인 등번호 : No.99"
        result = parse_profile(text, is_foreign=True)
        assert result.is_foreign is True

    def test_position_code_mapping(self):
        for korean, code in POS_MAP.items():
            text = f"선수명 : 선수 포지션 : {korean}"
            result = parse_profile(text)
            assert result.position == code, f"Failed for {korean} -> {code}"

    def test_team_code_map_contains_all_teams(self):
        assert TEAM_CODE_MAP["삼성"] == "SS"
        assert TEAM_CODE_MAP["두산"] == "OB"
        assert TEAM_CODE_MAP["LG"] == "LG"
        assert TEAM_CODE_MAP["SSG"] == "SSG"
        assert TEAM_CODE_MAP["NC"] == "NC"
        assert TEAM_CODE_MAP["KT"] == "KT"
        assert TEAM_CODE_MAP["키움"] == "WO"

    def test_player_profile_parsed_get(self):
        p = PlayerProfileParsed(player_name="Test")
        assert p.get("player_name") == "Test"
        assert p.get("nonexistent") is None
        assert p.get("nonexistent", "default") == "default"
        assert p.get("education_path") == []

    def test_player_profile_parsed_education_path_property(self):
        p = PlayerProfileParsed(education_or_career_path=["A", "B"])
        assert p.education_path == ["A", "B"]

    def test_player_profile_parsed_getitem(self):
        p = PlayerProfileParsed(player_name="Test")
        assert p["player_name"] == "Test"
        assert p["education_path"] == []

    def test_player_profile_parsed_getitem_invalid(self):
        p = PlayerProfileParsed()
        try:
            _ = p["invalid_key"]
            raise AssertionError()
        except KeyError:
            pass


class TestLabelRegex:
    def test_label_regex_matches_all_labels(self):
        text = "선수명 : A 등번호 : B 생년월일 : C 포지션 : D 신장/체중 : E 경력 : F 출신교 : G 입단 계약금 : H 연봉 : I 지명순위 : J 입단년도 : K"
        matches = LABEL_REGEX.findall(text)
        assert len(matches) == 11
