"""Tests for pairing legacy ``id=N`` chunks to source rows by their text.

The fixtures are verbatim legacy chunk bodies taken from the live index, so the
parsers are checked against the formatting that actually exists.
"""

from __future__ import annotations

from src.services.rag_legacy_content_match import (
    AMBIGUOUS,
    FIELD_CONFLICT,
    MATCHED,
    PARSE_FAILED,
    UNMATCHED,
    LegacySourceRow,
    match_award,
    match_movement,
    match_team_history,
    normalize_award_category,
    normalize_award_type,
    normalize_source_team_name,
    parse_fields,
    team_franchise_ids,
    MOVEMENT_FIELDS,
    MOVEMENT_TEAM_FIELD,
)

MOVEMENT_BODY = (
    "선수 이동 기록 (FA, 트레이드, 드래프트)\n"
    "날짜: 2022-05-04\n"
    "이동 유형: 부상자 명단\n"
    "선수: 구자욱(외야수)\n"
    "팀: 삼성\n"
    "비고: 10일"
)
MOVEMENT_BODY_ERA_TEAM = (
    "선수 이동 기록 (FA, 트레이드, 드래프트)\n"
    "날짜: 2024-07-18\n"
    "이동 유형: 부상자 명단\n"
    "선수: 이우성(외야수)\n"
    "팀: 해태 타이거즈\n"
    "비고: 10일"
)
AWARD_BODY = (
    "KBO 수상 기록 (MVP, 신인왕, 골든글러브)\n시즌: 2021\n수상 종류: Golden Glove\n선수: 강민호\n팀: SS\n\n포지션: C"
)
TEAM_HISTORY_BODY = (
    "KBO 구단 변천사\n구단: kt\n시즌: 2021\n순위: 1\n도시: 수원\n주경기장: 수원 kt wiz 파크\n\n현재 구단명: kt wiz"
)


class TestParseFields:
    def test_a_complete_body_parses(self) -> None:
        fields = parse_fields(MOVEMENT_BODY, MOVEMENT_FIELDS)
        assert fields == {
            "date": "2022-05-04",
            "section": "부상자 명단",
            "player": "구자욱(외야수)",
        }

    def test_the_team_is_read_separately_because_it_is_optional(self) -> None:
        """One live chunk carries no team, so it cannot be a required field."""
        team = MOVEMENT_TEAM_FIELD.search(MOVEMENT_BODY)
        assert team is not None
        assert team.group(1).strip() == "삼성"

    def test_a_missing_field_returns_none_rather_than_the_rest(self) -> None:
        """A partial dict would invite matching on a subset."""
        assert parse_fields("날짜: 2022-05-04\n선수: 구자욱(외야수)", MOVEMENT_FIELDS) is None


class TestNormalizeSourceTeamName:
    def test_a_name_with_an_embedded_newline_collapses_to_its_first_token(self) -> None:
        assert normalize_source_team_name("삼미\n슈퍼스타즈") == "삼미"
        assert normalize_source_team_name("해태\n타이거즈") == "해태"

    def test_a_plain_name_is_unchanged(self) -> None:
        assert normalize_source_team_name("삼성") == "삼성"

    def test_an_empty_name_is_empty_not_an_error(self) -> None:
        assert normalize_source_team_name(None) == ""
        assert normalize_source_team_name("   ") == ""


class TestNormalizeAwardType:
    def test_the_three_legacy_names_map_to_the_column_values(self) -> None:
        assert normalize_award_type("Golden Glove") == "골든글러브"
        assert normalize_award_type("Rookie of the Year") == "신인상"
        assert normalize_award_type("MVP") == "MVP"

    def test_an_unknown_name_passes_through(self) -> None:
        """The table holds kinds the legacy generator never wrote."""
        assert normalize_award_type("수비상") == "수비상"


class TestMatchMovement:
    def test_a_single_candidate_matches(self) -> None:
        by_key = {
            ("2022-05-04", "구자욱(외야수)", "부상자 명단"): [
                LegacySourceRow(3814, "2022-05-04_삼성_구자욱(외야수)_부상자 명단_abc", "삼성")
            ]
        }
        outcome = match_movement(MOVEMENT_BODY, by_key, {})
        assert outcome.reason == MATCHED
        assert outcome.source_row_id == 3814
        assert outcome.natural_source_row_id == "2022-05-04_삼성_구자욱(외야수)_부상자 명단_abc"

    def test_the_team_name_splits_era_and_current_code_twins(self) -> None:
        """643 live rows matched twice until the chunk's team name was used.

        A franchise appears under both its era code (HT 해태) and its current one
        (KIA 기아); the chunk names the team it belongs to.
        """
        by_key = {
            ("2024-07-18", "이우성(외야수)", "부상자 명단"): [
                LegacySourceRow(4963, "2024-07-18_KIA_이우성(외야수)_부상자 명단_a", "KIA"),
                LegacySourceRow(5297, "2024-07-18_HT_이우성(외야수)_부상자 명단_b", "HT"),
            ]
        }
        outcome = match_movement(MOVEMENT_BODY_ERA_TEAM, by_key, {"해태 타이거즈": "HT"})
        assert outcome.reason == MATCHED
        assert outcome.source_row_id == 5297

    def test_twins_stay_ambiguous_when_the_team_name_is_unknown(self) -> None:
        by_key = {
            ("2024-07-18", "이우성(외야수)", "부상자 명단"): [
                LegacySourceRow(4963, "a", "KIA"),
                LegacySourceRow(5297, "b", "HT"),
            ]
        }
        outcome = match_movement(MOVEMENT_BODY_ERA_TEAM, by_key, {})
        assert outcome.reason == AMBIGUOUS
        assert outcome.candidates == (4963, 5297)

    def test_an_absent_row_is_unmatched_not_a_parse_failure(self) -> None:
        """'the text did not parse' and 'the row is gone' are different claims."""
        outcome = match_movement(MOVEMENT_BODY, {}, {})
        assert outcome.reason == UNMATCHED

    def test_a_body_without_the_fields_is_a_parse_failure(self) -> None:
        outcome = match_movement("무언가 다른 본문", {}, {})
        assert outcome.reason == PARSE_FAILED
        assert outcome.source_row_id is None

    def test_a_body_without_a_team_still_matches_when_the_rest_is_unique(self) -> None:
        """One live chunk (id=4549, 서준원) was written without ``팀``.

        Requiring the field made that chunk a parse failure, which reads as "the
        text is unusable" — but the team is only needed to split twins.
        """
        body = (
            "선수 이동 기록 (FA, 트레이드, 드래프트)\n"
            "날짜: 2023-03-30\n"
            "이동 유형: 자유계약선수 - 참가활동정지\n"
            "선수: 서준원(투수)"
        )
        by_key = {
            ("2023-03-30", "서준원(투수)", "자유계약선수 - 참가활동정지"): [
                LegacySourceRow(4549, "2023-03-30_LT_서준원(투수)_정지_a")
            ]
        }
        outcome = match_movement(body, by_key, {})
        assert outcome.reason == MATCHED
        assert outcome.source_row_id == 4549

    def test_single_candidate_with_conflicting_team_returns_field_conflict(self) -> None:
        """Single candidate whose team contradicts chunk must NOT match."""
        by_key = {
            ("2022-05-04", "구자욱(외야수)", "부상자 명단"): [
                LegacySourceRow(3814, "2022-05-04_LG_구자욱(외야수)_부상자 명단_abc", team_code="LG")
            ]
        }
        outcome = match_movement(MOVEMENT_BODY, by_key, {"삼성": "SS"})
        assert outcome.reason == FIELD_CONFLICT
        assert outcome.candidates == (3814,)


class TestMatchAward:
    def test_the_english_award_name_matches_the_korean_column(self) -> None:
        by_key = {
            (2021, "골든글러브", "강민호"): [LegacySourceRow(30, "2021_골든글러브_포수_강민호")],
        }
        outcome = match_award(AWARD_BODY, by_key)
        assert outcome.reason == MATCHED
        assert outcome.source_row_id == 30

    def test_a_year_with_no_such_award_is_unmatched(self) -> None:
        outcome = match_award(AWARD_BODY, {(1987, "골든글러브", "강민호"): [LegacySourceRow(1, "x")]})
        assert outcome.reason == UNMATCHED

    def test_a_shortened_foreign_name_matches_the_full_one(self) -> None:
        """The legacy generator wrote 로하스 where the table holds 멜 로하스 주니어."""
        body = "KBO 수상 기록\n시즌: 2020\n수상 종류: MVP\n선수: 로하스\n팀: KT"
        exact: dict = {}
        by_year_type = {
            (2020, "MVP"): [LegacySourceRow(11, "2020_MVP_NONE_멜 로하스 주니어", player_name="멜 로하스 주니어")]
        }
        outcome = match_award(body, exact, by_year_type)
        assert outcome.reason == MATCHED
        assert outcome.source_row_id == 11

    def test_a_short_name_fitting_two_rows_stays_ambiguous(self) -> None:
        """A substring fallback that picks one of two is worse than saying so."""
        body = "KBO 수상 기록\n시즌: 2020\n수상 종류: MVP\n선수: 김\n팀: KT"
        by_year_type = {
            (2020, "MVP"): [
                LegacySourceRow(1, "a", player_name="김하성"),
                LegacySourceRow(2, "b", player_name="김현수"),
            ]
        }
        outcome = match_award(body, {}, by_year_type)
        assert outcome.reason == AMBIGUOUS
        assert outcome.candidates == (1, 2)

    def test_the_exact_name_wins_over_the_fallback_pool(self) -> None:
        body = "KBO 수상 기록\n시즌: 2020\n수상 종류: MVP\n선수: 김하성\n팀: KT"
        exact = {(2020, "MVP", "김하성"): [LegacySourceRow(7, "exact")]}
        by_year_type = {(2020, "MVP"): [LegacySourceRow(7, "exact", player_name="김하성")]}
        outcome = match_award(body, exact, by_year_type)
        assert outcome.reason == MATCHED
        assert outcome.source_row_id == 7

    def test_the_fallback_does_not_cross_years(self) -> None:
        """Scoping to the same year and award is what keeps a short name safe."""
        body = "KBO 수상 기록\n시즌: 2020\n수상 종류: MVP\n선수: 로하스\n팀: KT"
        by_year_type = {(2019, "MVP"): [LegacySourceRow(9, "old", player_name="멜 로하스 주니어")]}
        outcome = match_award(body, {}, by_year_type)
        assert outcome.reason == UNMATCHED

    def test_single_candidate_with_conflicting_position_returns_field_conflict(self) -> None:
        """Award chunk with position 'C' (catcher) conflicting with row category '외야수'."""
        by_key = {
            (2021, "골든글러브", "강민호"): [LegacySourceRow(30, "2021_골든글러브_외야수_강민호", category="외야수")],
        }
        outcome = match_award(AWARD_BODY, by_key)
        assert outcome.reason == FIELD_CONFLICT
        assert outcome.candidates == (30,)

    def test_single_candidate_with_conflicting_team_returns_field_conflict(self) -> None:
        """Award chunk with team 'SS' conflicting with row team '두산'/'DB'."""
        by_key = {
            (2021, "골든글러브", "강민호"): [
                LegacySourceRow(30, "2021_골든글러브_포수_강민호", team_code="DB", team_name="두산", category="포수")
            ],
        }
        outcome = match_award(
            AWARD_BODY,
            by_key,
            team_franchises={"SS": frozenset({1}), "DB": frozenset({4}), "두산": frozenset({4})},
        )
        assert outcome.reason == FIELD_CONFLICT
        assert outcome.candidates == (30,)

    def test_position_alias_c_matches_korean_catcher(self) -> None:
        """'C' in chunk matches '포수' in row category."""
        by_key = {
            (2021, "골든글러브", "강민호"): [
                LegacySourceRow(30, "2021_골든글러브_포수_강민호", category="포수", team_code="SS")
            ],
        }
        outcome = match_award(AWARD_BODY, by_key, team_franchises={"SS": frozenset({1})})
        assert outcome.reason == MATCHED
        assert "category" in outcome.matched_fields
        assert "team" in outcome.matched_fields
        assert outcome.chunk_content_hash is not None


class TestAwardTeamIsComparedByFranchise:
    """A club appears as an era code on one side and a Korean name on the other.

    Measured 2026-11-03: 289 of the 361 awards field conflicts were this, not a
    disagreement about which club won.
    """

    def test_an_era_code_matches_the_current_name(self) -> None:
        """The chunk says HT; the table says KIA 타이거즈. Both are franchise 5."""
        body = "KBO 수상 기록\n시즌: 2021\n수상 종류: Golden Glove\n선수: 이의리\n팀: HT\n포지션: P"
        by_key = {
            (2021, "골든글러브", "이의리"): [
                LegacySourceRow(1, "2021_골든글러브_투수_이의리", team_name="KIA 타이거즈", category="투수")
            ],
        }
        outcome = match_award(body, by_key, team_franchises={"HT": frozenset({5}), "KIA 타이거즈": frozenset({5})})
        assert outcome.reason == MATCHED

    def test_a_different_franchise_is_still_a_conflict(self) -> None:
        """The franchise check must not turn every team pair into a match."""
        body = "KBO 수상 기록\n시즌: 2021\n수상 종류: Golden Glove\n선수: 이의리\n팀: HT\n포지션: P"
        by_key = {
            (2021, "골든글러브", "이의리"): [LegacySourceRow(1, "x", team_name="삼성", category="투수")],
        }
        outcome = match_award(body, by_key, team_franchises={"HT": frozenset({5}), "삼성": frozenset({1})})
        assert outcome.reason == FIELD_CONFLICT

    def test_an_unresolvable_label_falls_back_to_the_string(self) -> None:
        """An unknown label must not pass by being unknown."""
        body = "KBO 수상 기록\n시즌: 2021\n수상 종류: Golden Glove\n선수: 이의리\n팀: ZZ\n포지션: P"
        by_key = {
            (2021, "골든글러브", "이의리"): [LegacySourceRow(1, "x", team_name="삼성", category="투수")],
        }
        outcome = match_award(body, by_key, team_franchises={"ZZ": frozenset(), "삼성": frozenset({1})})
        assert outcome.reason == FIELD_CONFLICT

    def test_the_position_space_variant_matches(self) -> None:
        """The table spells it 지명 타자 and 지명타자; the chunk says DH."""
        body = "KBO 수상 기록\n시즌: 2020\n수상 종류: Golden Glove\n선수: 김현수\n팀: LG\n포지션: DH"
        by_key = {
            (2020, "골든글러브", "김현수"): [LegacySourceRow(1, "x", team_name="LG", category="지명 타자")],
        }
        assert match_award(body, by_key, team_franchises={"LG": frozenset({3})}).reason == MATCHED

    def test_an_outfield_subtype_matches_the_generic_outfield(self) -> None:
        body = "KBO 수상 기록\n시즌: 1990\n수상 종류: Golden Glove\n선수: 이호성\n팀: OB\n포지션: OF"
        by_key = {
            (1990, "골든글러브", "이호성"): [LegacySourceRow(1, "x", team_name="OB", category="중견수")],
        }
        assert match_award(body, by_key, team_franchises={"OB": frozenset({4})}).reason == MATCHED


class TestTeamFranchiseIds:
    def test_era_codes_resolve_through_the_history_table(self) -> None:
        """team_franchises lacks NX/CB/TP, so resolving from it alone returns nothing."""
        assert team_franchise_ids("HT") == frozenset({5})
        assert team_franchise_ids("KIA") == frozenset({5})
        assert team_franchise_ids("NX") == frozenset({11})
        assert team_franchise_ids("WO") == frozenset({11})
        assert team_franchise_ids("CB") == frozenset({6})
        assert team_franchise_ids("TP") == frozenset({6})

    def test_korean_names_resolve_through_the_alias_table(self) -> None:
        assert team_franchise_ids("삼성") == frozenset({1})
        assert team_franchise_ids("KIA 타이거즈") == frozenset({5})
        assert team_franchise_ids("넥센") == frozenset({11})
        assert team_franchise_ids("키움") == frozenset({11})

    def test_the_clubs_english_spelling_resolves(self) -> None:
        """`awards.team_name` stores ``kt wiz``; the alias table had only ``KT 위즈``."""
        assert team_franchise_ids("kt wiz") == frozenset({10})
        assert team_franchise_ids("kt 위즈") == frozenset({10})

    def test_an_unknown_label_resolves_to_nothing(self) -> None:
        """Empty is the caller's cue to fall back to the literal string."""
        assert team_franchise_ids("ZZ") == frozenset()

    def test_empty_labels_resolve_to_nothing(self) -> None:
        assert team_franchise_ids("") == frozenset()
        assert team_franchise_ids(None) == frozenset()

    def test_the_resolver_seam_overrides_the_table(self) -> None:
        assert team_franchise_ids("삼성", {"삼성": frozenset({99})}) == frozenset({99})


class TestMatchTeamHistory:
    def test_the_short_name_matches_the_source_column(self) -> None:
        r"""The chunk says 삼미; the column holds 삼미\\n슈퍼스타즈."""
        by_key = {(1982, "삼미"): [LegacySourceRow(7, "1982_SM")]}
        body = "KBO 구단 변천사\n구단: 삼미\n시즌: 1982\n순위: 6\n도시: 인천"
        outcome = match_team_history(body, by_key)
        assert outcome.reason == MATCHED
        assert outcome.source_row_id == 7
        assert outcome.natural_source_row_id == "1982_SM"

    def test_a_plain_name_matches_too(self) -> None:
        by_key = {(2021, "kt"): [LegacySourceRow(182, "2021_KT")]}
        outcome = match_team_history(TEAM_HISTORY_BODY, by_key)
        assert outcome.reason == MATCHED
        assert outcome.source_row_id == 182

    def test_ranking_conflict_returns_field_conflict(self) -> None:
        """Chunk ranking 6 conflicting with row ranking 1."""
        by_key = {(1982, "삼미"): [LegacySourceRow(7, "1982_SM", ranking=1, city="인천")]}
        body = "KBO 구단 변천사\n구단: 삼미\n시즌: 1982\n순위: 6\n도시: 인천"
        outcome = match_team_history(body, by_key)
        assert outcome.reason == FIELD_CONFLICT
        assert outcome.candidates == (7,)

    def test_city_conflict_returns_field_conflict(self) -> None:
        """Chunk city 인천 conflicting with row city 수원."""
        by_key = {(1982, "삼미"): [LegacySourceRow(7, "1982_SM", ranking=6, city="수원")]}
        body = "KBO 구단 변천사\n구단: 삼미\n시즌: 1982\n순위: 6\n도시: 인천"
        outcome = match_team_history(body, by_key)
        assert outcome.reason == FIELD_CONFLICT
        assert outcome.candidates == (7,)
