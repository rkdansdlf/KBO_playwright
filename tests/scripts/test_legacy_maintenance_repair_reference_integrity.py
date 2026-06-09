from unittest.mock import MagicMock

from scripts.legacy.maintenance.repair_reference_integrity import (
    CODE_VARIANTS,
    TEAM_CODE_REPAIRS,
    _parse_game_id,
    _variant_candidates,
    repair_team_codes,
)


class TestTeamCodeRepairs:
    def test_hd_to_hu(self):
        assert TEAM_CODE_REPAIRS == {"HD": "HU"}


class TestCodeVariants:
    def test_ssg_includes_sk(self):
        assert "SK" in CODE_VARIANTS["SSG"]
        assert "SSG" in CODE_VARIANTS["SSG"]

    def test_kia_includes_ht(self):
        assert "HT" in CODE_VARIANTS["KIA"]
        assert "KIA" in CODE_VARIANTS["KIA"]


class TestParseGameId:
    def test_valid(self):
        result = _parse_game_id("20250401LGSS0")
        assert result is not None
        assert result[0] == "20250401"
        assert result[1] == "LGSS"

    def test_invalid(self):
        assert _parse_game_id("") is None
        assert _parse_game_id("abc") is None


class TestVariantCandidates:
    def test_no_parsed(self):
        result = _variant_candidates("invalid", {"G1"})
        assert result == []

    def test_no_variants(self):
        result = _variant_candidates("20250401XXXX0", set())
        assert result == []


class TestRepairTeamCodes:
    def test_dry_run(self):
        conn = MagicMock()
        conn.dialect.name = "sqlite"
        inspector = MagicMock()
        inspector.get_table_names.return_value = ["game"]
        inspector.get_columns.return_value = [{"name": "home_team"}, {"name": "away_team"}]
        conn.execute.return_value.scalar.return_value = 5

        actions = repair_team_codes(conn, inspector, apply=False)
        assert len(actions) > 0
        assert actions[0].status == "dry_run"
