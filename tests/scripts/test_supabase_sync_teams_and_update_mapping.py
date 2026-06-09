from unittest.mock import patch

from scripts.supabase.sync_teams_and_update_mapping import (
    create_team_mapping_rules,
    get_supabase_connection,
)


class TestCreateTeamMappingRules:
    def test_has_expected_codes(self):
        rules = create_team_mapping_rules([])
        assert rules["LG"] == "LG"
        assert rules["MBC"] == "LG"
        assert rules["KIA"] == "KIA"
        assert rules["HT"] == "KIA"
        assert rules["OB"] == "DOOSAN"
        assert rules["HH"] == "HANWHA"
        assert rules["SK"] == "SSG"
        assert len(rules) > 10


class TestGetSupabaseConnection:
    @patch("scripts.supabase.sync_teams_and_update_mapping.os.getenv")
    def test_missing_env(self, mock_getenv):
        import pytest

        mock_getenv.return_value = None
        with pytest.raises(ValueError, match="SUPABASE_DB_URL"):
            get_supabase_connection()
