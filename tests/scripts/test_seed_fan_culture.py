from unittest.mock import mock_open, patch


class TestSeedFanCulture:
    CSV_CONTENT = "team_id_a,team_id_b,rivalry_name,intensity\nSSG,LG,문학-잠실더비,HIGH\n"

    def test_seed_rivalries_dry_run(self):
        with patch("scripts.seed_fan_culture.SessionLocal"), \
             patch("builtins.open", mock_open(read_data=self.CSV_CONTENT)):
            from scripts.seed_fan_culture import seed_rivalries
            result = seed_rivalries(dry_run=True)
            assert result == 1

    def test_main_dry_run(self):
        with patch("scripts.seed_fan_culture.seed_rivalries") as mock_fn, \
             patch("sys.argv", ["script", "--dry-run"]):
            from scripts.seed_fan_culture import main
            main()
            mock_fn.assert_called_once_with(dry_run=True)

    def test_main_default(self):
        with patch("scripts.seed_fan_culture.seed_rivalries") as mock_fn, \
             patch("sys.argv", ["script"]):
            from scripts.seed_fan_culture import main
            main()
            mock_fn.assert_called_once_with(dry_run=False)
