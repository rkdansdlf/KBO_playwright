from unittest.mock import patch


class TestSeedFanCulture:
    CSV_CONTENT = "team_id_a,team_id_b,rivalry_name,intensity\nSSG,LG,문학-잠실더비,HIGH\n"

    def test_seed_rivalries_dry_run(self, tmp_path):
        csv_file = tmp_path / "team_rivalries.csv"
        csv_file.write_text(self.CSV_CONTENT, encoding="utf-8")
        with (
            patch("scripts.seed_fan_culture.SessionLocal"),
            patch("scripts.seed_fan_culture.RIVALRIES_CSV", csv_file),
        ):
            from scripts.seed_fan_culture import seed_rivalries

            result = seed_rivalries(dry_run=True)
            assert result == 1

    def test_main_dry_run(self):
        with patch("scripts.seed_fan_culture.seed_rivalries") as mock_fn, patch("sys.argv", ["script", "--dry-run"]):
            from scripts.seed_fan_culture import main

            main()
            mock_fn.assert_called_once_with(dry_run=True)

    def test_main_default(self):
        with patch("scripts.seed_fan_culture.seed_rivalries") as mock_fn, patch("sys.argv", ["script"]):
            from scripts.seed_fan_culture import main

            main()
            mock_fn.assert_called_once_with(dry_run=False)
