"""Unit tests for historical_boxscore_import CLI."""

import hashlib
from unittest.mock import patch

from src.cli.historical_boxscore_import import (
    main,
    process_historical_manifest,
    read_boxscore_manifest_entries,
    validate_boxscore_payload,
)


class TestHistoricalBoxscoreImportCli:
    def test_validate_boxscore_payload(self):
        data = {
            "teams": {"away": {"code": "HH"}, "home": {"code": "SSG"}},
            "hitters": {
                "away": [{"player_name": "김태균"}],
                "home": [{"player_name": "박재홍"}],
            },
            "pitchers": {
                "away": [{"player_name": "류현진"}],
                "home": [{"player_name": "김광현"}],
            },
        }
        valid, err = validate_boxscore_payload(data, strict=True)
        assert valid is True
        assert err is None

    def test_validate_boxscore_payload_incomplete(self):
        data = {
            "teams": {"away": {"code": "HH"}, "home": {"code": "SSG"}},
            "hitters": {"away": [], "home": []},
            "pitchers": {"away": [], "home": []},
        }
        valid, err = validate_boxscore_payload(data, strict=True)
        assert valid is False
        assert "incomplete" in (err or "")

    def test_process_historical_manifest_dry_run(self, tmp_path):
        html_content = """
        <html><body>
        <table><thead><tr><th>팀</th><th>R</th><th>H</th><th>E</th></tr></thead><tbody><tr><td>한화</td><td>1</td><td>5</td><td>0</td></tr><tr><td>SK</td><td>2</td><td>6</td><td>1</td></tr></tbody></table>
        <table><thead><tr><th>선수</th><th>타수</th><th>안타</th></tr></thead><tbody><tr><td>김태균</td><td>4</td><td>2</td></tr></tbody></table>
        <table><thead><tr><th>선수</th><th>이닝</th><th>삼진</th></tr></thead><tbody><tr><td>류현진</td><td>7</td><td>9</td></tr></tbody></table>
        </body></html>
        """
        html_file = tmp_path / "game_20090404.html"
        html_file.write_text(html_content, encoding="utf-8")
        sha256_val = hashlib.sha256(html_content.encode("utf-8")).hexdigest()

        manifest_file = tmp_path / "manifest.json"
        manifest_file.write_text(
            f"""
            [
              {{
                "game_id": "20090404HHSK0",
                "locator": "{html_file.name}",
                "season": 2009,
                "sha256": "{sha256_val}"
              }}
            ]
            """,
            encoding="utf-8",
        )

        entries = read_boxscore_manifest_entries(manifest_file)
        assert len(entries) == 1
        assert entries[0]["game_id"] == "20090404HHSK0"

        with patch(
            "src.cli.historical_boxscore_import._process_single_entry",
            return_value={"game_id": "20090404HHSK0", "season": 2009, "status": "valid"},
        ):
            report = process_historical_manifest(manifest_file, dry_run=True, strict=False)
        assert report["summary"]["total_entries"] == 1
        assert report["summary"]["valid"] == 1

    def test_main_cli_help(self, capsys):
        try:
            main(["--help"])
        except SystemExit as e:
            assert e.code == 0
        captured = capsys.readouterr()
        assert "Historical Boxscore Manifest Import CLI" in captured.out
