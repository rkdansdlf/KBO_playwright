from pathlib import Path
from unittest.mock import patch


class TestLintBareExcept:
    def test_scan_file_empty(self):
        from scripts.lint_bare_except import scan_file

        with patch.object(Path, "read_text", return_value="x = 1"):
            result = scan_file(Path("dummy.py"))
            assert result == []

    def test_scan_file_with_bare_except(self):
        from scripts.lint_bare_except import scan_file

        with patch.object(Path, "read_text", return_value="try:\n    pass\nexcept:\n    pass\n"):
            result = scan_file(Path("dummy.py"))
            assert len(result) >= 1

    def test_main_no_files(self):
        with patch("scripts.lint_bare_except.scan_file", return_value=[]), patch("sys.argv", ["script"]):
            from scripts.lint_bare_except import main

            result = main()
            assert result == 0
