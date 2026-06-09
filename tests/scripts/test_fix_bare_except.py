from pathlib import Path
from unittest.mock import patch


class TestFixBareExcept:
    def test_needs_logger(self):
        from scripts.fix_bare_except import needs_logger
        assert needs_logger("x = 1") is True
        assert needs_logger("import logging") is False

    def test_ensure_logger_adds_import(self):
        from scripts.fix_bare_except import ensure_logger
        text = "import os\nx = 1\n"
        result = ensure_logger(text)
        assert "import logging" in result
        assert "logger = logging.getLogger" in result

    def test_fix_file_no_change(self):
        from scripts.fix_bare_except import fix_file
        with patch.object(Path, "read_text", return_value="x = 1"), \
             patch.object(Path, "write_text") as mock_write:
            result = fix_file(Path("dummy.py"))
            assert result == 0
            mock_write.assert_not_called()

    def test_fix_file_with_pattern(self):
        from scripts.fix_bare_except import fix_file
        source = "try:\n    pass\nexcept Exception:\n    pass\n"
        with patch.object(Path, "read_text", return_value=source), \
             patch.object(Path, "write_text") as mock_write:
            result = fix_file(Path("dummy.py"))
            assert result == 1
            written = mock_write.call_args[0][0]
            assert "logger.exception" in written
