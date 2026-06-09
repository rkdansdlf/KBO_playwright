from pathlib import Path
from unittest.mock import patch


class TestConvertPrintToLogger:
    def test_process_file_no_changes(self):
        from scripts.convert_print_to_logger import process_file
        with patch.object(Path, "read_text", return_value="x = 1\n"), \
             patch.object(Path, "write_text"):
            result = process_file(Path("dummy.py"))
            assert result == 0

    def test_process_file_converts_print(self):
        from scripts.convert_print_to_logger import process_file
        source = 'print("hello")\n'
        with patch.object(Path, "read_text", return_value=source), \
             patch.object(Path, "write_text") as mock_write:
            result = process_file(Path("dummy.py"))
            assert result == 1
            written = mock_write.call_args[0][0]
            assert "logger.info" in written
            assert "import logging" in written

    def test_process_file_error_emoji(self):
        from scripts.convert_print_to_logger import process_file
        source = 'print("❌ error occurred")\n'
        with patch.object(Path, "read_text", return_value=source), \
             patch.object(Path, "write_text") as mock_write:
            result = process_file(Path("dummy.py"))
            assert result == 1
            written = mock_write.call_args[0][0]
            assert "logger.error" in written

    def test_process_file_warning_emoji(self):
        from scripts.convert_print_to_logger import process_file
        source = 'print("⚠️ warning")\n'
        with patch.object(Path, "read_text", return_value=source), \
             patch.object(Path, "write_text") as mock_write:
            result = process_file(Path("dummy.py"))
            assert result == 1
            written = mock_write.call_args[0][0]
            assert "logger.warning" in written
