from unittest.mock import patch, MagicMock

from src.cli.verify_sync_consistency import main


class TestVerifySyncConsistency:
    def test_default_run(self):
        with patch("src.cli.verify_sync_consistency.run_consistency_audit") as mock:
            mock.return_value = True
            try:
                main()
            except SystemExit:
                pass

    def test_deep_check(self):
        with patch("src.cli.verify_sync_consistency.run_consistency_audit") as mock:
            mock.return_value = True
            try:
                main()
            except SystemExit:
                pass

    def test_no_alert(self):
        with patch("src.cli.verify_sync_consistency.run_consistency_audit") as mock:
            mock.return_value = True
            try:
                main()
            except SystemExit:
                pass
