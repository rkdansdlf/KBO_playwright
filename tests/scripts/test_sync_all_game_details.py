import importlib
import subprocess
from unittest.mock import MagicMock, patch


class TestSyncAllGameDetails:
    def test_main_calls_subprocess(self):
        with patch("scripts.sync_all_game_details.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock()
            import scripts.sync_all_game_details
            importlib.reload(scripts.sync_all_game_details)
            scripts.sync_all_game_details.main()
            assert mock_run.call_count > 0

    def test_main_handles_failure(self):
        with patch("scripts.sync_all_game_details.subprocess.run") as mock_run:
            mock_run.side_effect = [MagicMock()] + [subprocess.CalledProcessError(1, ["cmd"])] + [MagicMock()] * 20
            import scripts.sync_all_game_details
            importlib.reload(scripts.sync_all_game_details)
            scripts.sync_all_game_details.main()
            assert mock_run.call_count > 1
