from unittest.mock import patch

from scripts.investigations.investigate_normalization import main


class TestMain:
    @patch("scripts.investigations.investigate_normalization.normalize_kbo_game_id")
    def test_main(self, mock_normalize):
        mock_normalize.return_value = "normalized"
        main()
        assert mock_normalize.call_count == 4
