from unittest.mock import MagicMock, patch

from scripts.analytics.generate_we_matrix import build_arg_parser, build_matrix, parse_runners, write_matrix


class TestParseRunners:
    def test_empty(self):
        assert parse_runners(None) == "000"

    def test_valid(self):
        assert parse_runners("1--") == "100"
        assert parse_runners("-2-") == "010"
        assert parse_runners("--3") == "001"
        assert parse_runners("123") == "111"


class TestBuildMatrix:
    @patch("scripts.analytics.generate_we_matrix.SessionLocal")
    @patch("scripts.analytics.generate_we_matrix.pd.read_sql")
    def test_empty_result(self, mock_read_sql, mock_session_local):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session
        mock_df = MagicMock()
        mock_df.empty = True
        mock_read_sql.return_value = mock_df

        result = build_matrix(max_inning=9, score_cap=7, min_sample_size=1)
        assert result == {}

    @patch("scripts.analytics.generate_we_matrix.SessionLocal")
    @patch("scripts.analytics.generate_we_matrix.pd.read_sql")
    def test_with_data(self, mock_read_sql, mock_session_local):
        import pandas as pd

        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session
        mock_df = pd.DataFrame({
            "inning": [1, 1],
            "inning_half": ["TOP", "BOT"],
            "score_diff": [0, 1],
            "outs": [0, 1],
            "bases_before": ["---", "1--"],
            "home_won": [1, 0],
        })
        mock_read_sql.return_value = mock_df

        result = build_matrix(max_inning=9, score_cap=7, min_sample_size=1)
        assert isinstance(result, dict)


class TestWriteMatrix:
    @patch("scripts.analytics.generate_we_matrix.Path.open")
    @patch("scripts.analytics.generate_we_matrix.json.dump")
    def test_writes_file(self, mock_json_dump, mock_open):
        mock_file = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_file
        write_matrix({}, MagicMock())
        mock_json_dump.assert_called_once()


class TestBuildArgParser:
    def test_parser_created(self):
        parser = build_arg_parser()
        args = parser.parse_args([])
        assert args.max_inning == 9
        assert args.score_cap == 7
        assert args.min_sample_size == 1
