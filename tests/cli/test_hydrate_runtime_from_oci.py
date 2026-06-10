from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.cli.hydrate_runtime_from_oci import main


class TestHydrateRuntimeFromOCICLI:
    def test_main_required_year(self):
        with (
            patch("src.cli.hydrate_runtime_from_oci.load_dotenv"),
            patch("src.cli.hydrate_runtime_from_oci.create_engine_for_url") as mock_create_engine,
            patch("src.cli.hydrate_runtime_from_oci.sessionmaker") as mock_sessionmaker,
            patch("src.cli.hydrate_runtime_from_oci.SessionLocal"),
            patch("src.cli.hydrate_runtime_from_oci.RuntimeHydrator") as MockHydrator,
        ):
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine
            mock_session = MagicMock()
            mock_sessionmaker.return_value = mock_session
            mock_hydrator_instance = MagicMock()
            mock_hydrator_instance.hydrate_year.return_value = {"players": 10}
            MockHydrator.return_value = mock_hydrator_instance

            main(["--source-url", "postgresql://source", "--year", "2025"])

            mock_create_engine.assert_called_once()
            MockHydrator.assert_called_once()
            mock_hydrator_instance.hydrate_year.assert_called_once_with(2025, target_date=None, preserve_aliases=False)

    def test_main_with_date_and_preserve_aliases(self):
        with (
            patch("src.cli.hydrate_runtime_from_oci.load_dotenv"),
            patch("src.cli.hydrate_runtime_from_oci.create_engine_for_url") as mock_create_engine,
            patch("src.cli.hydrate_runtime_from_oci.sessionmaker") as mock_sessionmaker,
            patch("src.cli.hydrate_runtime_from_oci.SessionLocal"),
            patch("src.cli.hydrate_runtime_from_oci.RuntimeHydrator") as MockHydrator,
        ):
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine
            mock_sessionmaker.return_value = MagicMock()
            mock_hydrator_instance = MagicMock()
            MockHydrator.return_value = mock_hydrator_instance

            main(["--source-url", "postgresql://source", "--year", "2025", "--date", "20250601", "--preserve-aliases"])

            import datetime

            call_kwargs = mock_hydrator_instance.hydrate_year.call_args.kwargs
            assert call_kwargs["preserve_aliases"] is True
            assert call_kwargs["target_date"] == datetime.date(2025, 6, 1)

    def test_main_missing_source_url_raises(self):
        with (
            patch("src.cli.hydrate_runtime_from_oci.load_dotenv"),
            patch("src.cli.hydrate_runtime_from_oci.os.getenv", return_value=None),
        ):
            try:
                main(["--year", "2025"])
            except SystemExit:
                pass
