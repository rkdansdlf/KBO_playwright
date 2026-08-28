"""Tests for FastAPI server gateway CLI and Master CLI routing."""

from __future__ import annotations

from unittest.mock import patch

from src.cli.kbo import main as kbo_main
from src.cli.serve_api import main as serve_main


def test_serve_cli_invokes_uvicorn() -> None:
    """Test serve CLI invokes uvicorn with configured options."""
    with patch("uvicorn.run") as mock_uvicorn:
        exit_code = serve_main(["--host", "0.0.0.0", "--port", "9000", "--reload"])
        assert exit_code == 0
        mock_uvicorn.assert_called_once_with(
            "src.api.app:app",
            host="0.0.0.0",
            port=9000,
            reload=True,
            workers=None,
        )


def test_kbo_master_cli_serve_dispatch() -> None:
    """Test Master CLI routing kbo serve."""
    with patch("src.cli.serve_api.main", return_value=0) as mock_main:
        exit_code = kbo_main(["serve", "--port", "8080"])
        assert exit_code == 0
        mock_main.assert_called_once_with(["--port", "8080"])
