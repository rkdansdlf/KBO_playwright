"""Environment loading contract.

`src/` used to call `load_dotenv()` at module scope in ten places, including
`src/db/engine.py`, which nearly every test imports. The observable damage was
a developer's real `TELEGRAM_CHAT_ID` reaching the pytest process, so a test
asserting "no chat id configured" passed alone and failed when a sibling module
ran first.

These tests pin both halves of the fix: the guard is honoured, and the
unguarded pattern cannot come back.
"""

from __future__ import annotations

import ast
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
ENV_LOADER = SRC / "config" / "env_loader.py"
LINT_SCRIPT = ROOT / "scripts" / "lint_module_level_dotenv.py"

#: Real values from a developer's `.env` must never be visible to tests. A
#: different assertion shape is needed per key, so they are listed explicitly.
SECRET_ENV_KEYS = (
    "TELEGRAM_CHAT_ID",
    "TELEGRAM_BOT_TOKEN",
    "NAVER_CLIENT_ID",
    "NAVER_CLIENT_SECRET",
    "TMAP_API_KEY",
    "KAKAO_REST_API_KEY",
    "OPENROUTER_API_KEY",
)


class TestGuardIsHonoured:
    def test_test_session_disables_env_loading(self):
        """`tests/conftest.py` must set the flag before any src import."""
        assert os.environ.get("KBO_ENV_FILE_LOADING") == "0"

    def test_importing_src_db_engine_does_not_touch_the_environment(self):
        """The whole point: an import must not mutate os.environ."""
        before = dict(os.environ)
        import src.db.engine  # imported for its import-time side effects

        added = {key for key in os.environ if key not in before}
        changed = {key for key, value in before.items() if os.environ[key] != value}
        assert not added, f"importing src.db.engine added env keys: {sorted(added)}"
        assert not changed, f"importing src.db.engine changed env values: {sorted(changed)}"

    def test_no_secret_from_a_real_env_file_is_visible(self):
        for key in SECRET_ENV_KEYS:
            assert os.environ.get(key) is None, f"{key} leaked into the test process"

    def test_flag_is_read_at_call_time(self, monkeypatch):
        """A test can opt back in, which is what opt-in tests need."""
        from src.config.env_loader import env_file_loading_enabled, load_project_env

        monkeypatch.setenv("KBO_ENV_FILE_LOADING", "0")
        assert env_file_loading_enabled() is False
        assert load_project_env() is False

        monkeypatch.setenv("KBO_ENV_FILE_LOADING", "1")
        assert env_file_loading_enabled() is True

    @pytest.mark.parametrize("falsy", ["0", "false", "FALSE", "no", "off", " 0 "])
    def test_every_falsy_spelling_disables_loading(self, monkeypatch, falsy: str):
        from src.config.env_loader import env_file_loading_enabled

        monkeypatch.setenv("KBO_ENV_FILE_LOADING", falsy)
        assert env_file_loading_enabled() is False

    def test_production_path_still_loads(self, monkeypatch, tmp_path):
        """Default behaviour is unchanged, so real runs keep working."""
        from src.config.env_loader import load_project_env

        monkeypatch.delenv("KBO_ENV_FILE_LOADING", raising=False)
        env_file = tmp_path / ".env"
        env_file.write_text("KBO_TEST_PROBE_VALUE=loaded\n", encoding="utf-8")
        monkeypatch.delenv("KBO_TEST_PROBE_VALUE", raising=False)

        assert load_project_env(env_file) is True
        assert os.environ["KBO_TEST_PROBE_VALUE"] == "loaded"

    def test_explicit_environment_wins_over_the_file(self, monkeypatch, tmp_path):
        """`override=False` keeps a deliberately-set variable authoritative."""
        from src.config.env_loader import load_project_env

        monkeypatch.delenv("KBO_ENV_FILE_LOADING", raising=False)
        env_file = tmp_path / ".env"
        env_file.write_text("KBO_TEST_PROBE_VALUE=from_file\n", encoding="utf-8")
        monkeypatch.setenv("KBO_TEST_PROBE_VALUE", "from_process")

        load_project_env(env_file)
        assert os.environ["KBO_TEST_PROBE_VALUE"] == "from_process"

    def test_missing_file_is_not_an_error(self, monkeypatch, tmp_path):
        from src.config.env_loader import load_project_env

        monkeypatch.delenv("KBO_ENV_FILE_LOADING", raising=False)
        assert load_project_env(tmp_path / "absent.env") is False


class TestUnguardedPatternCannotReturn:
    def test_lint_script_exists(self):
        assert LINT_SCRIPT.is_file(), "the module-level .env lint gate is missing"

    def test_lint_script_passes_on_the_repository(self):
        result = subprocess.run(
            [sys.executable, str(LINT_SCRIPT)],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stdout

    def test_lint_script_catches_a_reintroduced_module_level_load(self, tmp_path):
        """A guard that cannot fail is not a guard."""
        offender = tmp_path / "src" / "crawlers" / "regression_probe.py"
        offender.parent.mkdir(parents=True)
        offender.write_text("from dotenv import load_dotenv\n\nload_dotenv()\n", encoding="utf-8")

        result = subprocess.run(
            [sys.executable, str(LINT_SCRIPT), str(offender)],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 1, result.stdout
        assert "module-level" in result.stdout

    def test_lint_script_allows_a_guarded_module_level_load(self, tmp_path):
        offender = tmp_path / "src" / "crawlers" / "ok_probe.py"
        offender.parent.mkdir(parents=True)
        offender.write_text(
            "from src.config.env_loader import load_project_env\n\nload_project_env()\n",
            encoding="utf-8",
        )

        result = subprocess.run(
            [sys.executable, str(LINT_SCRIPT), str(offender)],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stdout

    def test_lint_script_allows_an_in_function_load(self, tmp_path):
        """An explicit CLI entry point loading on demand stays allowed."""
        offender = tmp_path / "src" / "cli" / "probe.py"
        offender.parent.mkdir(parents=True)
        offender.write_text(
            "from dotenv import load_dotenv\n\n\ndef main() -> int:\n    load_dotenv()\n    return 0\n",
            encoding="utf-8",
        )

        result = subprocess.run(
            [sys.executable, str(LINT_SCRIPT), str(offender)],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stdout


class TestNoModuleBypassesTheLoader:
    def test_dotenv_is_only_imported_by_the_loader(self):
        offenders: list[str] = []
        for path in SRC.rglob("*.py"):
            if path.resolve() == ENV_LOADER.resolve():
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                names: list[str] = []
                if isinstance(node, ast.Import):
                    names = [alias.name for alias in node.names]
                elif isinstance(node, ast.ImportFrom):
                    names = [node.module or ""]
                if any(name == "dotenv" or name.startswith("dotenv.") for name in names):
                    offenders.append(f"{path.relative_to(ROOT).as_posix()}:{node.lineno}")
        assert not offenders, f"modules importing python-dotenv directly: {offenders}"

    def test_conftest_flag_matches_the_loader_constant(self):
        """The literal in tests/conftest.py must not drift from the source."""
        from src.config.env_loader import ENV_FILE_LOADING_FLAG

        conftest = (ROOT / "tests" / "conftest.py").read_text(encoding="utf-8")
        assert f'os.environ["{ENV_FILE_LOADING_FLAG}"] = "0"' in conftest
