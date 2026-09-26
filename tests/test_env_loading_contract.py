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

#: Injected into isolated interpreters that need a path that does not exist.
MISSING = str(ROOT / "tests" / "_no_such_env_file_for_the_disabled_case")


def _run_isolated(
    code: str,
    *,
    env: dict[str, str] | None = None,
    unset: tuple[str, ...] = (),
) -> str:
    """Run `code` in a fresh interpreter and return its stdout.

    Anything that needs the loading guard *enabled* has to happen here rather
    than in the pytest process. Enabling the guard in-process opens a window in
    which an unrelated import can pull the real `.env` into the worker, and the
    credentials it loads survive the test that caused them.
    """
    child_env = {**os.environ, **(env or {})}
    for key in unset:
        child_env.pop(key, None)
    result = subprocess.run(
        [sys.executable, "-c", code],
        env=child_env,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
        cwd=ROOT,
    )
    assert result.returncode == 0, f"isolated run failed: {result.stderr}"
    return result.stdout.strip()


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

    def test_disabled_flag_is_read_at_call_time(self, monkeypatch):
        from src.config.env_loader import env_file_loading_enabled, load_project_env

        monkeypatch.setenv("KBO_ENV_FILE_LOADING", "0")
        assert env_file_loading_enabled() is False
        # No path given, so this would read the real `.env` if the guard were
        # ever bypassed. The guard being on is what makes this safe in-process.
        assert load_project_env() is False

    @pytest.mark.parametrize("falsy", ["0", "false", "FALSE", "no", "off", " 0 "])
    def test_every_falsy_spelling_disables_loading(self, monkeypatch, falsy: str):
        from src.config.env_loader import env_file_loading_enabled

        monkeypatch.setenv("KBO_ENV_FILE_LOADING", falsy)
        assert env_file_loading_enabled() is False

    def test_flag_enables_loading_when_opted_in(self):
        """The enabled half runs out-of-process on purpose.

        Flipping the ambient flag to a truthy value opens a window in which any
        module imported by pytest -- and several `src` modules call
        `load_project_env()` at module scope -- reads the real `.env` and leaks
        live credentials into the worker. The window closes when the test ends,
        but the secrets it loaded do not, so the leak outlives the test and
        surfaces later as an unrelated failure. An isolated interpreter has no
        such window.
        """
        output = _run_isolated(
            "from src.config.env_loader import env_file_loading_enabled, load_project_env\n"
            "print(env_file_loading_enabled())\n"
            f"print(load_project_env({MISSING!r}))\n",
            env={"KBO_ENV_FILE_LOADING": "1"},
        )

        assert output.splitlines() == ["True", "False"]

    def test_production_path_still_loads(self, tmp_path):
        """Default behaviour is unchanged, so real runs keep working."""
        env_file = tmp_path / ".env"
        env_file.write_text("KBO_TEST_PROBE_VALUE=loaded\n", encoding="utf-8")

        output = _run_isolated(
            "from src.config.env_loader import load_project_env\n"
            "import os\n"
            f"print(load_project_env({str(env_file)!r}))\n"
            "print(os.environ['KBO_TEST_PROBE_VALUE'])\n",
            unset=("KBO_ENV_FILE_LOADING", "KBO_TEST_PROBE_VALUE"),
        )

        assert output.splitlines() == ["True", "loaded"]

    def test_explicit_environment_wins_over_the_file(self, tmp_path):
        """`override=False` keeps a deliberately-set variable authoritative."""
        env_file = tmp_path / ".env"
        env_file.write_text("KBO_TEST_PROBE_VALUE=from_file\n", encoding="utf-8")

        output = _run_isolated(
            "from src.config.env_loader import load_project_env\n"
            "import os\n"
            f"load_project_env({str(env_file)!r})\n"
            "print(os.environ['KBO_TEST_PROBE_VALUE'])\n",
            env={"KBO_TEST_PROBE_VALUE": "from_process"},
            unset=("KBO_ENV_FILE_LOADING",),
        )

        assert output.splitlines() == ["from_process"]

    def test_missing_file_is_not_an_error(self, tmp_path):
        output = _run_isolated(
            "from src.config.env_loader import load_project_env\n"
            f"print(load_project_env({str(tmp_path / 'absent.env')!r}))\n",
            unset=("KBO_ENV_FILE_LOADING",),
        )

        assert output.splitlines() == ["False"]


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
