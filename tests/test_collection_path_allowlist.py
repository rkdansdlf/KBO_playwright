from __future__ import annotations

import ast
import warnings
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DIRECT_SAVE_NAMES = {"save_game_detail", "save_relay_data"}
ALLOWED_DIRECT_SAVE_FILES = {
    "scripts/crawl_2009_game_details.py",
    "src/cli/ingest_mock_game_html.py",
    "src/cli/live_crawler.py",
    "src/cli/run_pipeline_demo.py",
    "src/crawlers/game_detail_crawler.py",
    "src/repositories/relay_repository.py",
    "src/services/game_collection_service.py",
    "src/services/relay_recovery_service.py",
}


def test_db_writing_collection_paths_stay_allowlisted():
    offenders = {}
    for path in _repo_python_files():
        names = _direct_game_save_names(path)
        if names:
            offenders[_repo_path(path)] = names

    unexpected = {
        path: names
        for path, names in offenders.items()
        if path not in ALLOWED_DIRECT_SAVE_FILES
    }

    assert unexpected == {}
    assert "src/services/game_collection_service.py" in offenders
    assert "src/services/relay_recovery_service.py" in offenders


def _repo_python_files():
    for root_name in ("src", "scripts"):
        root = REPO_ROOT / root_name
        for path in root.rglob("*.py"):
            if "__pycache__" not in path.parts:
                yield path


def _direct_game_save_names(path: Path) -> list[str]:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imported_names: set[str] = set()
    module_aliases: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "src.repositories.game_repository":
            for alias in node.names:
                if alias.name in DIRECT_SAVE_NAMES:
                    imported_names.add(alias.asname or alias.name)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "src.repositories.game_repository":
                    module_aliases.add(alias.asname or alias.name.split(".")[-1])

    called_names: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Name) and func.id in imported_names:
            called_names.add(func.id)
        elif (
            isinstance(func, ast.Attribute)
            and func.attr in DIRECT_SAVE_NAMES
            and isinstance(func.value, ast.Name)
            and func.value.id in module_aliases
        ):
            called_names.add(func.attr)

    return sorted(imported_names | called_names)


def _repo_path(path: Path) -> str:
    return path.relative_to(REPO_ROOT).as_posix()
