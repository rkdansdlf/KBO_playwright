"""Impact mapping for database, schema, and infrastructure changes.

`test_impact_routing.py` covers profile selection. This module pins *which
existing project gate* each area routes to, because the interesting failures
are the ones where a change lands in an area that used to have no gate at all
(migrations, models, repositories, dependencies, CI).
"""

from __future__ import annotations

import pytest

from tools.agent_harness.project_adapter import KBOProjectAdapter


@pytest.fixture
def adapter() -> KBOProjectAdapter:
    return KBOProjectAdapter()


class TestSubsystemMapping:
    @pytest.mark.parametrize(
        "path",
        [
            "src/crawlers/food_crawler.py",
            "src/parsers/boxscore_parser.py",
            "tests/crawlers/test_food_crawler.py",
            "tests/parsers/test_boxscore_parser.py",
        ],
    )
    def test_crawler_surface(self, adapter: KBOProjectAdapter, path: str) -> None:
        assert adapter.subsystem_of(path) == "crawler"

    @pytest.mark.parametrize(
        "path",
        [
            "src/models/game.py",
            "src/repositories/game_repository.py",
            "src/db/engine.py",
            "src/sync/oracle_writer.py",
            "src/aggregators/season_stat_aggregator.py",
            "src/validators/quality_gate.py",
            "migrations/postgresql/055_crawl_execution_runs.sql",
            "migrations/sqlite/060_crawl_execution_runs.sql",
        ],
    )
    def test_database_surface(self, adapter: KBOProjectAdapter, path: str) -> None:
        assert adapter.subsystem_of(path) == "database"

    @pytest.mark.parametrize(
        "path",
        [
            "pyproject.toml",
            "uv.lock",
            "Dockerfile",
            "Dockerfile.playwright",
            "docker-compose.prod.yml",
            ".github/workflows/test_suite.yml",
            ".pre-commit-config.yaml",
        ],
    )
    def test_infrastructure_surface(self, adapter: KBOProjectAdapter, path: str) -> None:
        assert adapter.subsystem_of(path) in {"dependencies", "ci"}

    def test_unmapped_source_path_falls_back_to_platform(self, adapter: KBOProjectAdapter) -> None:
        assert adapter.subsystem_of("src/cli/kbo.py") is None
        assert "platform" in adapter.affected_subsystems(["src/cli/kbo.py"])

    def test_documentation_is_not_a_subsystem(self, adapter: KBOProjectAdapter) -> None:
        assert adapter.subsystem_of("Docs/guide.md") is None

    def test_windows_separators_normalize(self, adapter: KBOProjectAdapter) -> None:
        assert adapter.subsystem_of("src\\crawlers\\food_crawler.py") == "crawler"


class TestDatabaseRouting:
    def test_migration_change_escalates_to_refactor(self, adapter: KBOProjectAdapter) -> None:
        # A schema migration is not a feature change; it needs the wider gate.
        assert adapter.infer_profile_from_files(["migrations/postgresql/055_x.sql"]) == "refactor"

    def test_model_change_escalates_to_refactor(self, adapter: KBOProjectAdapter) -> None:
        assert adapter.infer_profile_from_files(["src/models/game.py"]) == "refactor"

    def test_repository_change_pulls_in_the_full_database_gate(self, adapter: KBOProjectAdapter) -> None:
        targets = adapter.pytest_targets(["src/repositories/game_repository.py"])

        assert "tests/migrations" in targets
        assert "tests/db" in targets
        assert "tests/repositories" in targets

    def test_migration_change_pulls_in_the_oracle_dialect_contract(self, adapter: KBOProjectAdapter) -> None:
        # Oracle is the production store, so a migration change must prove the
        # offline dialect contract too, not just that SQL runs on SQLite.
        targets = adapter.pytest_targets(["migrations/oracle/068_create_rag_chunk_terms.sql"])

        assert "tests/db" in targets
        assert "tests/migrations" in targets

    @pytest.mark.parametrize(
        "path",
        [
            "src/models/game.py",
            "src/repositories/game_repository.py",
            "src/db/engine.py",
            "migrations/sqlite/060_x.sql",
        ],
    )
    def test_database_change_requires_certification(self, adapter: KBOProjectAdapter, path: str) -> None:
        assert adapter.needs_certification([path]) is True

    def test_dependency_change_requires_certification(self, adapter: KBOProjectAdapter) -> None:
        assert adapter.needs_certification(["pyproject.toml"]) is True

    def test_crawler_change_does_not_require_certification(self, adapter: KBOProjectAdapter) -> None:
        assert adapter.needs_certification(["src/crawlers/food_crawler.py"]) is False

    def test_analytics_change_does_not_require_certification(self, adapter: KBOProjectAdapter) -> None:
        assert adapter.needs_certification(["src/rag/search_engine.py"], "analytics") is False


class TestExistingBehaviourIsPreserved:
    def test_no_changed_files_runs_only_the_harness_suite(self, adapter: KBOProjectAdapter) -> None:
        assert adapter.pytest_targets([]) == ["tests/agent_harness"]

    def test_crawler_change_pulls_in_the_selector_gate(self, adapter: KBOProjectAdapter) -> None:
        assert "tests/monitoring/test_crawler_selector_gate.py" in adapter.pytest_targets(["src/crawlers/x.py"])

    def test_crawler_gate_flag_only_for_crawler_paths(self, adapter: KBOProjectAdapter) -> None:
        assert adapter.needs_crawler_gate(["src/parsers/x.py"]) is True
        assert adapter.needs_crawler_gate(["src/rag/x.py"]) is False

    def test_refactor_profile_always_requires_certification(self, adapter: KBOProjectAdapter) -> None:
        assert adapter.needs_certification([], "refactor") is True

    def test_unknown_profile_raises(self, adapter: KBOProjectAdapter) -> None:
        with pytest.raises(ValueError, match="Unknown project verification profile"):
            adapter.source_of_truth("turbo")

    def test_target_list_is_deduplicated_and_sorted(self, adapter: KBOProjectAdapter) -> None:
        targets = adapter.pytest_targets(["src/crawlers/a.py", "src/crawlers/b.py", "src/parsers/c.py"])

        assert targets == sorted(targets)
        assert len(targets) == len(set(targets))
