"""Regression tests for the notification-layering lint gate."""

from __future__ import annotations

from pathlib import Path

from scripts.lint_notification_layering import (
    BYPASS_MARKER,
    DELEGATION_FORBIDDEN_IMPORTS,
    FACADE_FILENAME,
    MODULE_RANKS,
    PACKAGE_DIR,
    TRANSPORT_ALLOWED_IMPORTS,
    _package_imports,
    delegation_violations,
    main as lint_main,
    package_violations,
    scan,
    stale_ranks,
    transport_violations,
    unranked_modules,
)

ROOT = Path(__file__).resolve().parents[2]
REAL_PACKAGE_MODULES = sorted(path.stem for path in (ROOT / PACKAGE_DIR).glob("*.py") if path.name != FACADE_FILENAME)


def _write(root: Path, relative: str, body: str) -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    return path


class TestPackageLayering:
    def test_upward_import_is_an_inversion(self) -> None:
        issues = package_violations("dispatcher", [(7, "publisher")])

        assert len(issues) == 1
        assert "layer inversion" in issues[0]
        assert "'dispatcher' (rank 3)" in issues[0]
        assert "'publisher' (rank 4)" in issues[0]

    def test_downward_import_is_allowed(self) -> None:
        assert package_violations("publisher", [(3, "dispatcher"), (4, "recorder")]) == []

    def test_sideways_import_is_allowed(self) -> None:
        """`retention` maintains the ledger it shares a rank with."""
        assert package_violations("retention", [(9, "incident")]) == []

    def test_undeclared_target_rank_is_reported(self) -> None:
        issues = package_violations("dispatcher", [(5, "brand_new_module")])

        assert len(issues) == 1
        assert "no declared rank" in issues[0]
        assert "brand_new_module" in issues[0]

    def test_module_without_own_rank_is_reported(self) -> None:
        issues = package_violations("unknown_module", [])

        assert len(issues) == 1
        assert "has no declared rank" in issues[0]


class TestBoundaryRules:
    def test_transport_may_import_only_pure_contracts(self) -> None:
        assert transport_violations([(6, "policy"), (7, "alert_dto")]) == []

    def test_transport_importing_the_ledger_is_reported(self) -> None:
        issues = transport_violations([(11, "incident")])

        assert len(issues) == 1
        assert "transport must not import 'incident'" in issues[0]

    def test_transport_importing_orchestration_is_reported(self) -> None:
        assert len(transport_violations([(12, "publisher")])) == 1

    def test_delegating_service_may_import_delivery(self) -> None:
        assert delegation_violations([(13, "dispatcher"), (15, "recorder")]) == []

    def test_delegating_service_importing_the_ledger_is_reported(self) -> None:
        issues = delegation_violations([(14, "incident")])

        assert len(issues) == 1
        assert "must not import 'incident'" in issues[0]


class TestRankRegistry:
    def test_unranked_module_is_reported(self) -> None:
        issues = unranked_modules(["dispatcher", "brand_new_module"])

        assert len(issues) == 1
        assert "brand_new_module.py" in issues[0]

    def test_stale_rank_is_reported(self) -> None:
        issues = stale_ranks(["dispatcher"])

        assert any("does not exist" in issue for issue in issues)
        assert any("publisher" in issue for issue in issues)

    def test_every_real_module_has_a_rank(self) -> None:
        """A new module must declare a rank instead of silently joining a level."""
        assert unranked_modules(REAL_PACKAGE_MODULES) == []

    def test_no_rank_is_stale(self) -> None:
        assert stale_ranks(REAL_PACKAGE_MODULES) == []

    def test_ranks_are_non_negative_integers(self) -> None:
        assert all(isinstance(rank, int) and rank >= 0 for rank in MODULE_RANKS.values())

    def test_rank_order_matches_the_real_dependency_graph(self) -> None:
        """Every real edge must travel downward or sideways."""
        for module in REAL_PACKAGE_MODULES:
            path = ROOT / PACKAGE_DIR / f"{module}.py"
            imports = _package_imports(path.read_text(encoding="utf-8"), str(path))

            assert package_violations(module, imports) == [], module


class TestScan:
    def test_layer_inversion_is_detected(self, tmp_path: Path) -> None:
        path = _write(
            tmp_path, "notifications/dispatcher.py", "from src.notifications.publisher import AlertPublisher\n"
        )

        issues = scan(path)

        assert len(issues) == 1
        assert "layer inversion" in issues[0]

    def test_clean_package_module_reports_nothing(self, tmp_path: Path) -> None:
        path = _write(
            tmp_path, "notifications/dispatcher.py", "from src.notifications.recorder import DeliveryRecorder\n"
        )

        assert scan(path) == []

    def test_function_local_import_is_still_seen(self, tmp_path: Path) -> None:
        body = "def go():\n    from src.notifications.publisher import AlertPublisher\n    return AlertPublisher\n"
        path = _write(tmp_path, "notifications/dispatcher.py", body)

        assert scan(path) != []

    def test_plain_import_form_is_detected(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "notifications/dispatcher.py", "import src.notifications.publisher\n")

        assert scan(path) != []

    def test_facade_symbol_import_is_out_of_scope(self, tmp_path: Path) -> None:
        """`from src.notifications import X` is a facade import, not a layer edge."""
        path = _write(tmp_path, "notifications/dispatcher.py", "from src.notifications import AlertPublisher\n")

        assert scan(path) == []

    def test_facade_file_is_exempt(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "notifications/__init__.py", "from src.notifications.publisher import AlertPublisher\n")

        assert scan(path) == []

    def test_transport_boundary_is_detected(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "utils/alerting.py", "from src.notifications.incident import IncidentManager\n")

        assert scan(path) != []

    def test_delegation_boundary_is_detected(self, tmp_path: Path) -> None:
        path = _write(
            tmp_path, "services/notification_service.py", "from src.notifications.incident import IncidentManager\n"
        )

        assert scan(path) != []

    def test_unrelated_file_is_out_of_scope(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "other/thing.py", "from src.notifications.publisher import AlertPublisher\n")

        assert scan(path) == []

    def test_bypass_marker_suppresses(self, tmp_path: Path) -> None:
        body = f"# {BYPASS_MARKER}: deliberate while the split lands\nfrom src.notifications.publisher import AlertPublisher\n"
        path = _write(tmp_path, "notifications/dispatcher.py", body)

        assert scan(path) == []


class TestRepositoryState:
    def test_repository_is_clean(self) -> None:
        assert lint_main([]) == 0

    def test_violation_returns_one(self, tmp_path: Path) -> None:
        path = _write(
            tmp_path, "notifications/dispatcher.py", "from src.notifications.publisher import AlertPublisher\n"
        )

        assert lint_main([str(path)]) == 1

    def test_explicit_clean_file_returns_zero(self, tmp_path: Path) -> None:
        path = _write(
            tmp_path, "notifications/dispatcher.py", "from src.notifications.recorder import DeliveryRecorder\n"
        )

        assert lint_main([str(path)]) == 0

    def test_transport_allowlist_is_used_and_minimal(self) -> None:
        """A dead allowlist would silently permit the next inversion."""
        path = ROOT / "src" / "utils" / "alerting.py"
        imports = {target for _, target in _package_imports(path.read_text(encoding="utf-8"), str(path))}

        assert imports, "the transport imports no notification module anymore; shrink the allowlist"
        assert imports <= TRANSPORT_ALLOWED_IMPORTS

    def test_delegation_forbidden_set_is_meaningful(self) -> None:
        assert set(MODULE_RANKS) >= DELEGATION_FORBIDDEN_IMPORTS


if __name__ == "__main__":
    import pytest

    pytest.main([__file__, "-q"])
