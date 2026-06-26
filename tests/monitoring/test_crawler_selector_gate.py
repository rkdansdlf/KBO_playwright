from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.monitoring.crawler_selector_gate import (
    SelectorCheck,
    SelectorGateResult,
    SelectorGateSummary,
    SelectorIssue,
    SelectorTarget,
    _check_from_dict,
    _evaluate_target,
    _safe_artifact_name,
    _target_from_dict,
    evaluate_html_target,
    load_selector_config,
    run_selector_gate,
    render_selector_summary,
)


class TestDataclassProperties:
    def test_selector_issue_to_dict(self) -> None:
        issue = SelectorIssue(
            category="selector_missing",
            check_name="rows",
            selector="tbody tr",
            message="Expected at least 1 match(es), found 0",
            observed_count=0,
        )
        result = issue.to_dict()
        assert result["category"] == "selector_missing"
        assert result["check_name"] == "rows"
        assert result["selector"] == "tbody tr"
        assert result["observed_count"] == 0

    def test_selector_gate_result_ok_true(self) -> None:
        result = SelectorGateResult(
            target="t",
            source="s",
            source_type="inline",
            checks={},
            issues=[],
        )
        assert result.ok is True
        assert result.issue_count == 0

    def test_selector_gate_result_ok_false(self) -> None:
        issue = SelectorIssue("cat", "chk", "sel", "msg", 0)
        result = SelectorGateResult(
            target="t",
            source="s",
            source_type="inline",
            checks={},
            issues=[issue],
        )
        assert result.ok is False
        assert result.issue_count == 1

    def test_selector_gate_result_to_dict(self) -> None:
        issue = SelectorIssue("cat", "chk", "sel", "msg", 0)
        result = SelectorGateResult(
            target="t",
            source="s",
            source_type="inline",
            checks={"chk": {"count": 1}},
            issues=[issue],
        )
        payload = result.to_dict()
        assert payload["target"] == "t"
        assert payload["ok"] is False
        assert payload["issue_count"] == 1
        assert len(payload["issues"]) == 1

    def test_selector_gate_summary_ok_true(self) -> None:
        r1 = SelectorGateResult("t1", "s1", "inline", {}, [])
        r2 = SelectorGateResult("t2", "s2", "inline", {}, [])
        summary = SelectorGateSummary(targets=[r1, r2])
        assert summary.ok is True
        assert summary.target_count == 2
        assert summary.issue_count == 0

    def test_selector_gate_summary_ok_false(self) -> None:
        issue = SelectorIssue("c", "chk", "sel", "msg", 0)
        r1 = SelectorGateResult("t1", "s1", "inline", {}, [])
        r2 = SelectorGateResult("t2", "s2", "inline", {}, [issue])
        summary = SelectorGateSummary(targets=[r1, r2])
        assert summary.ok is False
        assert summary.issue_count == 1

    def test_selector_gate_summary_to_dict(self) -> None:
        result = SelectorGateResult("t1", "s1", "inline", {}, [])
        summary = SelectorGateSummary(targets=[result], report_path="/tmp/report.json")
        payload = summary.to_dict()
        assert payload["ok"] is True
        assert payload["target_count"] == 1
        assert payload["report_path"] == "/tmp/report.json"
        assert len(payload["targets"]) == 1


class TestEvaluateHtmlTarget:
    def test_max_count_exceeded(self) -> None:
        html = """
        <html><body>
          <ul class="items"><li>A</li><li>B</li><li>C</li></ul>
        </body></html>
        """
        target = SelectorTarget(
            name="t",
            source="inline",
            source_type="inline",
            checks=[
                SelectorCheck(name="items", selector="li", min_count=1, max_count=2),
            ],
        )
        result = evaluate_html_target(target, html)
        assert result.ok is False
        assert any(i.category == "selector_too_many" for i in result.issues)

    def test_required_text_missing(self) -> None:
        html = "<html><body><div class='box'>Hello World</div></body></html>"
        target = SelectorTarget(
            name="t",
            source="inline",
            source_type="inline",
            checks=[
                SelectorCheck(name="box", selector=".box", required_text="MISSING"),
            ],
        )
        result = evaluate_html_target(target, html)
        assert result.ok is False
        assert any(i.category == "text_missing" for i in result.issues)

    def test_required_text_present(self) -> None:
        html = "<html><body><div class='box'>Hello World</div></body></html>"
        target = SelectorTarget(
            name="t",
            source="inline",
            source_type="inline",
            checks=[
                SelectorCheck(name="box", selector=".box", required_text="World"),
            ],
        )
        result = evaluate_html_target(target, html)
        assert result.ok is True

    def test_required_attr_missing(self) -> None:
        html = "<html><body><div class='box'>text</div></body></html>"
        target = SelectorTarget(
            name="t",
            source="inline",
            source_type="inline",
            checks=[
                SelectorCheck(name="box", selector=".box", required_attrs=("data-id",)),
            ],
        )
        result = evaluate_html_target(target, html)
        assert result.ok is False
        assert any(i.category == "attribute_missing" for i in result.issues)

    def test_required_attr_present(self) -> None:
        html = '<html><body><div class="box" data-id="123">text</div></body></html>'
        target = SelectorTarget(
            name="t",
            source="inline",
            source_type="inline",
            checks=[
                SelectorCheck(name="box", selector=".box", required_attrs=("data-id",)),
            ],
        )
        result = evaluate_html_target(target, html)
        assert result.ok is True

    def test_multiple_checks_mixed_issues(self) -> None:
        html = """
        <html><body>
          <table><tbody><tr><td>LG</td></tr></tbody></table>
        </body></html>
        """
        target = SelectorTarget(
            name="t",
            source="inline",
            source_type="inline",
            checks=[
                SelectorCheck(name="rows", selector="tbody tr", min_count=2),
                SelectorCheck(name="team", selector=".team", required_text="LG"),
            ],
        )
        result = evaluate_html_target(target, html)
        assert result.ok is False
        categories = {i.category for i in result.issues}
        assert "selector_missing" in categories

    def test_matched_texts_truncated_to_five(self) -> None:
        html_items = "<ul>" + "".join(f"<li class='x'>Item {i}</li>" for i in range(10)) + "</ul>"
        html = f"<html><body>{html_items}</body></html>"
        target = SelectorTarget(
            name="t",
            source="inline",
            source_type="inline",
            checks=[SelectorCheck(name="items", selector="li.x")],
        )
        result = evaluate_html_target(target, html)
        assert len(result.checks["items"]["matched_text"]) == 5


class TestLoadSelectorConfig:
    def test_load_valid_config(self, tmp_path: Path) -> None:
        html_path = tmp_path / "page.html"
        html_path.write_text("<html></html>", encoding="utf-8")
        config = {
            "targets": [
                {
                    "name": "t1",
                    "source": str(html_path),
                    "source_type": "file",
                    "checks": [{"name": "c1", "selector": "div"}],
                }
            ]
        }
        config_path = tmp_path / "cfg.json"
        config_path.write_text(json.dumps(config), encoding="utf-8")

        targets = load_selector_config(config_path)

        assert len(targets) == 1
        assert targets[0].name == "t1"
        assert targets[0].source_type == "file"

    def test_load_config_missing_targets_key_returns_empty(self, tmp_path: Path) -> None:
        config_path = tmp_path / "cfg.json"
        config_path.write_text(json.dumps({"not_targets": []}), encoding="utf-8")

        targets = load_selector_config(config_path)
        assert targets == []

    def test_load_config_targets_not_list_raises(self, tmp_path: Path) -> None:
        config_path = tmp_path / "cfg.json"
        config_path.write_text(json.dumps({"targets": "not-a-list"}), encoding="utf-8")

        with pytest.raises(TypeError):
            load_selector_config(config_path)


class TestTargetFromDict:
    def test_file_type_relative_path_resolved(self, tmp_path: Path) -> None:
        html_path = tmp_path / "page.html"
        html_path.write_text("<html></html>", encoding="utf-8")
        payload = {
            "name": "t1",
            "source": "page.html",
            "source_type": "file",
        }
        target = _target_from_dict(payload, tmp_path)
        assert Path(target.source).is_absolute()
        assert str(target.source) == str(html_path.resolve())

    def test_file_type_absolute_path_kept(self, tmp_path: Path) -> None:
        abs_path = tmp_path / "page.html"
        abs_path.write_text("<html></html>", encoding="utf-8")
        payload = {
            "name": "t1",
            "source": str(abs_path),
            "source_type": "file",
        }
        target = _target_from_dict(payload, tmp_path)
        assert target.source == str(abs_path)

    def test_defaults(self, tmp_path: Path) -> None:
        payload = {"name": "t1", "source": "http://example.com", "source_type": "url"}
        target = _target_from_dict(payload, tmp_path)
        assert target.wait_until == "networkidle"
        assert target.timeout_ms == 30_000
        assert target.checks == ()

    def test_explicit_values(self, tmp_path: Path) -> None:
        payload = {
            "name": "t1",
            "source": "http://example.com",
            "source_type": "url",
            "wait_until": "domcontentloaded",
            "timeout_ms": 5000,
        }
        target = _target_from_dict(payload, tmp_path)
        assert target.wait_until == "domcontentloaded"
        assert target.timeout_ms == 5000


class TestCheckFromDict:
    def test_string_required_attrs_becomes_tuple(self) -> None:
        payload = {"name": "c1", "selector": "div", "required_attrs": "data-id"}
        check = _check_from_dict(payload)
        assert check.required_attrs == ("data-id",)

    def test_list_required_attrs_stays_tuple(self) -> None:
        payload = {"name": "c1", "selector": "div", "required_attrs": ["a", "b"]}
        check = _check_from_dict(payload)
        assert check.required_attrs == ("a", "b")

    def test_missing_required_attrs_defaults_to_empty(self) -> None:
        payload = {"name": "c1", "selector": "div"}
        check = _check_from_dict(payload)
        assert check.required_attrs == ()

    def test_max_count_none_when_missing(self) -> None:
        payload = {"name": "c1", "selector": "div"}
        check = _check_from_dict(payload)
        assert check.max_count is None

    def test_max_count_explicit_none(self) -> None:
        payload = {"name": "c1", "selector": "div", "max_count": None}
        check = _check_from_dict(payload)
        assert check.max_count is None

    def test_max_count_int_value(self) -> None:
        payload = {"name": "c1", "selector": "div", "max_count": 5}
        check = _check_from_dict(payload)
        assert check.max_count == 5

    def test_min_count_default(self) -> None:
        payload = {"name": "c1", "selector": "div"}
        check = _check_from_dict(payload)
        assert check.min_count == 1


class TestEvaluateTarget:
    def test_inline_source_type(self) -> None:
        target = SelectorTarget(
            name="t",
            source="<html><body><div class='x'>hi</div></body></html>",
            source_type="inline",
            checks=[SelectorCheck(name="x", selector=".x")],
        )
        result = _evaluate_target(target, None)
        assert result.ok is True

    def test_file_source_type(self, tmp_path: Path) -> None:
        html_path = tmp_path / "page.html"
        html_path.write_text("<html><body><div class='x'>hi</div></body></html>", encoding="utf-8")
        target = SelectorTarget(
            name="t",
            source=str(html_path),
            source_type="file",
            checks=[SelectorCheck(name="x", selector=".x")],
        )
        result = _evaluate_target(target, None)
        assert result.ok is True

    def test_unsupported_source_type_raises(self) -> None:
        target = SelectorTarget(
            name="t",
            source="data",
            source_type="database",
            checks=[],
        )
        with pytest.raises(ValueError, match="Unsupported selector target source_type"):
            _evaluate_target(target, None)


class TestRunSelectorGate:
    def test_run_without_output_dir(self) -> None:
        target = SelectorTarget(
            name="t",
            source="<html><body><div class='x'>hi</div></body></html>",
            source_type="inline",
            checks=[SelectorCheck(name="x", selector=".x")],
        )
        summary = run_selector_gate([target])
        assert summary.ok is True
        assert summary.report_path is None

    def test_run_with_output_dir_writes_report(self, tmp_path: Path) -> None:
        target = SelectorTarget(
            name="t",
            source="<html><body><div class='x'>hi</div></body></html>",
            source_type="inline",
            checks=[SelectorCheck(name="x", selector=".x")],
        )
        output_dir = tmp_path / "out"
        summary = run_selector_gate([target], output_dir=output_dir)
        assert summary.ok is True
        assert summary.report_path is not None
        report_file = output_dir / "selector_gate_report.json"
        assert report_file.exists()
        payload = json.loads(report_file.read_text(encoding="utf-8"))
        assert payload["ok"] is True
        assert payload["target_count"] == 1

    def test_run_creates_output_dir_if_missing(self, tmp_path: Path) -> None:
        target = SelectorTarget(
            name="t",
            source="<html><body><div class='x'>hi</div></body></html>",
            source_type="inline",
            checks=[SelectorCheck(name="x", selector=".x")],
        )
        output_dir = tmp_path / "nested" / "deep" / "out"
        summary = run_selector_gate([target], output_dir=output_dir)
        assert output_dir.exists()
        assert summary.ok is True

    def test_run_multiple_targets(self) -> None:
        t1 = SelectorTarget(
            name="t1",
            source="<html><body><div class='x'>hi</div></body></html>",
            source_type="inline",
            checks=[SelectorCheck(name="x", selector=".x")],
        )
        t2 = SelectorTarget(
            name="t2",
            source="<html><body><div class='y'>yo</div></body></html>",
            source_type="inline",
            checks=[SelectorCheck(name="y", selector=".y")],
        )
        summary = run_selector_gate([t1, t2])
        assert summary.ok is True
        assert summary.target_count == 2


class TestRenderSelectorSummary:
    def test_render_pass_no_report(self) -> None:
        result = SelectorGateResult("t1", "s1", "inline", {}, [])
        summary = SelectorGateSummary(targets=[result])
        text = render_selector_summary(summary)
        assert "PASS" in text
        assert "Targets: 1" in text
        assert "Issues: 0" in text

    def test_render_fail_with_issues(self) -> None:
        issue = SelectorIssue("selector_missing", "rows", "tbody tr", "Expected at least 1", 0)
        result = SelectorGateResult("t1", "s1", "inline", {}, [issue])
        summary = SelectorGateSummary(targets=[result], report_path="/tmp/r.json")
        text = render_selector_summary(summary)
        assert "FAIL" in text
        assert "Report: /tmp/r.json" in text
        assert "selector_missing" in text
        assert "tbody tr" in text

    def test_render_multiple_targets(self) -> None:
        r1 = SelectorGateResult("t1", "s1", "inline", {}, [])
        issue = SelectorIssue("c", "chk", "sel", "msg", 0)
        r2 = SelectorGateResult("t2", "s2", "inline", {}, [issue])
        summary = SelectorGateSummary(targets=[r1, r2])
        text = render_selector_summary(summary)
        assert "FAIL" in text
        assert "- t1: PASS" in text
        assert "- t2: FAIL (1 issue(s))" in text


class TestSafeArtifactName:
    def test_simple_name(self) -> None:
        assert _safe_artifact_name("my_target") == "my_target"

    def test_special_chars_replaced(self) -> None:
        assert _safe_artifact_name("my target!@#") == "my_target"

    def test_leading_trailing_underscores_stripped(self) -> None:
        assert _safe_artifact_name("___name___") == "name"

    def test_all_invalid_falls_back(self) -> None:
        assert _safe_artifact_name("!@#$%") == "selector_target"

    def test_dots_and_dashes_preserved(self) -> None:
        assert _safe_artifact_name("v1.0-beta") == "v1.0-beta"
