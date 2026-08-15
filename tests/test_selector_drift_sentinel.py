"""Unit tests for SelectorDriftSentinel."""

from __future__ import annotations

from src.monitoring.selector_drift_sentinel import (
    PageContract,
    SelectorDriftSentinel,
    create_default_kbo_sentinel,
)


def test_sentinel_healthy_html() -> None:
    """Healthy HTML containing all required selectors should pass."""
    sentinel = SelectorDriftSentinel()
    sentinel.register_contract(
        PageContract(
            page_name="test_page",
            required_selectors=(".box-score-area", "#tblAwayHitter1"),
            min_table_columns={"#tblAwayHitter1": 3},
            expected_text_snippets=("타자 기록",),
        ),
    )

    html = """
    <html>
        <body>
            <div class="box-score-area">
                <h2>타자 기록</h2>
                <table id="tblAwayHitter1">
                    <tr><th>이름</th><th>타수</th><th>안타</th></tr>
                </table>
            </div>
        </body>
    </html>
    """
    report = sentinel.check_html("test_page", html)
    assert report.is_healthy is True
    assert report.drift_detected is False
    assert len(report.missing_selectors) == 0


def test_sentinel_detects_missing_selector() -> None:
    """DOM mutation missing critical selector should report drift."""
    sentinel = SelectorDriftSentinel()
    sentinel.register_contract(
        PageContract(
            page_name="test_page",
            required_selectors=(".new-box-score-layout",),
        ),
    )

    html = "<html><body><div class='old-layout'></div></body></html>"
    report = sentinel.check_html("test_page", html)
    assert report.is_healthy is False
    assert report.drift_detected is True
    assert ".new-box-score-layout" in report.missing_selectors


def test_sentinel_detects_column_shrinkage() -> None:
    """Table having fewer columns than contract should report drift."""
    sentinel = SelectorDriftSentinel()
    sentinel.register_contract(
        PageContract(
            page_name="test_table",
            min_table_columns={"#stats_table": 5},
        ),
    )

    html = """
    <html>
        <body>
            <table id="stats_table">
                <tr><th>이름</th><th>안타</th></tr>
            </table>
        </body>
    </html>
    """
    report = sentinel.check_html("test_table", html)
    assert report.is_healthy is False
    assert len(report.mismatched_columns) == 1
    assert "expected >= 5" in report.mismatched_columns[0]


def test_default_kbo_sentinel() -> None:
    """Default sentinel should have standard page contracts pre-registered."""
    sentinel = create_default_kbo_sentinel()
    report = sentinel.check_html("schedule", "<html><body><table class='tbl-type06'></table></body></html>")
    assert report.is_healthy is True
