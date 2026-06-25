"""
Integration tests for parking_parser with realistic fixture HTML.
"""

from __future__ import annotations

from pathlib import Path

FIXTURE_DIR = Path(__file__).resolve().parent.parent / "fixtures" / "html"

from src.parsers.parking_parser import parse_parking


def test_parking_parses_fees():
    html = """<html><body>
        <h2>주차 요금 안내</h2>
        <table class="fee_table">
            <tr><td>기본요금</td><td>5,000원</td></tr>
            <tr><td>추가요금</td><td>3,000원</td></tr>
            <tr><td>경기일 주차</td><td>10,000원</td></tr>
        </table>
        <div class="info">기본요금: 5,000원 추가 3,000원 행사 10,000원</div>
    </body></html>"""
    result = parse_parking(html, "ssg_landers_parking")
    assert len(result) == 1
    fees = result[0]["fee_rules"]
    assert len(fees) >= 2
    amounts = {f["label"]: f["amount"] for f in fees}
    assert amounts.get("기본") == 5000
    assert amounts.get("추가") == 3000


def test_parking_schema():
    html = "<html><body>기본요금: 5,000원</body></html>"
    result = parse_parking(html, "daegu_parking")
    assert result[0]["lot"]["stadium_id"] == "DAEGU"
    assert result[0]["lot"]["lot_type"] == "official"
