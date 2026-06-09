"""
Parser for stadium parking information pages. Extracts lot names and fee rules.
"""

from __future__ import annotations

import re
from typing import Any

from src.parsers.base_parser import BaseStadiumParser

PARKING_FEE_PATTERN = re.compile(
    r"(기본|추가|일일|행사|경기|무료)\s*(?:요금|시간|금액)?\s*:?\s*(\d{1,3}(?:,\d{3})*)\s*(?:원)"
)

STADIUM_FROM_SOURCE_KEY = {
    "ssg_landers_parking": "MUNHAK",
    "daegu_parking": "DAEGU",
    "jamsil_parking_official": "JAMSIL",
}


class ParkingParser(BaseStadiumParser):
    SOURCE_KEY_MAP = STADIUM_FROM_SOURCE_KEY

    def parse(self) -> list[dict[str, Any]]:
        stadium_id = self.SOURCE_KEY_MAP.get(self.source_key, "UNKNOWN")
        lots = []
        fees = []
        for match in PARKING_FEE_PATTERN.finditer(self.text):
            label, amount = match.group(1), match.group(2).replace(",", "")
            fees.append({"label": label, "amount": int(amount)})

        lot_name = f"{stadium_id} 주차장"
        lots.append(
            {
                "lot": {
                    "stadium_id": stadium_id,
                    "name": lot_name,
                    "lot_type": "official",
                    "is_event_day_available": True,
                    "reservation_required": False,
                },
                "fee_rules": fees,
            }
        )

        return lots


def parse_parking(html: str, source_key: str, metadata: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    return ParkingParser(html, source_key, metadata).parse()


if __name__ == "__main__":
    import sys

    html = sys.stdin.read() if not sys.stdin.isatty() else "<html><body><p>기본요금: 5,000원</p></body></html>"
    result = parse_parking(html, "ssg_landers_parking")
    for item in result:
        print(item)
