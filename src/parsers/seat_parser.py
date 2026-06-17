"""
Parser for stadium seat section information pages. Extracts section names and grades.
"""

from __future__ import annotations

import re
from typing import Any

from src.parsers.base_parser import BaseStadiumParser

SECTION_PATTERNS = [
    re.compile(r"([가-힣]+(?:석|존|zone|Zone))"),
    re.compile(r"(블루|오렌지|레드|네이비|그린|화이트|골드|[1-3][Ff])\s*(.*?)(?:석|존)"),
]

STADIUM_FROM_SOURCE_KEY = {
    "lg_twins_seat": "JAMSIL",
    "seoul_stadium_seat": "JAMSIL",
}


class SeatSectionParser(BaseStadiumParser):
    SOURCE_KEY_MAP = STADIUM_FROM_SOURCE_KEY

    def parse(self) -> list[dict[str, Any]]:
        stadium_id = self.SOURCE_KEY_MAP.get(self.source_key, "UNKNOWN")
        sections = []
        seen = set()

        for pattern in SECTION_PATTERNS:
            for match in pattern.finditer(self.text):
                name = match.group(0).strip()
                if name in seen or len(name) < 2:
                    continue
                seen.add(name)
                sections.append(
                    {
                        "stadium_id": stadium_id,
                        "section_name": name,
                        "section_code": name,
                        "seat_grade": name,
                        "source_id": None,
                    },
                )

        return sections


def parse_seat_sections(html: str, source_key: str, metadata: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    return SeatSectionParser(html, source_key, metadata).parse()


if __name__ == "__main__":
    import sys

    html = sys.stdin.read() if not sys.stdin.isatty() else "<html><body><p>블루석 오렌지석 레드석</p></body></html>"
    result = parse_seat_sections(html, "lg_twins_seat")
    for item in result:
        print(item)  # noqa: T201
