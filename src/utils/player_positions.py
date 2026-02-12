"""
Utility to standardize KBO player positions from raw strings (Korean/Hanja) to standard codes.
"""
from __future__ import annotations

from enum import Enum
from typing import List, Optional, Set


class PositionCode(str, Enum):
    P = "P"    # Pitcher (투)
    C = "C"    # Catcher (포)
    B1 = "1B"  # 1st Base (一)
    B2 = "2B"  # 2nd Base (二)
    B3 = "3B"  # 3rd Base (三)
    SS = "SS"  # Shortstop (유)
    LF = "LF"  # Left Fielder (좌)
    CF = "CF"  # Center Fielder (중)
    RF = "RF"  # Right Fielder (우)
    DH = "DH"  # Designated Hitter (지)
    PH = "PH"  # Pinch Hitter (타)
    PR = "PR"  # Pinch Runner (주)
    UNKNOWN = "UNKNOWN"


# Mapping from raw KBO characters to PositionCode
RAW_MAP = {
    "투": PositionCode.P,
    "포": PositionCode.C,
    "一": PositionCode.B1,
    "二": PositionCode.B2,
    "三": PositionCode.B3,
    "유": PositionCode.SS,
    "좌": PositionCode.LF,
    "중": PositionCode.CF,
    "우": PositionCode.RF,
    "지": PositionCode.DH,
    "타": PositionCode.PH,
    "주": PositionCode.PR,
    # Additional variations
    "1": PositionCode.B1,
    "2": PositionCode.B2,
    "3": PositionCode.B3,
    "4": PositionCode.B2,
    "5": PositionCode.B3,
    "6": PositionCode.SS,
    "7": PositionCode.LF,
    "8": PositionCode.CF,
    "9": PositionCode.RF,
}


def normalize_position(raw_pos: Optional[str]) -> List[PositionCode]:
    """
    Normalizes a KBO position string into a list of PositionCode.
    Handles composite strings like '타一', '주二', '유三'.
    
    Examples:
        '타一' -> [PH, 1B]
        '주二' -> [PR, 2B]
        '유三' -> [SS, 3B]
        '중'   -> [CF]
    """
    if not raw_pos:
        return []

    raw_pos = raw_pos.strip()
    if not raw_pos or raw_pos == "-":
        return []

    # Handle characters one by one or in recognized chunks
    codes: List[PositionCode] = []
    
    # We iterate through the string and check if each character or pair is in RAW_MAP
    # KBO position strings are usually short (1-3 chars).
    i = 0
    while i < len(raw_pos):
        char = raw_pos[i]
        
        # Check if it's a known mapping
        if char in RAW_MAP:
            codes.append(RAW_MAP[char])
        else:
            # If not in map, but we want to be resilient
            pass
            
        i += 1

    if not codes and raw_pos:
        return [PositionCode.UNKNOWN]

    return codes


def get_primary_position(raw_pos: Optional[str]) -> PositionCode:
    """
    Returns the most 'final' position in a sequence.
    Example: '타一' -> 1B (since they entered as PH but played 1B)
             '유三' -> 3B (moved from SS to 3B)
             '주'   -> PR
    """
    codes = normalize_position(raw_pos)
    if not codes:
        return PositionCode.UNKNOWN
    
    # Heuristic: the LAST position in the string usually represents the final state
    # UNLESS it's just '타' or '주' followed by nothing else in the map.
    return codes[-1]


def is_infield(pos: PositionCode) -> bool:
    return pos in {PositionCode.B1, PositionCode.B2, PositionCode.B3, PositionCode.SS}


def is_outfield(pos: PositionCode) -> bool:
    return pos in {PositionCode.LF, PositionCode.CF, PositionCode.RF}

def is_battery(pos: PositionCode) -> bool:
    return pos in {PositionCode.P, PositionCode.C}
