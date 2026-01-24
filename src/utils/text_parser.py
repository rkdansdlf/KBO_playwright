"""
Text Parser for KBO Relay.
Extracts:
- Outs (from "1사", "2사", etc.)
- Runners (from "1루", "1,2루", "만루")
- Score Changes (from "1점 득점", "홈런")
"""
import re

class KBOTextParser:
    @staticmethod
    def parse_runners(text: str) -> int:
        """
        Parses runner state bitmask from text.
        0=Empty, 1=1B, 2=2B, 4=3B
        Combinations: 3=1,2B, 5=1,3B, 6=2,3B, 7=Full
        
        Typical text: "1사 1,2루", "무사 만루", "2사 3루"
        This usually appears in the PRE-STATE description or result text.
        """
        if "만루" in text:
            return 7
        
        runners = 0
        if "1루" in text:
            runners |= 1
        if "2루" in text:
            runners |= 2
        if "3루" in text:
            runners |= 4
            
        return runners

    @staticmethod
    def parse_outs(text: str) -> int:
        """Parses out count 0, 1, 2."""
        if "2사" in text or "투아웃" in text:
            return 2
        if "1사" in text or "원아웃" in text:
            return 1
        if "무사" in text or "노아웃" in text:
            return 0
        return 0 # Default/Fallback

    @staticmethod
    def parse_score_change(text: str) -> int:
        """
        Parses runs scored from event description.
        e.g. "좌월 1점 홈런", "1타점 적시타", "밀어내기 볼넷 (1점 득점)"
        """
        # Explicit score mention
        match = re.search(r'(\d+)점\s*(?:홈런|득점)', text)
        if match:
            return int(match.group(1))
            
        # Solo HR without "1점" explicit sometimes? 
        # Usually KBO text says "XXX 1점 홈런" or "XXX 솔로 홈런"
        if "솔로 홈런" in text:
            return 1
        if "투런 홈런" in text or "2점 홈런" in text:
            return 2
        if "쓰리런 홈런" in text or "3점 홈런" in text:
            return 3
        if "만루 홈런" in text:
            return 4
            
        return 0
