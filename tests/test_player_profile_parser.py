import pytest

from src.parsers.player_profile_parser import (
    parse_profile,
)


@pytest.mark.parametrize(
    "raw_text,expected",
    [
        (
            (
                "선수명: 가라비토등번호: No.60생년월일: 1995년 08월 19일포지션: 투수(우투우타)"
                "신장/체중: 183cm/100kg경력: 도미니카 Liceo Enedina Puella Renville (고)"
                "입단 계약금: 200000달러연봉: 356666달러지명순위: 25 삼성 자유선발입단년도: 25삼성"
            ),
            {
                "player_name": "가라비토",
                "back_number": 60,
                "birth_date": "1995-08-19",
                "position": "P",
                "throwing_hand": "R",
                "batting_hand": "R",
                "height_cm": 183,
                "weight_kg": 100,
                "education_or_career_path": ["도미니카 Liceo Enedina Puella Renville (고)"],
                "signing_bonus_amount": 200000,
                "signing_bonus_currency": "USD",
                "salary_amount": 356666,
                "salary_currency": "USD",
                "draft_year": 2025,
                "draft_team_code": "SS",
                "draft_type": "자유선발",
                "entry_year": 2025,
                "entry_team_code": "SS",
            },
        ),
        (
            (
                "선수명: 양의지등번호: No.25생년월일: 1987년 06월 05일포지션: 포수(우투우타)"
                "신장/체중: 180cm/95kg경력: 송정동초-무등중-진흥고-두산-경찰-두산-NC"
                "입단 계약금: 3000만원연봉: 160000만원지명순위: 06 두산 2차 8라운드 59순위입단년도: 06두산"
            ),
            {
                "player_name": "양의지",
                "back_number": 25,
                "birth_date": "1987-06-05",
                "position": "C",
                "throwing_hand": "R",
                "batting_hand": "R",
                "height_cm": 180,
                "weight_kg": 95,
                "education_or_career_path": ["송정동초", "무등중", "진흥고", "두산", "경찰", "두산", "NC"],
                "signing_bonus_amount": 3000 * 10_000,
                "signing_bonus_currency": "KRW",
                "salary_amount": 160000 * 10_000,
                "salary_currency": "KRW",
                "draft_year": 2006,
                "draft_team_code": "OB",
                "draft_round": 8,
                "draft_pick_overall": 59,
                "draft_type": "2차",
                "entry_year": 2006,
                "entry_team_code": "OB",
            },
        ),
        (
            (
                "선수명: 강동우생년월일: 1974년 04월 20일출신교: 칠성초-경상중-경북고-단국대-삼성-두산-KIA-한화"
                "지명순위: 98 삼성 1차"
            ),
            {
                "player_name": "강동우",
                "birth_date": "1974-04-20",
                "education_or_career_path": [
                    "칠성초",
                    "경상중",
                    "경북고",
                    "단국대",
                    "삼성",
                    "두산",
                    "KIA",
                    "한화",
                ],
                "draft_year": 1998,
                "draft_team_code": "SS",
                "draft_type": "1차",
            },
        ),
    ],
)
def test_parse_profile_examples(raw_text, expected):
    parsed = parse_profile(raw_text)
    for key, value in expected.items():
        assert getattr(parsed, key) == value

