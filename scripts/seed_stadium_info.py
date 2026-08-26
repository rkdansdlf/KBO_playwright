import argparse
import logging
import os
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from src.repositories.stadium_info_repository import StadiumInfoRepository


def SessionLocal() -> Session:
    target_url = os.environ.get("DATABASE_URL") or "sqlite:///./data/kbo_dev.db"
    try:
        engine = create_engine(target_url)
        factory = sessionmaker(bind=engine)
        return factory()
    except (SQLAlchemyError, RuntimeError, OSError, ValueError, TypeError):
        engine = create_engine("sqlite:///./data/kbo_dev.db")
        factory = sessionmaker(bind=engine)
        return factory()


STADIUM_DATA = [
    {
        "stadium_code": "JAMSIL",
        "name_kr": "잠실야구장",
        "name_en": "Jamsil Baseball Stadium",
        "home_team_id": "LG",
        "capacity": 23750,
        "opened_year": 1982,
        "location": "서울특별시 송파구",
        "address": "서울특별시 송파구 올림픽로 25",
        "parking_info": "잠실종합운동장 주차장 이용 (유료). 주말 경기 시 대중교통 권장.",
        "public_transport": {
            "subway": ["잠실역 (2호선, 8호선)", "종합운동장역 (2호선)", "석촌역 (9호선)"],
            "bus": ["301", "340", "360", "362"],
        },
        "facilities": {"restaurant": True, "shop": True, "disabled_access": True, "kids_zone": True},
        "latitude": 37.5114,
        "longitude": 127.0734,
        "is_dome": False,
        "is_active": True,
    },
    {
        "stadium_code": "MUNHAK",
        "name_kr": "인천SSG랜더스필드",
        "name_en": "Incheon SSG Landers Field",
        "home_team_id": "SSG",
        "capacity": 23600,
        "opened_year": 2002,
        "location": "인천광역시 미추홀구",
        "address": "인천광역시 미추홀구 매소홀로 618",
        "parking_info": "구장 내 주차장 2,000대 가능. 경기일 혼잡.",
        "public_transport": {"subway": ["문학경기장역 (인천1호선)"], "bus": ["6", "8", "16", "27"]},
        "facilities": {"restaurant": True, "shop": True, "disabled_access": True, "kids_zone": True},
        "latitude": 37.4351,
        "longitude": 126.6908,
        "is_dome": False,
        "is_active": True,
    },
    {
        "stadium_code": "GOCHEOK",
        "name_kr": "고척스카이돔",
        "name_en": "Gocheok Sky Dome",
        "home_team_id": "KH",
        "capacity": 16700,
        "opened_year": 2015,
        "location": "서울특별시 구로구",
        "address": "서울특별시 구로구 경인로 430",
        "parking_info": "주차 공간 협소. 대중교통 이용 강력 권장.",
        "public_transport": {"subway": ["고척역 (1호선)"], "bus": ["5", "10", "51", "150"]},
        "facilities": {"restaurant": True, "shop": True, "disabled_access": True, "kids_zone": False},
        "latitude": 37.4981,
        "longitude": 126.8670,
        "is_dome": True,
        "is_active": True,
    },
    {
        "stadium_code": "SUWON",
        "name_kr": "수원 kt wiz 파크",
        "name_en": "Suwon KT Wiz Park",
        "home_team_id": "KT",
        "capacity": 20600,
        "opened_year": 2014,
        "location": "경기도 수원시 장안구",
        "address": "경기도 수원시 장안구 경수대로 893",
        "parking_info": "수원종합운동장 주차장 이용.",
        "public_transport": {"subway": ["수원역 (1호선)", "화서역 (1호선)"], "bus": ["2", "7", "11", "30"]},
        "facilities": {"restaurant": True, "shop": True, "disabled_access": True, "kids_zone": True},
        "latitude": 37.2990,
        "longitude": 126.9990,
        "is_dome": False,
        "is_active": True,
    },
    {
        "stadium_code": "SAJIK",
        "name_kr": "부산 사직 야구장",
        "name_en": "Busan Sajik Baseball Stadium",
        "home_team_id": "LT",
        "capacity": 26700,
        "opened_year": 1985,
        "location": "부산광역시 동래구",
        "address": "부산광역시 동래구 사직로 45",
        "parking_info": "사직종합운동장 주차장 이용.",
        "public_transport": {"subway": ["사직역 (3호선)", "종합운동장역 (3호선)"], "bus": ["10", "20", "30", "40"]},
        "facilities": {"restaurant": True, "shop": True, "disabled_access": True, "kids_zone": False},
        "latitude": 35.1942,
        "longitude": 129.0618,
        "is_dome": False,
        "is_active": True,
    },
    {
        "stadium_code": "DAEGU",
        "name_kr": "대구 삼성 라이온즈 파크",
        "name_en": "Daegu Samsung Lions Park",
        "home_team_id": "SS",
        "capacity": 24000,
        "opened_year": 2016,
        "location": "대구광역시 수성구",
        "address": "대구광역시 수성구 야구전설로 1",
        "parking_info": "구장 내 주차장 1,300대. 대중교통 권장.",
        "public_transport": {"subway": ["대공원역 (3호선)"], "bus": ["수성1", "수성2", "105", "131"]},
        "facilities": {"restaurant": True, "shop": True, "disabled_access": True, "kids_zone": True},
        "latitude": 35.8261,
        "longitude": 128.6789,
        "is_dome": False,
        "is_active": True,
    },
    {
        "stadium_code": "CHANGWON",
        "name_kr": "창원NC파크",
        "name_en": "Changwon NC Park",
        "home_team_id": "NC",
        "capacity": 22000,
        "opened_year": 2019,
        "location": "경상남도 창원시 마산회원구",
        "address": "경상남도 창원시 마산회원구 삼호로 77",
        "parking_info": "구장 전용 주차장 1,500대.",
        "public_transport": {"subway": [], "bus": ["11", "12", "13", "50", "60"]},
        "facilities": {"restaurant": True, "shop": True, "disabled_access": True, "kids_zone": True},
        "latitude": 35.2222,
        "longitude": 128.5673,
        "is_dome": False,
        "is_active": True,
    },
    {
        "stadium_code": "GWANGJU",
        "name_kr": "광주-기아 챔피언스 필드",
        "name_en": "Gwangju Kia Champions Field",
        "home_team_id": "KIA",
        "capacity": 20500,
        "opened_year": 2014,
        "location": "광주광역시 북구",
        "address": "광주광역시 북구 서양로 50",
        "parking_info": "구장 내 주차장 1,000대. 경기일 혼잡.",
        "public_transport": {"subway": ["송정공원역 (1호선)"], "bus": ["금빛1", "금빛2", "15", "20"]},
        "facilities": {"restaurant": True, "shop": True, "disabled_access": True, "kids_zone": True},
        "latitude": 35.1683,
        "longitude": 126.8860,
        "is_dome": False,
        "is_active": True,
    },
    {
        "stadium_code": "HANBAT",
        "name_kr": "대전 한화생명 이글스 파크",
        "name_en": "Daejeon Hanwha Life Eagles Park",
        "home_team_id": "HH",
        "capacity": 20000,
        "opened_year": 2025,
        "location": "대전광역시 중구",
        "address": "대전광역시 중구 대종로 373",
        "parking_info": "대전종합운동장 주차장 이용.",
        "public_transport": {"subway": ["대전역 (1호선)", "중구청역 (1호선)"], "bus": ["1", "2", "3", "6"]},
        "facilities": {"restaurant": True, "shop": True, "disabled_access": True, "kids_zone": True},
        "latitude": 36.3165,
        "longitude": 127.4290,
        "is_dome": False,
        "is_active": True,
    },
    {
        "stadium_code": "MUDEUNG",
        "name_kr": "광주 무등경기장 야구장",
        "name_en": "Gwangju Mudeung Baseball Stadium",
        "home_team_id": "HT",
        "capacity": 12500,
        "opened_year": 1965,
        "location": "광주광역시 북구",
        "address": "광주광역시 북구 임동 90",
        "parking_info": "광주무등경기장 주차장.",
        "public_transport": {"subway": ["양동시장역 (1호선)"], "bus": ["16", "26", "51"]},
        "facilities": {"restaurant": False, "shop": True, "disabled_access": True, "kids_zone": False},
        "latitude": 35.1661,
        "longitude": 126.8887,
        "is_dome": False,
        "is_active": False,
    },
    {
        "stadium_code": "DONGDAEMUN",
        "name_kr": "동대문야구장",
        "name_en": "Dongdaemun Baseball Stadium",
        "home_team_id": "MBC",
        "capacity": 25000,
        "opened_year": 1959,
        "location": "서울특별시 중구",
        "address": "서울특별시 중구 을지로7가 2",
        "parking_info": "동대문운동장 주차장 (철거).",
        "public_transport": {"subway": ["동대문역사문화공원역 (2, 4, 5호선)"], "bus": []},
        "facilities": {"restaurant": False, "shop": False, "disabled_access": False, "kids_zone": False},
        "latitude": 37.5678,
        "longitude": 127.0094,
        "is_dome": False,
        "is_active": False,
    },
    {
        "stadium_code": "JEONJU",
        "name_kr": "전주종합운동장 야구장",
        "name_en": "Jeonju Baseball Stadium",
        "home_team_id": "SL",
        "capacity": 10000,
        "opened_year": 1977,
        "location": "전북특별자치도 전주시",
        "address": "전북특별자치도 전주시 덕진구 기린대로 451",
        "parking_info": "전주종합운동장 주차장.",
        "public_transport": {"subway": [], "bus": ["104", "165", "380"]},
        "facilities": {"restaurant": False, "shop": False, "disabled_access": False, "kids_zone": False},
        "latitude": 35.8407,
        "longitude": 127.1309,
        "is_dome": False,
        "is_active": False,
    },
    {
        "stadium_code": "MASAN",
        "name_kr": "마산야구장",
        "name_en": "Masan Baseball Stadium",
        "home_team_id": "NC",
        "capacity": 11000,
        "opened_year": 1982,
        "location": "경상남도 창원시 마산회원구",
        "address": "경상남도 창원시 마산회원구 삼호로 63",
        "parking_info": "마산종합운동장 주차장.",
        "public_transport": {"subway": [], "bus": ["100", "101", "108"]},
        "facilities": {"restaurant": True, "shop": True, "disabled_access": True, "kids_zone": False},
        "latitude": 35.2227,
        "longitude": 128.5824,
        "is_dome": False,
        "is_active": True,
    },
    {
        "stadium_code": "CHEONGJU",
        "name_kr": "청주종합운동장 야구장",
        "name_en": "Cheongju Baseball Stadium",
        "home_team_id": "HH",
        "capacity": 10500,
        "opened_year": 1979,
        "location": "충청북도 청주시 서원구",
        "address": "충청북도 청주시 서원구 사직대로 229",
        "parking_info": "청주예술의전당/체육관 주차장.",
        "public_transport": {"subway": [], "bus": ["30-1", "30-2", "407"]},
        "facilities": {"restaurant": False, "shop": True, "disabled_access": True, "kids_zone": False},
        "latitude": 36.6397,
        "longitude": 127.4728,
        "is_dome": False,
        "is_active": True,
    },
    {
        "stadium_code": "GUNSAN",
        "name_kr": "군산월명종합운동장 야구장",
        "name_en": "Gunsan Wolmyeong Baseball Stadium",
        "home_team_id": "KIA",
        "capacity": 10000,
        "opened_year": 1989,
        "location": "전북특별자치도 군산시",
        "address": "전북특별자치도 군산시 번영로 281",
        "parking_info": "월명종합경기장 주차장.",
        "public_transport": {"subway": [], "bus": ["1", "2", "3", "7", "8"]},
        "facilities": {"restaurant": False, "shop": False, "disabled_access": True, "kids_zone": False},
        "latitude": 35.9752,
        "longitude": 126.7369,
        "is_dome": False,
        "is_active": True,
    },
    {
        "stadium_code": "CHUNCHEON",
        "name_kr": "춘천야구장",
        "name_en": "Chuncheon Baseball Stadium",
        "home_team_id": "SM",
        "capacity": 7000,
        "opened_year": 1982,
        "location": "강원특별자치도 춘천시",
        "address": "강원특별자치도 춘천시 송암길 25",
        "parking_info": "송암스포츠타운 주차장.",
        "public_transport": {"subway": [], "bus": ["16", "100"]},
        "facilities": {"restaurant": False, "shop": False, "disabled_access": True, "kids_zone": False},
        "latitude": 37.8541,
        "longitude": 127.6908,
        "is_dome": False,
        "is_active": False,
    },
    {
        "stadium_code": "JEJU",
        "name_kr": "제주 오라야구장",
        "name_en": "Jeju Ora Baseball Stadium",
        "home_team_id": "OB",
        "capacity": 8500,
        "opened_year": 1984,
        "location": "제주특별자치도 제주시",
        "address": "제주특별자치도 제주시 오라1동 1163-4",
        "parking_info": "제주종합경기장 주차장.",
        "public_transport": {"subway": [], "bus": ["311", "312", "325"]},
        "facilities": {"restaurant": False, "shop": False, "disabled_access": True, "kids_zone": False},
        "latitude": 33.4988,
        "longitude": 126.5165,
        "is_dome": False,
        "is_active": False,
    },
    {
        "stadium_code": "SIMIN",
        "name_kr": "대구시민운동장 야구장",
        "name_en": "Daegu Civic Baseball Stadium",
        "home_team_id": "SS",
        "capacity": 10000,
        "opened_year": 1948,
        "location": "대구광역시 북구",
        "address": "대구광역시 북구 고성로 191",
        "parking_info": "대구시민운동장 주차장.",
        "public_transport": {"subway": ["북구청역 (3호선)"], "bus": ["300", "323", "523"]},
        "facilities": {"restaurant": False, "shop": True, "disabled_access": True, "kids_zone": False},
        "latitude": 35.8812,
        "longitude": 128.5878,
        "is_dome": False,
        "is_active": False,
    },
    {
        "stadium_code": "MOKDONG",
        "name_kr": "목동야구장",
        "name_en": "Mokdong Baseball Stadium",
        "home_team_id": "WO",
        "capacity": 10500,
        "opened_year": 1989,
        "location": "서울특별시 양천구",
        "address": "서울특별시 양천구 안양천로 939",
        "parking_info": "목동종합운동장 주차장.",
        "public_transport": {"subway": ["오목교역 (5호선)"], "bus": ["571", "603", "6624"]},
        "facilities": {"restaurant": True, "shop": True, "disabled_access": True, "kids_zone": False},
        "latitude": 37.5303,
        "longitude": 126.8821,
        "is_dome": False,
        "is_active": True,
    },
    {
        "stadium_code": "POHANG",
        "name_kr": "포항야구장",
        "name_en": "Pohang Baseball Stadium",
        "home_team_id": "SS",
        "capacity": 12000,
        "opened_year": 2012,
        "location": "경상북도 포항시 남구",
        "address": "경상북도 포항시 남구 희망대로 790",
        "parking_info": "포항종합운동장 주차장.",
        "public_transport": {"subway": [], "bus": ["107", "209", "308"]},
        "facilities": {"restaurant": False, "shop": True, "disabled_access": True, "kids_zone": False},
        "latitude": 35.9986,
        "longitude": 129.3582,
        "is_dome": False,
        "is_active": True,
    },
    {
        "stadium_code": "ULSAN",
        "name_kr": "울산문수야구장",
        "name_en": "Ulsan Munsu Baseball Stadium",
        "home_team_id": "LT",
        "capacity": 12000,
        "opened_year": 2014,
        "location": "울산광역시 남구",
        "address": "울산광역시 남구 문수로 44",
        "parking_info": "문수체육공원 주차장.",
        "public_transport": {"subway": [], "bus": ["106", "407", "714"]},
        "facilities": {"restaurant": False, "shop": True, "disabled_access": True, "kids_zone": False},
        "latitude": 35.5342,
        "longitude": 129.2625,
        "is_dome": False,
        "is_active": True,
    },
]


REGULATION_DATA = [
    {
        "stadium_code": "GOCHEOK",
        "regulation_type": "GROUND_RULE",
        "title": "돔구장 비/천장 관련 규정",
        "description": "고척스카이돔은 돔구장으로 우천으로 인한 경기 취소/지연 없음. 다만 천장 구조물에 맞은 타구는 인플레이.",
        "source": "KBO 공식 규정",
    },
    {
        "stadium_code": "GOCHEOK",
        "regulation_type": "GROUND_RULE",
        "title": "펜스 높이 및 거리",
        "description": "좌우 99m, 중앙 118m, 펜스 높이 3.8m. 돔 천장 최고 높이 65m.",
        "source": "KBO 공식 규정",
    },
    {
        "stadium_code": "JAMSIL",
        "regulation_type": "GROUND_RULE",
        "title": "잠실야구장 펜스 규정",
        "description": "좌우 100m, 중앙 125m, 펜스 높이 2.6m. 중앙 담장이 좌우보다 높음.",
        "source": "KBO 공식 규정",
    },
    {
        "stadium_code": "DAEGU",
        "regulation_type": "GROUND_RULE",
        "title": "라이온즈 파크 외야 규정",
        "description": "좌우 99m, 중앙 122m. 펜스 높이 좌우 3.2m, 중앙 3.8m. 중앙 담장에 '라이온즈' 로고가 있어 심판 판정 참고.",
        "source": "KBO 공식 규정",
    },
    {
        "stadium_code": "JAMSIL",
        "regulation_type": "ADVERTISING",
        "title": "LG-두산 공동 사용 구장 광고 규정",
        "description": "잠실야구장은 LG 트윈스와 두산 베어스가 공동 홈구장으로 사용. 외야 펜스 광고는 구단별로 지정된 공간에 한정. 홈경기 구단이 중앙 광고 권한 보유.",
        "source": "KBO 구장 공동 사용 협정",
    },
    {
        "stadium_code": "JAMSIL",
        "regulation_type": "DUGOUT",
        "title": "잠실 더그아웃 배정",
        "description": "1루 측: 홈팀 (LG/두산), 3루 측: 원정팀. 단, 공동 사용 구장의 특성상 홈팀 우선 배정.",
        "source": "KBO 경기 운영 규정",
    },
]


def seed_stadium_info(session: Session | None = None, db_url: str | None = None) -> int:
    if session is None:
        if db_url:
            engine = create_engine(db_url)
            session = sessionmaker(bind=engine)()
        else:
            session = SessionLocal()

    repo = StadiumInfoRepository(session)
    count = 0
    try:
        for data in STADIUM_DATA:
            repo.save_stadium_info(data)
            count += 1
        for reg in REGULATION_DATA:
            repo.save_regulation(reg)
        session.commit()
        logger.info("Seeded %s stadiums and %s regulations.", count, len(REGULATION_DATA))
        return count

    except (SQLAlchemyError, RuntimeError, ValueError, TypeError):
        session.rollback()
        logger.exception("Error seeding stadium info")
        raise
    finally:
        session.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Seed stadium info and regulations.")
    parser.add_argument("--db-url", type=str, default=None, help="Target database URL")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO)
    seed_stadium_info(db_url=args.db_url)
    return 0


if __name__ == "__main__":
    sys.exit(main())
