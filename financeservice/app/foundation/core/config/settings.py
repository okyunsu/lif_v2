import os
from dotenv import load_dotenv
from typing import Dict, Any

# 환경 변수 로드
load_dotenv()

class Settings:
    """애플리케이션 설정 클래스"""
    
    # 데이터베이스 설정
    DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:password@localhost:5432/findb")
    
    # DART API 설정
    DART_API_KEY: str = os.getenv("DART_API_KEY", "")
    DART_API_URL: str = "https://opendart.fss.or.kr/api"
    
    # DART API 엔드포인트
    DART_ENDPOINTS: Dict[str, str] = {
        "CORP_CODE": f"{DART_API_URL}/corpCode.xml",
        "FINANCIAL_STATEMENT": f"{DART_API_URL}/fnlttSinglAcnt.json",
        "REPORT_LIST": f"{DART_API_URL}/list.json"
    }
    
    # 기타 설정
    DEBUG: bool = os.getenv("DEBUG", "False").lower() in ("true", "1", "t")
    
    @classmethod
    def validate(cls) -> None:
        """필수 환경 변수가 설정되어 있는지 검증합니다."""
        if not cls.DART_API_KEY:
            raise ValueError("DART API 키가 필요합니다. 환경 변수 DART_API_KEY를 설정하세요.")

# 설정 인스턴스 생성
settings = Settings()

# 애플리케이션 시작 시 설정 검증
try:
    settings.validate()
except ValueError as e:
    import logging
    logging.error(f"설정 검증 실패: {str(e)}")
    # 애플리케이션 실행에 필수적인 설정이 없으면 여기서 종료할 수도 있음
    # import sys
    # sys.exit(1) 