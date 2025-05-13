import os
import logging
from typing import Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from dotenv import load_dotenv

from app.domain.service.company_info_service import CompanyInfoService
from app.domain.service.financial_statement_service import FinancialStatementService
from app.domain.model.schema.company_schema import CompanySchema

# 로깅 설정
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# 핸들러가 없으면 추가
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(handler)

class FinService:
    """
    재무 정보 서비스 파사드 클래스.
    
    클라이언트가 다양한 재무 관련 서비스에 쉽게 접근할 수 있도록 단일 인터페이스를 제공합니다.
    내부적으로 각 책임에 맞는 서비스에 작업을 위임합니다.
    
    위임 대상:
    - CompanyInfoService: 회사 정보 관련 기능
    - FinancialStatementService: 재무제표 관련 기능
    """
    def __init__(self, db_session: AsyncSession):
        """서비스 초기화"""
        # 환경 변수 로드
        load_dotenv()
        self.api_key = os.getenv("DART_API_KEY")
        if not self.api_key:
            logger.error("DART API 키가 설정되지 않았습니다.")
            raise ValueError("DART API 키가 필요합니다. 환경 변수 DART_API_KEY를 설정하세요.")
            
        # 서비스 초기화
        self.db_session = db_session
        self.company_service = CompanyInfoService(db_session)
        self.statement_service = FinancialStatementService(db_session)
        
        logger.info("FinService가 초기화되었습니다.")

    async def get_company_info(self, company_name: str) -> CompanySchema:
        """
        회사 정보를 조회합니다.
        
        Args:
            company_name: 회사명
            
        Returns:
            CompanySchema: 회사 정보 객체
        """
        logger.info(f"회사 정보 조회 요청: {company_name}")
        return await self.company_service.get_company_info(company_name)

    async def get_financial_statements(self, company_name: str, year: Optional[int] = None) -> Dict[str, Any]:
        """
        재무제표 데이터를 조회합니다.
        
        Args:
            company_name: 회사명
            year: 조회할 연도 (None인 경우 최근 연도)
            
        Returns:
            Dict: 포맷팅된 재무제표 데이터
        """
        logger.info(f"재무제표 조회 요청 - 회사: {company_name}, 연도: {year}")
        try:
            return await self.statement_service.get_formatted_financial_data(company_name, year)
        except Exception as e:
            logger.error(f"재무제표 조회 실패: {str(e)}")
            raise ValueError(f"재무제표 조회 실패: {str(e)}")

    async def crawl_financial_data(self, company_name: str, year: Optional[int] = None) -> Dict[str, Any]:
        """
        회사의 재무제표 데이터를 크롤링하고 저장합니다.
        
        Args:
            company_name: 회사명
            year: 크롤링할 연도 (None인 경우 최근 연도)
            
        Returns:
            Dict: 크롤링 결과
        """
        logger.info(f"재무제표 크롤링 요청 - 회사: {company_name}, 연도: {year}")
        try:
            result = await self.statement_service.fetch_and_save_financial_data(company_name, year)
            return result
        except Exception as e:
            logger.error(f"재무제표 크롤링 실패: {str(e)}")
            return {"status": "error", "message": str(e)}