import logging
from typing import Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession

from app.foundation.core.config.settings import settings
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
    재무 정보 서비스 파사드 클래스
    
    클라이언트가 다양한 재무 관련 서비스에 쉽게 접근할 수 있도록 단일 인터페이스를 제공합니다.
    내부적으로 각 책임에 맞는 서비스에 작업을 위임합니다.
    
    의존성:
    - CompanyInfoService: 회사 정보 관련 기능
    - FinancialStatementService: 재무제표 관련 기능
    
    서비스 계층 구조:
    - FinService (파사드)
      ├── CompanyInfoService
      │   └── DartApiService
      └── FinancialStatementService
          ├── CompanyInfoService
          ├── DartApiService
          ├── FinancialDataProcessor
          └── FinancialDataFormatter
    """
    def __init__(
        self, 
        db_session: AsyncSession,
        company_service: Optional[CompanyInfoService] = None,
        statement_service: Optional[FinancialStatementService] = None
    ):
        """
        서비스 초기화
        
        Args:
            db_session: 데이터베이스 세션
            company_service: 회사 정보 서비스 (없으면 새로 생성)
            statement_service: 재무제표 서비스 (없으면 새로 생성)
        """
        # 환경 변수 검증
        if not settings.DART_API_KEY:
            logger.error("DART API 키가 설정되지 않았습니다.")
            raise ValueError("DART API 키가 필요합니다. 환경 변수 DART_API_KEY를 설정하세요.")
            
        # 서비스 초기화
        self.db_session = db_session
        self.company_service = company_service or CompanyInfoService(db_session)
        self.statement_service = statement_service or FinancialStatementService(
            db_session,
            company_service=self.company_service
        )
        
        logger.info("FinService가 초기화되었습니다.")

    async def get_company_info(self, company_name: str) -> Dict[str, Any]:
        """
        회사 정보를 조회합니다.
        
        Args:
            company_name: 회사명
            
        Returns:
            Dict: 회사 정보 응답
            {
                "status": "success" | "error",
                "message": str,
                "data": CompanySchema | None
            }
        """
        logger.info(f"회사 정보 조회 요청: {company_name}")
        try:
            company_info = await self.company_service.get_company_info(company_name)
            return {
                "status": "success",
                "message": f"{company_name} 회사 정보를 성공적으로 조회했습니다.",
                "data": company_info
            }
        except Exception as e:
            logger.error(f"회사 정보 조회 실패: {str(e)}")
            return {
                "status": "error",
                "message": f"회사 정보 조회 실패: {str(e)}",
                "data": None
            }

    async def get_financial_statements(self, company_name: str, year: Optional[int] = None) -> Dict[str, Any]:
        """
        재무제표 데이터를 조회합니다.
        
        Args:
            company_name: 회사명
            year: 조회할 연도 (None인 경우 최근 연도)
            
        Returns:
            Dict: 포맷팅된 재무제표 데이터
            {
                "status": "success" | "error",
                "message": str,
                "data": List[Dict] - 포맷팅된 재무제표 데이터
            }
        """
        logger.info(f"재무제표 조회 요청 - 회사: {company_name}, 연도: {year}")
        try:
            return await self.statement_service.get_formatted_financial_data(company_name, year)
        except Exception as e:
            logger.error(f"재무제표 조회 실패: {str(e)}")
            return {
                "status": "error",
                "message": f"재무제표 조회 실패: {str(e)}",
                "data": []
            }

    async def crawl_financial_data(self, company_name: str, year: Optional[int] = None) -> Dict[str, Any]:
        """
        회사의 재무제표 데이터를 크롤링하고 저장합니다.
        
        Args:
            company_name: 회사명
            year: 크롤링할 연도 (None인 경우 최근 연도)
            
        Returns:
            Dict: 크롤링 결과
            {
                "status": "success" | "error",
                "message": str,
                "data": List[Dict] - 저장된 재무제표 데이터
            }
        """
        logger.info(f"재무제표 크롤링 요청 - 회사: {company_name}, 연도: {year}")
        try:
            return await self.statement_service.fetch_and_save_financial_data(company_name, year)
        except Exception as e:
            logger.error(f"재무제표 크롤링 실패: {str(e)}")
            return {
                "status": "error",
                "message": f"재무제표 크롤링 실패: {str(e)}",
                "data": []
            }