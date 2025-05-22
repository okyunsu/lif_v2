from fastapi import HTTPException, Query
from app.domain.service.fin_service import FinService
import logging
from typing import Optional

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class FinController:
    def __init__(self):
        logger.info("FinController가 초기화되었습니다.")
        self.service = FinService()

    async def get_financial(
        self, 
        company_name: str = Query(..., description="회사명"),
        year: Optional[int] = Query(None, description="조회할 연도. 지정하지 않으면 직전 연도의 데이터를 조회")
    ) -> dict:
        """회사명으로 재무제표를 조회합니다.
        
        Args:
            company_name: 회사명
            year: 조회할 연도. None이면 직전 연도의 데이터를 조회
        """
        logger.info(f"재무제표 조회 요청 - 회사: {company_name}, 연도: {year}")
        try:
            result = await self.service.get_financial_statements(company_name, year)
            if result["status"] == "error":
                raise HTTPException(status_code=404, detail=result["message"])
            return result
        except Exception as e:
            logger.error(f"재무제표 조회 중 오류 발생: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    async def crawl_financial(
        self,
        company_name: str = Query(..., description="회사명"),
        year: Optional[int] = Query(None, description="크롤링할 연도. 지정하지 않으면 직전 연도의 데이터를 크롤링")
    ) -> dict:
        """회사명으로 재무제표를 크롤링합니다.
        
        Args:
            company_name: 회사명
            year: 크롤링할 연도. None이면 직전 연도의 데이터를 크롤링
        """
        logger.info(f"재무제표 크롤링 요청 - 회사: {company_name}, 연도: {year}")
        try:
            result = await self.service.crawl_financial_data(company_name, year)
            if result["status"] == "error":
                raise HTTPException(status_code=404, detail=result["message"])
            return result
        except Exception as e:
            logger.error(f"재무제표 크롤링 중 오류 발생: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))