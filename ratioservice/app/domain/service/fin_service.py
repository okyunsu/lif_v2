import logging
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.service.ratio_service import RatioService
from app.domain.model.schema.schema import FinancialMetricsResponse

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
    파사드: DB에서 데이터 조회 + RatioService로 계산 위임
    컨트롤러에서 바로 사용 가능하도록 단순화
    """
    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session
        self.ratio_service = RatioService(db_session)

    async def calculate_financial_ratios(self, company_name: str, year: Optional[int] = None) -> FinancialMetricsResponse:
        """
        회사명(및 연도)로 재무비율 계산
        """
        logger.info(f"재무비율 계산 요청 - 회사: {company_name}, 연도: {year}")
        try:
            return await self.ratio_service.calculate_financial_ratios(company_name, year)
        except Exception as e:
            logger.error(f"재무비율 계산 중 오류: {str(e)}")
            raise