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
    파사드 패턴: 금융 데이터 분석 서비스
    
    컨트롤러와 내부 서비스 계층 간의 인터페이스를 단순화합니다.
    모든 금융 데이터 요청을 처리하고 적절한 서비스로 위임합니다.
    """
    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session
        self.ratio_service = RatioService(db_session)

    async def calculate_financial_ratios(self, company_name: str, year: Optional[int] = None) -> FinancialMetricsResponse:
        """
        회사명(및 연도)로 재무비율 계산을 요청합니다.
        
        Args:
            company_name: 회사명
            year: 대상 연도 (None이면 가장 최근 데이터 사용)
            
        Returns:
            FinancialMetricsResponse: 재무비율 응답 객체
            
        Raises:
            ValueError: 입력값 오류 또는 데이터가 없는 경우
            Exception: 기타 서비스 처리 중 발생한 오류
        """
        logger.info(f"재무비율 계산 요청 - 회사: {company_name}, 연도: {year if year else '최근'}")
        try:
            return await self.ratio_service.calculate_financial_ratios(company_name, year)
        except ValueError as e:
            # 입력값 오류 또는 데이터 없음 - 그대로 상위로 전달
            logger.error(f"재무비율 계산 값 오류: {str(e)}")
            raise
        except Exception as e:
            # 기타 오류 - 로깅 후 상위로 전달
            logger.error(f"재무비율 계산 중 오류: {str(e)}", exc_info=True)
            raise