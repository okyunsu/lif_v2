import logging
from typing import Dict, Any
from app.domain.service.financial_statement_service import FinancialStatementService

logger = logging.getLogger(__name__)

class AutoCrawlService:
    """자동 크롤링 서비스
    
    재무제표 데이터의 자동 크롤링을 담당하는 서비스입니다.
    스케줄러나 수동 실행 요청에 의해 호출될 수 있습니다.
    """
    
    def __init__(self):
        """서비스 초기화"""
        self.statement_service = FinancialStatementService()
        logger.info("AutoCrawlService가 초기화되었습니다.")
    
    async def execute_crawl(self) -> Dict[str, Any]:
        """재무제표 데이터 자동 크롤링을 실행합니다.
        
        Returns:
            Dict[str, Any]: 크롤링 결과
            {
                "status": "success" | "error",
                "message": str,
                "data": List[Dict] - 크롤링된 데이터
            }
        """
        logger.info("재무제표 데이터 자동 크롤링 시작")
        try:
            result = await self.statement_service.auto_crawl_financial_data()
            
            if result["status"] == "success":
                logger.info(f"재무제표 데이터 자동 크롤링 완료: {len(result.get('data', []))}개 회사 처리")
            else:
                logger.error(f"재무제표 데이터 자동 크롤링 실패: {result.get('message')}")
            
            return result
            
        except Exception as e:
            error_msg = f"재무제표 데이터 자동 크롤링 중 오류 발생: {str(e)}"
            logger.exception(error_msg)
            return {
                "status": "error",
                "message": error_msg,
                "data": []
            } 