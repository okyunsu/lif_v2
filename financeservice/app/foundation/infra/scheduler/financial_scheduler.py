import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from app.domain.service.financial_statement_service import FinancialStatementService
from app.foundation.infra.database.database import get_db_session

logger = logging.getLogger(__name__)

class FinancialDataScheduler:
    """재무제표 데이터 자동 크롤링 스케줄러"""
    
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        logger.info("재무제표 데이터 스케줄러가 초기화되었습니다.")
    
    def start(self):
        """스케줄러를 시작합니다."""
        if not self.scheduler.running:
            # 매일 새벽 3시에 실행
            self.scheduler.add_job(
                self.crawl_financial_data,
                CronTrigger(hour=3, minute=0),
                id="daily_financial_data_crawl",
                replace_existing=True,
                misfire_grace_time=3600  # 1시간 내에 실행되지 못하면 건너뜀
            )
            self.scheduler.start()
            logger.info("재무제표 데이터 스케줄러가 시작되었습니다. 매일 새벽 3시에 실행됩니다.")
    
    def shutdown(self):
        """스케줄러를 종료합니다."""
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("재무제표 데이터 스케줄러가 종료되었습니다.")
    
    async def crawl_financial_data(self):
        """재무제표 데이터를 자동으로 크롤링합니다."""
        logger.info("재무제표 데이터 자동 크롤링 시작")
        try:
            # DB 세션 생성
            async with get_db_session() as session:
                service = FinancialStatementService(session)
                result = await service.auto_crawl_financial_data()
                
                if result["status"] == "success":
                    logger.info(f"재무제표 데이터 자동 크롤링 완료: {len(result.get('data', []))}개 회사 처리")
                else:
                    logger.error(f"재무제표 데이터 자동 크롤링 실패: {result.get('message')}")
        
        except Exception as e:
            logger.exception(f"재무제표 데이터 자동 크롤링 중 오류 발생: {str(e)}")

# 싱글톤 인스턴스 생성
financial_scheduler = FinancialDataScheduler() 