import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from app.domain.service.auto_crawl_service import AutoCrawlService
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

class FinancialDataScheduler:
    """재무제표 데이터 자동 크롤링 스케줄러"""
    
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self.crawl_service = AutoCrawlService()
        logger.info("재무제표 데이터 스케줄러가 초기화되었습니다.")
    
    def start(self):
        """스케줄러를 시작합니다."""
        if not self.scheduler.running:
            # 매일 오전 11시 30분에 실행
            self.scheduler.add_job(
                self.crawl_financial_data,
                CronTrigger(hour=11, minute=50, timezone=ZoneInfo("Asia/Seoul")),
                id="daily_financial_data_crawl",
                replace_existing=True,
                misfire_grace_time=3600  # 1시간 내에 실행되지 못하면 건너뜀
            )
            self.scheduler.start()
            logger.info("재무제표 데이터 스케줄러가 시작되었습니다. 매일 오전 11시 50분에 실행됩니다.")
    
    def shutdown(self):
        """스케줄러를 종료합니다."""
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("재무제표 데이터 스케줄러가 종료되었습니다.")
    
    async def run_crawl_now(self):
        """재무제표 데이터 크롤링을 즉시 실행합니다."""
        logger.info("재무제표 데이터 크롤링 수동 실행 시작")
        await self.crawl_financial_data()
        return {"status": "success", "message": "재무제표 데이터 크롤링이 시작되었습니다."}
        
    async def crawl_financial_data(self):
        """재무제표 데이터 크롤링을 실행합니다."""
        try:
            result = await self.crawl_service.execute_crawl()
            if result["status"] == "success":
                logger.info("재무제표 데이터 크롤링이 성공적으로 완료되었습니다.")
            else:
                logger.error(f"재무제표 데이터 크롤링 실패: {result.get('message')}")
            return result
        except Exception as e:
            error_msg = f"재무제표 데이터 크롤링 중 오류 발생: {str(e)}"
            logger.exception(error_msg)
            return {
                "status": "error",
                "message": error_msg,
                "data": []
            }

# 싱글톤 인스턴스 생성
financial_scheduler = FinancialDataScheduler() 