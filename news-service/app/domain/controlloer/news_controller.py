from app.domain.service.news_service import NewsService
import logging

logger = logging.getLogger(__name__) # news_main 대신 __name__ 사용 권장

class NewsController:
    def __init__(self):
        self.news_service = NewsService()

    def get_news(self, company_name: str):
        # 1. 뉴스 정보 및 본문 가져오기 (get_news가 본문까지 가져오도록 수정됨)
        self.news_service.get_news(company_name)
        
        return {
            "company": company_name,
  
        }
