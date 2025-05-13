from fastapi import APIRouter, FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
import logging
import sys
import os
from dotenv import load_dotenv
from contextlib import asynccontextmanager
from app.api.fin_router import router as fin_api_router
from app.foundation.infra.scheduler.financial_scheduler import financial_scheduler

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("finance_api")

# .env 파일 로드
load_dotenv()
    
# ✅ 애플리케이션 시작 시 실행
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 Finance API 서비스 시작")
    # 스케줄러 시작
    financial_scheduler.start()
    logger.info("재무제표 데이터 자동 크롤링 스케줄러가 시작되었습니다.")
    
    # 시작 시 크롤링 실행 여부 확인
    run_crawl_on_startup = os.getenv("RUN_CRAWL_ON_STARTUP", "false").lower() == "true"
    if run_crawl_on_startup:
        logger.info("서비스 시작 시 재무제표 크롤링을 실행합니다.")
        # 비동기 태스크로 크롤링 실행
        import asyncio
        asyncio.create_task(financial_scheduler.run_crawl_now())
    
    yield
    logger.info("🛑 Finance API 서비스 종료")
    # 스케줄러 종료
    financial_scheduler.shutdown()
    logger.info("재무제표 데이터 자동 크롤링 스케줄러가 종료되었습니다.")


# ✅ FastAPI 앱 생성 
app = FastAPI(
    title="Finance API",
    description="Finance API Service",
    version="0.1.0",
    lifespan=lifespan
)

# ✅ CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ 서브 라우터 생성
fin_router = APIRouter(prefix="/fin", tags=["Finance API"])

# ✅ 서브 라우터와 엔드포인트를 연결함
app.include_router(fin_api_router, prefix="/fin", tags=["Finance API"])

# ✅ 서브 라우터 등록
app.include_router(fin_router, tags=["Finance API"])

@app.get("/")
async def root():
    """루트 엔드포인트"""
    return {"message": "Finance Service API에 오신 것을 환영합니다!"}

# 애플리케이션 실행 (uvicorn에서 실행 시 사용되지 않음)
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)

