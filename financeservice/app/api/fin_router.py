from fastapi import APIRouter, Request, Query, Body
import logging
from app.domain.controller.fin_controller import FinController
from app.foundation.infra.database.database import get_db_session
from app.domain.model.schema.schema import (
    CompanyNameRequest,
        
)
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.foundation.infra.scheduler.financial_scheduler import financial_scheduler
from typing import Optional, List

# 로거 설정
logger = logging.getLogger("fin_router")
logger.setLevel(logging.INFO)
router = APIRouter()

# GET
@router.get("/financial", summary="모든 회사 목록 조회")
async def get_all_companies():
    """
    등록된 모든 회사의 목록을 조회합니다.
    """
    print("📋 모든 회사 목록 조회")
    logger.info("📋 모든 회사 목록 조회")
    
    # 샘플 데이터
    companies = [
        {"id": 1, "name": "샘플전자", "industry": "전자제품"},
        {"id": 2, "name": "테스트기업", "industry": "소프트웨어"},
        {"id": 3, "name": "예시주식", "industry": "금융"}
    ]
    return {"companies": companies}

# POST
@router.post("/financial", summary="회사명으로 재무제표 크롤링")
async def get_financial_by_name(
    payload: CompanyNameRequest,
    db: AsyncSession = Depends(get_db_session)
):
    """
    회사명으로 재무제표를 크롤링하고 저장합니다.
    - DART API를 통해 재무제표 데이터를 가져옵니다.
    - 가져온 데이터를 데이터베이스에 저장합니다.
    - 크롤링 성공/실패 여부를 반환합니다.
    """
    print(f"🕞🕞🕞🕞🕞🕞get_financial_by_name 호출 - 회사명: {payload.company_name}")
    logger.info(f"🕞🕞🕞🕞🕞🕞get_financial_by_name 호출 - 회사명: {payload.company_name}")
    controller = FinController(db)
    return await controller.get_financial(company_name=payload.company_name)

# 크롤링 수동 실행 엔드포인트
@router.post("/financial/crawl-now", summary="재무제표 크롤링 즉시 실행")
async def run_crawling_now():
    """
    재무제표 데이터 크롤링을 즉시 실행합니다.
    - 모든 회사의 재무제표 데이터를 크롤링합니다.
    - 백그라운드에서 실행되며, 실행 시작 여부를 반환합니다.
    """
    logger.info("🚀 재무제표 크롤링 수동 실행 요청")
    result = await financial_scheduler.run_crawl_now()
    return result

# PUT
@router.put("/financial", summary="회사 정보 전체 수정")
async def update_company(request: Request):
    """
    회사 정보를 전체 수정합니다.
    """
    print("📝 회사 정보 전체 수정")
    logger.info("📝 회사 정보 전체 수정")
    
    # 샘플 응답
    return {
        "message": "회사 정보가 성공적으로 수정되었습니다.",
        "updated_data": {
            "name": "수정된샘플전자",
            "industry": "수정된산업"
        }
    }

# DELETE
@router.delete("/financial", summary="회사 정보 삭제")
async def delete_company():
    """
    회사 정보를 삭제합니다.
    """
    print("🗑️ 회사 정보 삭제")
    logger.info("🗑️ 회사 정보 삭제")
    
    # 샘플 응답
    return {
        "message": "회사 정보가 성공적으로 삭제되었습니다."
    }

# PATCH
@router.patch("/financial", summary="회사 정보 부분 수정")
async def patch_company(request: Request):
    """
    회사 정보를 부분적으로 수정합니다.
    """
    print("✏️ 회사 정보 부분 수정")
    logger.info("✏️ 회사 정보 부분 수정")
    
    # 샘플 응답
    return {
        "message": "회사 정보가 부분적으로 수정되었습니다.",
        "updated_fields": {
            "name": "부분수정샘플전자"
        }
    }
