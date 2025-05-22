from fastapi import APIRouter, Request, Query, Body, Depends, HTTPException
import logging
from app.domain.controller.fin_controller import FinController
from app.domain.service.fin_service import FinService
from typing import Dict, Any, Optional, List
from app.foundation.infra.scheduler.financial_scheduler import financial_scheduler

# 로거 설정
logger = logging.getLogger("fin_router")
logger.setLevel(logging.INFO)
router = APIRouter()

# GET
@router.get("/company/{company_name}")
async def get_company_info(company_name: str) -> Dict[str, Any]:
    """
    회사 정보를 조회합니다.
    
    Args:
        company_name: 회사명
        
    Returns:
        Dict: 회사 정보
    """
    controller = FinController(FinService())
    return await controller.get_company_info(company_name)

@router.get("/financial/{company_name}")
async def get_financial_data(
    company_name: str,
    year: Optional[int] = None
) -> Dict[str, Any]:
    """
    재무제표 데이터를 조회합니다.
    
    Args:
        company_name: 회사명
        year: 연도 (없으면 최근 3년)
        
    Returns:
        Dict: 재무제표 데이터
    """
    controller = FinController(FinService())
    return await controller.get_financial_data(company_name, year)

@router.post("/financial")
async def save_financial_data(
    company_name: str,
    year: Optional[int] = None
) -> Dict[str, Any]:
    """
    재무제표 데이터를 저장합니다.
    
    Args:
        company_name: 회사명
        year: 연도 (없으면 최근 3년)
        
    Returns:
        Dict: 저장 결과
    """
    controller = FinController(FinService())
    return await controller.save_financial_data(company_name, year)

@router.get("/auto-crawl")
async def execute_auto_crawl() -> Dict[str, Any]:
    """
    자동 크롤링을 실행합니다.
    
    Returns:
        Dict: 실행 결과
    """
    controller = FinController(FinService())
    return await controller.execute_auto_crawl()

# POST
@router.post("/crawl", summary="재무제표 크롤링")
async def crawl_financial(
    company_name: str = Query(..., description="회사명"),
    year: Optional[int] = Query(None, description="크롤링할 연도. 지정하지 않으면 직전 연도의 데이터를 크롤링")
):
    """회사명으로 재무제표를 크롤링합니다."""
    return await fin_controller.crawl_financial(company_name, year) 

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
