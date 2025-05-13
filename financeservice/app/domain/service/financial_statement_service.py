from typing import Dict, Any, List, Optional
import logging
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime

from app.domain.model.schema.company_schema import CompanySchema
from app.domain.repository.fin_repository import (
    save_financial_statements,
    get_existing_years,
    check_existing_data,
    get_financial_data
)
from app.domain.service.dart_api_service import DartApiService
from app.domain.service.financial_data_processor import FinancialDataProcessor
from app.domain.service.company_info_service import CompanyInfoService
from app.domain.service.financial_data_formatter import FinancialDataFormatter

logger = logging.getLogger(__name__)

class FinancialStatementService:
    """재무제표 데이터 서비스"""
    
    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session
        self.dart_api = DartApiService()
        self.data_processor = FinancialDataProcessor()
        self.company_info_service = CompanyInfoService(db_session)
        self.data_formatter = FinancialDataFormatter()

    async def auto_crawl_financial_data(self) -> Dict[str, Any]:
        """상위 100개 회사의 재무제표를 자동으로 크롤링합니다."""
        try:
            # 월 확인 코드 제거 - 항상 크롤링 실행
            
            # 1. 상위 100개 회사 조회 (KOSPI 100)
            companies = await self.dart_api.fetch_top_companies(limit=100)
            logger.info(f"KOSPI 100 기업 중 {len(companies)}개 회사 정보를 가져왔습니다.")
            
            # 2. 각 회사별로 처리
            results = []
            success_companies = []
            failed_companies = []
            current_year = datetime.now().year
            
            for idx, company in enumerate(companies):
                try:
                    logger.info(f"[{idx+1}/{len(companies)}] {company.corp_name} 처리 중...")
                    # 2.1. 기존 데이터 확인
                    existing_years = await get_existing_years(self.db_session, company.corp_name)
                    
                    # 2.2. 새로운 보고서 확인
                    has_new_report = await self.dart_api.check_new_report_available(
                        company.corp_code, 
                        current_year
                    )
                    
                    if has_new_report:
                        # 새로운 보고서가 있으면 저장
                        result = await self.fetch_and_save_financial_data(
                            company.corp_name, 
                            current_year
                        )
                        results.append({
                            "company": company.corp_name,
                            "year": current_year,
                            "status": result["status"],
                            "message": result["message"]
                        })
                        
                        if result["status"] == "success" and result.get("data"):
                            success_companies.append(company.corp_name)
                        else:
                            failed_companies.append(company.corp_name)
                            
                    elif not existing_years:  # 최초 크롤링인 경우
                        # 최근 3개년 데이터 저장
                        company_success = False
                        for year in range(current_year-2, current_year+1):
                            result = await self.fetch_and_save_financial_data(
                                company.corp_name, 
                                year
                            )
                            results.append({
                                "company": company.corp_name,
                                "year": year,
                                "status": result["status"],
                                "message": result["message"]
                            })
                            
                            # 하나의 연도라도 성공했으면 성공으로 간주
                            if result["status"] == "success" and result.get("data"):
                                company_success = True
                        
                        if company_success:
                            success_companies.append(company.corp_name)
                        else:
                            failed_companies.append(company.corp_name)
                    else:
                        # 기존 데이터가 있고 새 보고서도 없으면 성공으로 간주
                        success_companies.append(company.corp_name)
                        logger.info(f"{company.corp_name}: 기존 데이터가 있고 새 보고서가 없습니다.")
                
                except Exception as e:
                    logger.error(f"회사 {company.corp_name} 처리 중 오류 발생: {str(e)}")
                    failed_companies.append(company.corp_name)
                    results.append({
                        "company": company.corp_name,
                        "status": "error",
                        "message": str(e)
                    })
            
            # 3. 결과 요약 출력
            logger.info(f"===== 재무제표 크롤링 결과 요약 =====")
            logger.info(f"총 회사 수: {len(companies)}")
            logger.info(f"성공한 회사 수: {len(success_companies)}")
            logger.info(f"실패한 회사 수: {len(failed_companies)}")
            
            if failed_companies:
                logger.warning(f"실패한 회사 목록: {', '.join(failed_companies)}")
            
            return {
                "status": "success",
                "message": "자동 크롤링이 완료되었습니다.",
                "data": results,
                "summary": {
                    "total": len(companies),
                    "success": len(success_companies),
                    "failed": len(failed_companies),
                    "success_companies": success_companies,
                    "failed_companies": failed_companies
                }
            }
            
        except Exception as e:
            logger.error(f"자동 크롤링 중 오류 발생: {str(e)}")
            return {
                "status": "error",
                "message": str(e)
            }

    async def fetch_and_save_financial_data(self, company_name: str, year: Optional[int] = None) -> Dict[str, Any]:
        """회사명으로 재무제표 데이터를 조회하고 저장합니다."""
        try:
            # 1. 회사 정보 조회
            company_info = await self.company_info_service.get_company_info(company_name)
            
            # 2. 기존 데이터 확인
            existing_data = await check_existing_data(self.db_session, company_name, year)
            if existing_data:
                logger.info(f"기존 데이터가 존재합니다: {company_name}, 연도: {year}")
                return {
                    "status": "success",
                    "message": f"{company_name}의 재무제표 데이터가 이미 존재합니다.",
                    "data": existing_data
                }
            
            # 3. 재무제표 데이터 조회
            logger.info(f"{company_name}의 재무제표 조회 시작 (corp_code: {company_info.corp_code}, year: {year})")
            statements = await self.dart_api.fetch_financial_statements(company_info.corp_code, year)
            
            if not statements:
                logger.warning(f"{company_name}의 재무제표 데이터를 찾을 수 없습니다. (corp_code: {company_info.corp_code}, year: {year})")
                return {
                    "status": "error",
                    "message": f"{company_name}의 재무제표 데이터를 찾을 수 없습니다."
                }
            
            # 4. 중복 제거
            statements = await self.data_processor.deduplicate_statements(statements)
            
            # 5. 새로운 데이터 저장
            # 비동기 리스트 컴프리헨션으로 변경
            statement_data = await asyncio.gather(*[
                self.data_processor.prepare_statement_data(stmt, company_info)
                for stmt in statements
            ])
            await save_financial_statements(self.db_session, statement_data)
            
            # 6. 저장된 데이터 조회하여 반환
            saved_data = await get_financial_data(self.db_session, company_name, year)
            
            logger.info(f"{company_name}의 재무제표 데이터 저장 성공 (항목 {len(saved_data)}개)")
            return {
                "status": "success",
                "message": f"{company_name}의 재무제표 데이터가 성공적으로 저장되었습니다.",
                "data": saved_data
            }
            
        except Exception as e:
            logger.error(f"{company_name}의 재무제표 데이터 저장 실패: {str(e)}")
            return {
                "status": "error",
                "message": str(e)
            }

    async def get_formatted_financial_data(self, company_name: str, year: Optional[int] = None) -> Dict[str, Any]:
        """회사명으로 재무제표 데이터를 조회하고 포맷팅하여 반환합니다."""
        try:
            # 데이터 조회
            data = await self.fetch_and_save_financial_data(company_name, year)
            if data["status"] == "error":
                return {
                    "status": "success",
                    "message": "재무제표가 성공적으로 조회되었습니다.",
                    "data": []
                }
            
            # 재무제표 데이터 포맷팅
            financial_data = await self.data_formatter.format_financial_data(data["data"])
            
            return {
                "status": "success",
                "message": "재무제표가 성공적으로 조회되었습니다.",
                "data": financial_data
            }
        except Exception as e:
            logger.error(f"재무제표 데이터 포맷팅 실패: {str(e)}")
            return {
                "status": "error",
                "message": str(e)
            }