from typing import Dict, Any, List, Optional, Tuple
import logging
import asyncio
from datetime import datetime

from app.domain.model.schema.company_schema import CompanySchema
from app.domain.repository.fin_repository import (
    save_financial_statements,
    get_existing_years,
    get_financial_data,
    get_financial_statements,
    get_key_financial_items,
    get_statement_summary,
    get_financial_statements_by_corp_code,
    save_financial_ratios
)
from app.domain.service.dart_api_service import DartApiService
from app.domain.service.financial_data_processor import FinancialDataProcessor
from app.domain.service.financial_data_formatter import FinancialDataFormatter

logger = logging.getLogger(__name__)

class FinancialStatementService:
    """
    재무제표 데이터 서비스
    
    재무제표 데이터의 크롤링, 저장, 조회, 포맷팅을 담당합니다.
    
    의존성:
    - CompanyInfoService: 회사 정보 조회
    - DartApiService: DART API 통신
    - FinancialDataProcessor: 데이터 가공
    - FinancialDataFormatter: 데이터 포맷팅
    """
    
    def __init__(
        self,
        dart_api: Optional[DartApiService] = None,
        data_processor: Optional[FinancialDataProcessor] = None,
        data_formatter: Optional[FinancialDataFormatter] = None
    ):
        """의존성 주입을 통한 초기화"""
        self.dart_api = dart_api or DartApiService()
        self.data_processor = data_processor or FinancialDataProcessor()
        self.data_formatter = data_formatter or FinancialDataFormatter()
        logger.info("FinancialStatementService가 초기화되었습니다.")

    async def auto_crawl_financial_data(self) -> Dict[str, Any]:
        """
        KOSPI 100 기업의 재무제표를 자동으로 크롤링합니다.
        
        Returns:
            Dict: 크롤링 결과 요약
            {
                "status": "success" | "error",
                "message": str,
                "data": List[Dict] - 회사별 크롤링 결과,
                "summary": Dict - 크롤링 결과 요약
            }
        """
        try:
            # 1. KOSPI 100 기업 목록 조회
            companies = await self.dart_api.fetch_top_companies(limit=100)
            logger.info(f"KOSPI 100 기업 중 {len(companies)}개 회사 정보를 가져왔습니다.")
            
            # 2. 각 회사별로 재무제표 크롤링
            results, success_companies, failed_companies = await self._crawl_companies_data(companies)
            
            # 3. 결과 요약 생성
            summary = await self._create_crawl_summary(companies, success_companies, failed_companies)
            
            return {
                "status": "success",
                "message": "자동 크롤링이 완료되었습니다.",
                "data": results,
                "summary": summary
            }
            
        except Exception as e:
            logger.error(f"자동 크롤링 중 오류 발생: {str(e)}")
            return {
                "status": "error",
                "message": f"자동 크롤링 실패: {str(e)}",
                "data": [],
                "summary": {}
            }

    async def _crawl_companies_data(self, companies: List[CompanySchema]) -> Tuple[List[Dict[str, Any]], List[str], List[str]]:
        """
        회사 목록의 재무제표 데이터를 크롤링합니다.
        
        Args:
            companies: 회사 정보 목록
            
        Returns:
            Tuple: (결과 목록, 성공한 회사 목록, 실패한 회사 목록)
        """
        results = []
        success_companies = []
        failed_companies = []
        current_year = datetime.now().year
        
        for idx, company in enumerate(companies):
            try:
                logger.info(f"[{idx+1}/{len(companies)}] {company.corp_name} 처리 중...")
                
                # 1. 기존 데이터 확인
                existing_years = await get_existing_years(company.corp_name)
                
                # 2. 새로운 보고서 확인
                has_new_report = await self.dart_api.check_new_report_available(
                    company.corp_code, 
                    current_year
                )
                
                # 3. 데이터 크롤링 전략 결정
                if has_new_report:
                    # 새로운 보고서가 있으면 현재 연도 데이터만 크롤링
                    success = await self._crawl_single_year(company, current_year, results)
                    if success:
                        success_companies.append(company.corp_name)
                    else:
                        failed_companies.append(company.corp_name)
                        
                elif not existing_years:
                    # 최초 크롤링인 경우 최근 3개년 데이터 크롤링
                    success = await self._crawl_multiple_years(company, current_year, results)
                    if success:
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
        
        return results, success_companies, failed_companies

    async def _crawl_single_year(self, company: CompanySchema, year: int, results: List[Dict[str, Any]]) -> bool:
        """
        단일 연도의 재무제표 데이터를 크롤링합니다.
        
        Args:
            company: 회사 정보
            year: 현재 연도 (이전 연도의 데이터를 크롤링)
            results: 결과를 저장할 리스트
            
        Returns:
            bool: 크롤링 성공 여부
        """
        # 현재 연도가 아닌 이전 연도의 데이터를 크롤링
        target_year = year - 1
        result = await self.fetch_and_save_financial_data(company.corp_name, target_year)
        results.append({
            "company": company.corp_name,
            "year": target_year,
            "status": result["status"],
            "message": result["message"]
        })
        
        return result["status"] == "success" and result.get("data")

    async def _crawl_multiple_years(self, company: CompanySchema, current_year: int, results: List[Dict[str, Any]]) -> bool:
        """
        여러 연도의 재무제표 데이터를 크롤링합니다.
        
        Args:
            company: 회사 정보
            current_year: 현재 연도
            results: 결과를 저장할 리스트
            
        Returns:
            bool: 크롤링 성공 여부 (하나라도 성공하면 True)
        """
        company_success = False
        
        # 최근 3개년 데이터 크롤링 (현재 연도 제외, 이전 3년)
        for year in range(current_year-3, current_year):
            result = await self.fetch_and_save_financial_data(company.corp_name, year)
            results.append({
                "company": company.corp_name,
                "year": year,
                "status": result["status"],
                "message": result["message"]
            })
            
            # 하나의 연도라도 성공했으면 성공으로 간주
            if result["status"] == "success" and result.get("data"):
                company_success = True
                
        return company_success

    async def _create_crawl_summary(self, companies: List[CompanySchema], 
                             success_companies: List[str], 
                             failed_companies: List[str]) -> Dict[str, Any]:
        """
        크롤링 결과 요약을 생성합니다.
        
        Args:
            companies: 전체 회사 목록
            success_companies: 성공한 회사 목록
            failed_companies: 실패한 회사 목록
            
        Returns:
            Dict: 크롤링 결과 요약
        """
        # 결과 요약 출력
        logger.info(f"===== 재무제표 크롤링 결과 요약 =====")
        logger.info(f"총 회사 수: {len(companies)}")
        logger.info(f"성공한 회사 수: {len(success_companies)}")
        logger.info(f"실패한 회사 수: {len(failed_companies)}")
        
        if failed_companies:
            logger.warning(f"실패한 회사 목록: {', '.join(failed_companies)}")
            
        return {
            "total": len(companies),
            "success": len(success_companies),
            "failed": len(failed_companies),
            "success_companies": success_companies,
            "failed_companies": failed_companies
        }

    async def fetch_and_save_financial_data(self, company_name: str, year: Optional[int] = None) -> Dict[str, Any]:
        """
        재무제표 데이터를 가져와 저장합니다.
        
        Args:
            company_name: 회사명
            year: 연도 (없으면 최근 3년)
            
        Returns:
            Dict: 처리 결과
        """
        try:
            # 1. 기존 데이터 확인
            existing_data = await get_financial_data(company_name, year)
            if existing_data:
                logger.info(f"기존 데이터가 있습니다: {company_name} ({year if year else '전체'})")
                return {
                    "status": "success",
                    "message": "기존 데이터가 있습니다.",
                    "data": existing_data
                }
            
            # 2. 회사 정보 조회
            company = await self.dart_api.fetch_company_info(company_name)
            if not company:
                return {
                    "status": "error",
                    "message": "회사 정보를 찾을 수 없습니다."
                }
            
            # 3. DART API에서 데이터 가져오기
            statements = await self.dart_api.fetch_financial_statements(company.corp_code, year)
            if not statements:
                return {
                    "status": "error",
                    "message": "재무제표 데이터를 찾을 수 없습니다."
                }
            
            # 4. 데이터 저장
            success = await save_financial_statements(statements)
            if not success:
                return {
                    "status": "error",
                    "message": "재무제표 데이터 저장에 실패했습니다."
                }
            
            return {
                "status": "success",
                "message": "재무제표 데이터가 저장되었습니다.",
                "data": statements
            }
            
        except Exception as e:
            logger.error(f"재무제표 데이터 처리 중 오류 발생: {str(e)}")
            return {
                "status": "error",
                "message": f"재무제표 데이터 처리 중 오류가 발생했습니다: {str(e)}"
            }

    async def get_formatted_financial_data(self, company_name: str, year: Optional[int] = None) -> Dict[str, Any]:
        """
        저장된 재무제표 데이터를 조회합니다.
        
        Args:
            company_name: 회사명
            year: 연도 (없으면 최근 3년)
            
        Returns:
            Dict: 재무제표 데이터
        """
        try:
            # 1. 데이터 조회
            data = await get_financial_data(company_name, year)
            if not data:
                return {
                    "status": "error",
                    "message": "재무제표 데이터를 찾을 수 없습니다."
                }
            
            # 2. 데이터 포맷팅
            formatted_data = self.data_formatter.format_financial_data(data)
            
            return {
                "status": "success",
                "message": "재무제표 데이터를 조회했습니다.",
                "data": formatted_data
            }
            
        except Exception as e:
            logger.error(f"재무제표 데이터 조회 중 오류 발생: {str(e)}")
            return {
                "status": "error",
                "message": f"재무제표 데이터 조회 중 오류가 발생했습니다: {str(e)}"
            }

    async def get_financial_statements(
        self,
        company_name: str,
        year: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """재무제표 데이터를 조회합니다."""
        try:
            # 1. DB에서 데이터 조회
            statements = await get_financial_statements(company_name=company_name, year=year)
            
            # 2. DB에 데이터가 없으면 DART API에서 조회
            if not statements:
                logger.info(f"DB에 데이터가 없어 DART API에서 조회합니다: {company_name}")
                statements = await self.dart_api.get_financial_statements(company_name, year)
                
                if statements:
                    # 3. DART API에서 조회한 데이터를 DB에 저장
                    await save_financial_statements(statements)
            
            # 4. 데이터 가공 및 포맷팅
            processed_data = self.data_processor.process_financial_statements(statements)
            formatted_data = self.data_formatter.format_financial_data(processed_data)
            
            return formatted_data
        except Exception as e:
            logger.error(f"재무제표 데이터 조회 중 오류 발생: {str(e)}")
            return []

    async def get_key_financial_items(self, company_name: str = None) -> List[Dict[str, Any]]:
        """주요 재무 항목을 조회합니다."""
        try:
            items = await get_key_financial_items(company_name)
            return self.data_formatter.format_financial_data(items)
        except Exception as e:
            logger.error(f"주요 재무 항목 조회 중 오류 발생: {str(e)}")
            return []

    async def get_statement_summary(self) -> List[Dict[str, Any]]:
        """회사별 재무제표 종류와 데이터 수를 조회합니다."""
        try:
            return await get_statement_summary()
        except Exception as e:
            logger.error(f"재무제표 요약 조회 중 오류 발생: {str(e)}")
            return []

    async def get_financial_statements_by_corp_code(self, corp_code: str) -> List[Dict[str, Any]]:
        """회사 코드로 재무제표 데이터를 조회합니다."""
        try:
            statements = await get_financial_statements_by_corp_code(corp_code)
            return self.data_formatter.format_financial_data(statements)
        except Exception as e:
            logger.error(f"재무제표 데이터 조회 중 오류 발생: {str(e)}")
            return []

    async def save_financial_ratios(self, ratios: Dict[str, Any]) -> None:
        """재무비율을 저장합니다."""
        try:
            await save_financial_ratios(ratios)
        except Exception as e:
            logger.error(f"재무비율 저장 중 오류 발생: {str(e)}")
            raise