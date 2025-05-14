from typing import Dict, Any, List, Optional, Tuple
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
        db_session: AsyncSession,
        company_service: Optional[CompanyInfoService] = None,
        dart_api_service: Optional[DartApiService] = None,
        data_processor: Optional[FinancialDataProcessor] = None,
        data_formatter: Optional[FinancialDataFormatter] = None
    ):
        """
        서비스 초기화
        
        Args:
            db_session: 데이터베이스 세션
            company_service: 회사 정보 서비스 (없으면 새로 생성)
            dart_api_service: DART API 서비스 (없으면 새로 생성)
            data_processor: 데이터 처리기 (없으면 새로 생성)
            data_formatter: 데이터 포맷터 (없으면 새로 생성)
        """
        self.db_session = db_session
        
        # 의존성 주입 또는 생성
        self.dart_api = dart_api_service or DartApiService()
        self.data_processor = data_processor or FinancialDataProcessor()
        self.company_service = company_service or CompanyInfoService(db_session, self.dart_api)
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
                existing_years = await get_existing_years(self.db_session, company.corp_name)
                
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
        회사명으로 재무제표 데이터를 조회하고 저장합니다.
        
        Args:
            company_name: 회사명
            year: 조회할 연도 (None인 경우 최근 연도)
            
        Returns:
            Dict: 조회 및 저장 결과
            {
                "status": "success" | "error",
                "message": str,
                "data": List[Dict] - 저장된 재무제표 데이터
            }
        """
        try:
            # 1. 회사 정보 조회
            company_info = await self.company_service.get_company_info(company_name)
            
            # 2. 기존 데이터 확인
            existing_data = await check_existing_data(self.db_session, company_name, year)
            if existing_data:
                logger.info(f"기존 데이터가 존재합니다: {company_name}, 연도: {year}")
                return {
                    "status": "success",
                    "message": f"{company_name}의 재무제표 데이터가 이미 존재합니다.",
                    "data": existing_data
                }
            
            # 3. DART API에서 재무제표 데이터 조회
            statements = await self._fetch_financial_statements(company_info, year)
            if not statements:
                return {
                    "status": "error",
                    "message": f"{company_name}의 재무제표 데이터를 찾을 수 없습니다.",
                    "data": []
                }
            
            # 4. 데이터 처리 및 저장
            processed_statements = await self._process_and_save_statements(statements, company_info)
            
            # 5. 저장된 데이터 조회하여 반환
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
                "message": str(e),
                "data": []
            }
    
    async def _fetch_financial_statements(self, company_info: CompanySchema, year: Optional[int]) -> List[Dict[str, Any]]:
        """
        DART API에서 재무제표 데이터를 조회합니다.
        
        Args:
            company_info: 회사 정보
            year: 조회할 연도
            
        Returns:
            List[Dict]: 조회된 재무제표 데이터
        """
        logger.info(f"{company_info.corp_name}의 재무제표 조회 시작 (corp_code: {company_info.corp_code}, year: {year})")
        statements = await self.dart_api.fetch_financial_statements(company_info.corp_code, year)
        
        if not statements:
            logger.warning(f"{company_info.corp_name}의 재무제표 데이터를 찾을 수 없습니다. (corp_code: {company_info.corp_code}, year: {year})")
            
        return statements
    
    async def _process_and_save_statements(self, statements: List[Dict[str, Any]], company_info: CompanySchema) -> List[Dict[str, Any]]:
        """
        재무제표 데이터를 처리하고 저장합니다.
        
        Args:
            statements: 원시 재무제표 데이터
            company_info: 회사 정보
            
        Returns:
            List[Dict]: 처리된 재무제표 데이터
        """
        # 1. 데이터 처리
        processed_statements = await self.data_processor.process_raw_statements(statements, company_info)
        
        # 2. 저장
        await save_financial_statements(self.db_session, processed_statements)
        
        return processed_statements

    async def get_formatted_financial_data(self, company_name: str, year: Optional[int] = None) -> Dict[str, Any]:
        """
        회사명으로 재무제표 데이터를 조회하고 포맷팅하여 반환합니다.
        
        Args:
            company_name: 회사명
            year: 조회할 연도 (None인 경우 최근 연도)
            
        Returns:
            Dict: 포맷팅된 재무제표 데이터
            {
                "status": "success" | "error",
                "message": str,
                "data": List[Dict] - 포맷팅된 재무제표 데이터
            }
        """
        try:
            # 1. 데이터 조회 및 저장
            data = await self.fetch_and_save_financial_data(company_name, year)
            
            # 2. 조회 실패 시 빈 데이터 반환
            if data["status"] == "error" or not data.get("data"):
                logger.warning(f"{company_name}의 재무제표 데이터를 찾을 수 없습니다.")
                return {
                    "status": "error",
                    "message": "재무제표가 존재하지 않습니다.",
                    "data": []
                }
            
            # 3. 재무제표 데이터 포맷팅
            return await self.data_formatter.format_financial_data(data["data"])
            
        except Exception as e:
            logger.error(f"재무제표 데이터 포맷팅 실패: {str(e)}")
            return {
                "status": "error",
                "message": f"재무제표 데이터 조회 실패: {str(e)}",
                "data": []
            }