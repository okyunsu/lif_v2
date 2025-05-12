import os
import logging
import aiohttp
import zipfile
import xml.etree.ElementTree as ET
from io import BytesIO
from typing import List, Optional, Dict, Any, Tuple
from dotenv import load_dotenv
from datetime import datetime, timedelta

from app.domain.model.schema.schema import DartApiResponse
from app.domain.model.schema.company_schema import CompanySchema
from app.domain.model.schema.report_schema import ReportSchema
from app.domain.model.schema.statement_schema import StatementSchema

# 로깅 설정
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# 핸들러가 없으면 추가
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(handler)

class DartApiService:
    """DART API 통신 서비스"""
    
    # API 엔드포인트 상수
    CORP_CODE_URL = "https://opendart.fss.or.kr/api/corpCode.xml"
    FINANCIAL_STATEMENT_URL = "https://opendart.fss.or.kr/api/fnlttSinglAcnt.json"
    CASH_FLOW_URL = "https://opendart.fss.or.kr/api/fnlttCashFlow.json"
    REPORT_LIST_URL = "https://opendart.fss.or.kr/api/list.json"
    
    def __init__(self):
        """DART API 서비스 초기화"""
        load_dotenv()
        self.api_key = os.getenv("DART_API_KEY")
        if not self.api_key:
            logger.error("DART API 키가 필요합니다.")
            raise ValueError("DART API 키가 필요합니다.")
        logger.info("DartApiService가 초기화되었습니다.")

    async def fetch_top_companies(self, limit: int = 100) -> List[CompanySchema]:
        """시가총액 상위 회사 목록을 조회합니다."""
        logger.info(f"상위 {limit}개 회사 조회 시작")
        
        try:
            content = await self._make_api_request(self.CORP_CODE_URL, {"crtfc_key": self.api_key})
            companies = self._parse_company_xml(content, limit)
            logger.info(f"상위 {len(companies)}개 회사 조회 완료")
            return companies
        except Exception as e:
            logger.error(f"회사 목록 조회 실패: {str(e)}")
            raise

    async def check_new_report_available(self, corp_code: str, year: int) -> bool:
        """새로운 보고서가 있는지 확인합니다."""
        # TODO: 추후 실제 운영 시에는 아래 주석을 해제하고 테스트용 코드를 제거할 것
        # current_month = datetime.now().month
        # if not (12 <= current_month <= 3):  # 12월 ~ 3월 사이만 확인
        #     return False

        params = {
            "crtfc_key": self.api_key,
            "corp_code": corp_code,
            "bgn_de": f"{year}0101",
            "end_de": f"{year}1231",
            "pblntf_ty": "A001"  # 사업보고서
        }

        try:
            data = await self._make_json_api_request(self.REPORT_LIST_URL, params)
            if data.get("status") != "000":
                return False

            # 최근 7일 이내 보고서가 있는지 확인
            for item in data.get("list", []):
                rcept_dt = datetime.strptime(item.get("rcept_dt", ""), "%Y%m%d")
                if (datetime.now() - rcept_dt) <= timedelta(days=7):
                    return True

            return False
        except Exception as e:
            logger.error(f"보고서 확인 중 오류 발생: {str(e)}")
            return False

    async def fetch_company_info(self, company_name: str) -> CompanySchema:
        """DART API에서 회사 정보를 조회합니다."""
        logger.info(f"회사 정보 조회 시작: {company_name}")
        
        try:
            content = await self._make_api_request(self.CORP_CODE_URL, {"crtfc_key": self.api_key})
            company = self._find_company_by_name(content, company_name)
            logger.info(f"회사 정보를 찾았습니다: {company_name}")
            return company
        except Exception as e:
            logger.error(f"회사 정보 조회 실패: {str(e)}")
            raise

    async def fetch_financial_statements(self, corp_code: str, year: Optional[int] = None) -> List[Dict[str, Any]]:
        """DART API에서 재무제표 데이터를 조회합니다."""
        logger.info(f"재무제표 조회 시작 - corp_code: {corp_code}, year: {year}")
        
        # 연도 설정
        current_year = datetime.now().year
        target_year = self._determine_target_year(year, current_year)
        
        # 사업보고서 조회
        statements = []
        report_codes = [("11011", "사업보고서")]
        
        for reprt_code, reprt_name in report_codes:
            # 기본 파라미터 설정
            params = self._prepare_financial_statement_params(corp_code, target_year, reprt_code)
            
            # 재무상태표와 손익계산서 조회
            bs_is_statements = await self._fetch_bs_is_statements(params, target_year, reprt_name)
            if bs_is_statements:
                statements.extend(bs_is_statements)
            
            # 현금흐름표 조회
            cf_statements = await self._fetch_cash_flow_statements(params, target_year, reprt_name)
            if cf_statements:
                statements.extend(cf_statements)
            
            # 데이터를 찾았다면 더 이상 시도하지 않음
            if statements:
                logger.info(f"{target_year}년도 {reprt_name}에서 재무제표 데이터를 찾았습니다.")
                break
            
            # 데이터가 없고, 연도를 지정하지 않은 경우 이전 연도 시도
            if not statements and year is None and target_year > current_year - 3:
                logger.info(f"직전 연도({target_year}) 데이터가 없어 이전 연도({target_year-1}) 조회를 시도합니다.")
                return await self.fetch_financial_statements(corp_code, target_year - 1)
        
        logger.info(f"조회된 재무제표 수: {len(statements)}")
        return statements

    # ===== 내부 헬퍼 메서드 =====
    
    async def _make_api_request(self, url: str, params: Dict[str, Any]) -> bytes:
        """API 요청을 수행하고 바이너리 응답을 반환합니다."""
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                if response.status != 200:
                    logger.error(f"API 요청 실패: {response.status}")
                    raise Exception(f"API 요청 실패: {response.status}")
                return await response.read()

    async def _make_json_api_request(self, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """API 요청을 수행하고 JSON 응답을 반환합니다."""
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                if response.status != 200:
                    logger.error(f"API 요청 실패: {response.status}")
                    raise Exception(f"API 요청 실패: {response.status}")
                return await response.json()

    def _parse_company_xml(self, content: bytes, limit: int) -> List[CompanySchema]:
        """회사 정보 XML을 파싱하여 CompanySchema 리스트로 반환합니다."""
        companies = []
        with zipfile.ZipFile(BytesIO(content)) as zip_file:
            with zip_file.open('CORPCODE.xml') as xml_file:
                tree = ET.parse(xml_file)
                root = tree.getroot()
                
                # 상장사만 필터링 (stock_code가 있는 경우)
                for company in root.findall('.//list'):
                    stock_code = company.findtext('stock_code')
                    if stock_code and stock_code.strip():
                        now = datetime.now().isoformat()
                        companies.append(CompanySchema(
                            corp_code=company.findtext('corp_code'),
                            corp_name=company.findtext('corp_name'),
                            stock_code=stock_code,
                            created_at=now,
                            updated_at=now
                        ))
                        if len(companies) >= limit:
                            break
        return companies

    def _find_company_by_name(self, content: bytes, company_name: str) -> CompanySchema:
        """회사명으로 회사 정보를 찾아 반환합니다."""
        with zipfile.ZipFile(BytesIO(content)) as zip_file:
            with zip_file.open('CORPCODE.xml') as xml_file:
                tree = ET.parse(xml_file)
                root = tree.getroot()
                
                for company in root.findall('.//list'):
                    if company.findtext('corp_name') == company_name:
                        now = datetime.now().isoformat()
                        return CompanySchema(
                            corp_code=company.findtext('corp_code'),
                            corp_name=company_name,
                            stock_code=company.findtext('stock_code') or "",
                            created_at=now,
                            updated_at=now
                        )
                
                logger.error(f"회사명 '{company_name}'을 찾을 수 없습니다.")
                raise ValueError(f"회사명 '{company_name}'을 찾을 수 없습니다.")

    def _determine_target_year(self, year: Optional[int], current_year: int) -> int:
        """조회할 연도를 결정합니다."""
        if year is None or not isinstance(year, int):
            target_year = current_year - 1
            logger.info(f"연도가 지정되지 않아 {target_year}년도 데이터를 조회합니다.")
        else:
            target_year = year
            logger.info(f"{target_year}년도 데이터를 조회합니다.")
        return target_year

    def _prepare_financial_statement_params(self, corp_code: str, year: int, reprt_code: str) -> Dict[str, str]:
        """재무제표 API 요청 파라미터를 준비합니다."""
        return {
            "crtfc_key": self.api_key,
            "corp_code": corp_code,
            "bsns_year": str(year),
            "reprt_code": reprt_code,
            "fs_div": "CFS"  # 연결재무제표
        }

    async def _fetch_bs_is_statements(self, params: Dict[str, str], year: int, reprt_name: str) -> List[Dict[str, Any]]:
        """재무상태표와 손익계산서 데이터를 조회합니다."""
        logger.info(f"{year}년도 {reprt_name} 조회를 시작합니다.")
        statements = []
        
        try:
            data = await self._make_json_api_request(self.FINANCIAL_STATEMENT_URL, params)
            api_response = DartApiResponse(**data)
            
            if api_response.status != "000":
                logger.error(f"{year}년도 {reprt_name} API 응답 실패: {api_response.message}")
                return []
            
            for item in api_response.list:
                if item.get("sj_div") in ["BS", "IS"]:
                    self._add_year_labels(item, year)
                    statements.append(item)
            
            return statements
        except Exception as e:
            logger.error(f"{reprt_name} 재무제표 조회 중 오류 발생: {str(e)}")
            return []

    async def _fetch_cash_flow_statements(self, params: Dict[str, str], year: int, reprt_name: str) -> List[Dict[str, Any]]:
        """현금흐름표 데이터를 조회합니다."""
        statements = []
        
        try:
            data = await self._make_json_api_request(self.CASH_FLOW_URL, params)
            api_response = DartApiResponse(**data)
            
            if api_response.status != "000":
                logger.error(f"{year}년도 {reprt_name} 현금흐름표 API 응답 실패: {api_response.message}")
                return []
            
            for item in api_response.list:
                item["sj_div"] = "CF"
                item["sj_nm"] = "현금흐름표"
                self._add_year_labels(item, year)
                statements.append(item)
            
            return statements
        except Exception as e:
            logger.error(f"{reprt_name} 현금흐름표 조회 중 오류 발생: {str(e)}")
            return []

    def _add_year_labels(self, item: Dict[str, Any], year: int) -> None:
        """재무제표 항목에 연도 레이블을 추가합니다."""
        item["thstrm_nm"] = f"{year}년"
        item["frmtrm_nm"] = f"{year-1}년"
        item["bfefrmtrm_nm"] = f"{year-2}년" 