import os
import logging
import aiohttp
import zipfile
import xml.etree.ElementTree as ET
from io import BytesIO
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timedelta

from app.foundation.core.config.settings import settings
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

# KOSPI 100 기업 리스트 (2024년 기준)
KOSPI_100_COMPANIES = [
    "삼성전자", "SK하이닉스", "LG에너지솔루션", "삼성바이오로직스", "POSCO홀딩스", 
    "삼성SDI", "LG화학", "현대차", "기아", "NAVER", 
    "KB금융", "카카오", "삼성물산", "신한지주", "SK이노베이션", 
    "포스코퓨처엠", "하나금융지주", "삼성생명", "LG", "SK텔레콤", 
    "한국전력", "SK", "현대모비스", "KT&G", "한화솔루션", 
    "우리금융지주", "HD현대중공업", "한국조선해양", "기업은행", "현대건설", 
    "두산에너빌리티", "고려아연", "KT", "S-Oil", "롯데케미칼",
    "삼성화재", "HK이노엔", "LG디스플레이", "KG스틸", "엔씨소프트", 
    "현대글로비스", "SK스퀘어", "LG생활건강", "CJ제일제당", "LG유플러스", 
    "현대제철", "금호석유화학", "한온시스템", "코웨이", "한미사이언스", 
    "아모레퍼시픽", "삼성에스디에스", "두산밥캣", "호텔신라", "한미약품", 
    "한화시스템", "하이브", "KODEX 200", "셀트리온", "크래프톤", 
    "DB손해보험", "넷마블", "현대위아", "강원랜드", "GS건설", 
    "SK바이오사이언스", "대한항공", "쌍용C&E", "한화생명", "LS", 
    "한국항공우주", "포스코인터내셔널", "SK케미칼", "한국가스공사", "효성첨단소재", 
    "대우건설", "미래에셋증권", "두산", "효성티앤씨", "이마트", 
    "SKC", "한샘", "삼성엔지니어링", "현대로템", "효성", 
    "LIG넥스원", "한국금융지주", "현대미포조선", "GS", "OCI", 
    "HD현대", "롯데쇼핑", "현대두산인프라코어", "녹십자", "삼성전기", 
    "NH투자증권", "한국타이어앤테크놀로지", "CJ대한통운", "만도", "한솔케미칼"
]

class DartApiService:
    """
    DART API 통신 서비스
    
    DART API를 통해 회사 정보, 재무제표, 보고서 등의 데이터를 조회합니다.
    """
    
    def __init__(self):
        """DART API 서비스 초기화"""
        self.api_key = settings.DART_API_KEY
        self.endpoints = settings.DART_ENDPOINTS
        logger.info("DartApiService가 초기화되었습니다.")

    async def fetch_top_companies(self, limit: int = 100) -> List[CompanySchema]:
        """KOSPI 100 회사 목록을 조회합니다."""
        logger.info("KOSPI 100 회사 조회 시작")
        
        try:
            content = await self._make_api_request(self.endpoints["CORP_CODE"], {"crtfc_key": self.api_key})
            companies = await self._parse_company_xml(content, limit)
            logger.info(f"KOSPI 100 회사 조회 완료: 총 {len(companies)}개 회사")
            return companies
        except Exception as e:
            logger.error(f"회사 목록 조회 실패: {str(e)}")
            raise

    async def check_new_report_available(self, corp_code: str, year: int) -> bool:
        """새로운 보고서가 있는지 확인합니다."""
        # 현재 연도가 아닌 이전 연도의 보고서를 확인
        target_year = year - 1
        
        params = {
            "crtfc_key": self.api_key,
            "corp_code": corp_code,
            "bgn_de": f"{target_year}0101",
            "end_de": f"{target_year}1231",
            "pblntf_ty": "A001"  # 사업보고서
        }

        try:
            data = await self._make_json_api_request(self.endpoints["REPORT_LIST"], params)
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
            content = await self._make_api_request(self.endpoints["CORP_CODE"], {"crtfc_key": self.api_key})
            company = await self._find_company_by_name(content, company_name)
            logger.info(f"회사 정보를 찾았습니다: {company_name}")
            return company
        except Exception as e:
            logger.error(f"회사 정보 조회 실패: {str(e)}")
            raise

    async def get_company_info(self, corp_code: str) -> Optional[CompanySchema]:
        """회사 코드로 회사 정보를 조회합니다."""
        logger.info(f"회사 정보 조회 시작 (corp_code: {corp_code})")
        
        try:
            content = await self._make_api_request(self.endpoints["CORP_CODE"], {"crtfc_key": self.api_key})
            with zipfile.ZipFile(BytesIO(content)) as zip_file:
                with zip_file.open('CORPCODE.xml') as xml_file:
                    tree = ET.parse(xml_file)
                    root = tree.getroot()
                    
                    # 회사 코드로 회사 정보 검색
                    company = root.find(f'.//list[corp_code="{corp_code}"]')
                    if company is not None:
                        now = datetime.now().isoformat()
                        result = CompanySchema(
                            corp_code=corp_code,
                            corp_name=company.findtext('corp_name'),
                            stock_code=company.findtext('stock_code') or "",
                            created_at=now,
                            updated_at=now
                        )
                        logger.info(f"회사 정보를 찾았습니다: {result.corp_name}")
                        return result
                    
                    logger.error(f"회사 코드 '{corp_code}'에 해당하는 회사를 찾을 수 없습니다.")
                    return None
        except Exception as e:
            logger.error(f"회사 정보 조회 실패: {str(e)}")
            return None

    async def fetch_financial_statements(self, corp_code: str, year: Optional[int] = None) -> List[Dict[str, Any]]:
        """DART API에서 재무제표 데이터를 조회합니다."""
        logger.info(f"재무제표 조회 시작 - corp_code: {corp_code}, year: {year}")
        
        # 연도 설정
        current_year = datetime.now().year
        target_year = await self._determine_target_year(year, current_year)
        
        # 사업보고서 조회
        statements = []
        
        # 사업보고서만 조회 (연간 보고서)
        report_code = "11011"  # 사업보고서 코드
        
        # 연도별로 시도 (최신부터 과거순으로)
        years_to_try = [target_year, target_year - 1, target_year - 2]
        logger.info(f"[{corp_code}] 조회할 연도 범위: {years_to_try}")
        
        for try_year in years_to_try:
            logger.info(f"[{corp_code}] {try_year}년도 사업보고서 조회 시도")
            
            # 재무제표 유형 시도 (연결재무제표, 일반재무제표)
            for fs_div, fs_name in [("CFS", "연결재무제표"), ("OFS", "일반재무제표")]:
                # 기본 파라미터 설정
                params = {
                    "crtfc_key": self.api_key,
                    "corp_code": corp_code,
                    "bsns_year": str(try_year),
                    "reprt_code": report_code,
                    "fs_div": fs_div
                }
                
                logger.info(f"[{corp_code}] {try_year}년도 사업보고서 {fs_name} 조회 시도")
                
                try:
                    # API 요청
                    data = await self._make_json_api_request(self.endpoints["FINANCIAL_STATEMENT"], params)
                    api_response = DartApiResponse(**data)
                    
                    if api_response.status != "000":
                        logger.warning(f"[{corp_code}] {try_year}년도 사업보고서 {fs_name} API 응답 실패: {api_response.message}")
                        continue
                    
                    if not api_response.list:
                        logger.warning(f"[{corp_code}] {try_year}년도 사업보고서 {fs_name} API 응답은 성공했으나 데이터가 없습니다.")
                        continue
                    
                    # 결과 처리
                    for item in api_response.list:
                        self._add_year_labels(item, try_year)
                        statements.append(item)
                    
                    logger.info(f"[{corp_code}] {try_year}년도 사업보고서 {fs_name} 데이터 조회 성공 (항목 {len(api_response.list)}개)")
                    return statements  # 데이터를 찾았으면 즉시 반환
                    
                except Exception as e:
                    logger.error(f"[{corp_code}] {try_year}년도 사업보고서 {fs_name} 조회 중 오류 발생: {str(e)}")
        
        if not statements:
            logger.warning(f"[{corp_code}] 모든 연도에서 사업보고서 재무제표를 찾지 못했습니다.")
        
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

    async def _parse_company_xml(self, content: bytes, limit: int) -> List[CompanySchema]:
        """회사 정보 XML을 파싱하여 CompanySchema 리스트로 반환합니다."""
        companies = []
        found_companies = set()  # 중복 방지를 위한 집합
        
        # 회사명 -> 상장법인 정보 매핑을 위한 딕셔너리
        company_map = {}
        
        with zipfile.ZipFile(BytesIO(content)) as zip_file:
            with zip_file.open('CORPCODE.xml') as xml_file:
                tree = ET.parse(xml_file)
                root = tree.getroot()
                
                # 1단계: 모든 상장법인 정보를 수집합니다
                for company in root.findall('.//list'):
                    corp_name = company.findtext('corp_name')
                    stock_code = company.findtext('stock_code')
                    corp_code = company.findtext('corp_code')
                    
                    # 주식 코드가 있는 경우만 상장법인으로 간주
                    if corp_name and stock_code and stock_code.strip():
                        # 아직 해당 회사명이 딕셔너리에 없거나, 이미 있는데 KOSPI 100에 포함된 회사라면 업데이트
                        if corp_name not in company_map or corp_name in KOSPI_100_COMPANIES:
                            company_map[corp_name] = {
                                'corp_code': corp_code,
                                'stock_code': stock_code,
                                'element': company
                            }
                
                # 2단계: KOSPI 100 기업 목록에 있는 회사들을 찾습니다
                for kospi_company in KOSPI_100_COMPANIES:
                    if kospi_company in company_map and kospi_company not in found_companies:
                        company_info = company_map[kospi_company]
                        found_companies.add(kospi_company)
                        
                        now = datetime.now().isoformat()
                        companies.append(CompanySchema(
                            corp_code=company_info['corp_code'],
                            corp_name=kospi_company,
                            stock_code=company_info['stock_code'],
                            created_at=now,
                            updated_at=now
                        ))
                        
                        logger.info(f"KOSPI 100 회사 '{kospi_company}'을(를) 찾았습니다. (corp_code: {company_info['corp_code']}, stock_code: {company_info['stock_code']})")
                        
                        # 최대 limit에 도달하면 종료
                        if len(companies) >= limit:
                            break
        
        # 결과 분석
        found_count = len(companies)
        missing_count = len(KOSPI_100_COMPANIES) - found_count
        
        if missing_count > 0:
            # 찾지 못한 회사 목록 구하기
            missing_companies = set(KOSPI_100_COMPANIES) - found_companies
            logger.warning(f"KOSPI 100 기업 중 {missing_count}개 기업을 찾지 못했습니다.")
            logger.warning(f"찾지 못한 기업: {', '.join(list(missing_companies)[:10])}{'...' if len(missing_companies) > 10 else ''}")
        
        logger.info(f"KOSPI 100 기업 중 {found_count}개 기업 정보를 찾았습니다.")
        return companies

    async def _find_company_by_name(self, content: bytes, company_name: str) -> CompanySchema:
        """회사명으로 회사 정보를 찾아 반환합니다. 동명 회사가 있으면 유가증권시장 상장법인을 우선합니다."""
        with zipfile.ZipFile(BytesIO(content)) as zip_file:
            with zip_file.open('CORPCODE.xml') as xml_file:
                tree = ET.parse(xml_file)
                root = tree.getroot()
                
                # 일반 회사 검색 로직
                candidates = []  # 후보 회사들
                
                # 모든 가능한 회사 정보 수집
                for company in root.findall('.//list'):
                    if company.findtext('corp_name') == company_name:
                        stock_code = company.findtext('stock_code')
                        corp_code = company.findtext('corp_code')
                        
                        # 상장사 여부와 stock_code 길이에 따라 점수 부여
                        score = 0
                        if stock_code and stock_code.strip():
                            score += 100  # 상장사는 높은 점수
                            # 유가증권시장은 보통 6자리 숫자 코드
                            if len(stock_code.strip()) == 6:
                                score += 50
                        
                        candidates.append({
                            'element': company,
                            'corp_code': corp_code,
                            'stock_code': stock_code or "",
                            'score': score
                        })
                
                # 후보가 있으면 점수 순으로 정렬하여 최고 점수 회사 선택
                if candidates:
                    candidates.sort(key=lambda x: x['score'], reverse=True)
                    best_candidate = candidates[0]
                    
                    now = datetime.now().isoformat()
                    result = CompanySchema(
                        corp_code=best_candidate['corp_code'],
                        corp_name=company_name,
                        stock_code=best_candidate['stock_code'],
                        created_at=now,
                        updated_at=now
                    )
                    
                    logger.info(f"회사 '{company_name}'을(를) 찾았습니다. (corp_code: {result.corp_code}, stock_code: {result.stock_code}, 점수: {best_candidate['score']})")
                    return result
                
                # 회사를 전혀 찾지 못한 경우
                logger.error(f"회사명 '{company_name}'을 찾을 수 없습니다.")
                raise ValueError(f"회사명 '{company_name}'을 찾을 수 없습니다.")

    async def _determine_target_year(self, year: Optional[int], current_year: int) -> int:
        """조회할 연도를 결정합니다."""
        if year is None or not isinstance(year, int):
            target_year = current_year - 1
            logger.info(f"연도가 지정되지 않아 {target_year}년도 데이터를 조회합니다.")
        else:
            target_year = year
            logger.info(f"{target_year}년도 데이터를 조회합니다.")
        return target_year

    def _add_year_labels(self, item: Dict[str, Any], year: int) -> None:
        """재무제표 항목에 연도 레이블을 추가합니다."""
        item["thstrm_nm"] = f"{year}년"
        item["frmtrm_nm"] = f"{year-1}년"
        item["bfefrmtrm_nm"] = f"{year-2}년" 