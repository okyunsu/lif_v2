import logging
from typing import Optional, List, Dict, Any, Union
from app.foundation.infra.database.supabase_client import supabase
from app.foundation.infra.utils.convert import convert_amount

logger = logging.getLogger(__name__)

# ===== 데이터 조회 함수 =====

async def get_company_info(company_name: str = None, corp_code: str = None) -> Optional[Dict[str, Any]]:
    """회사 정보를 조회합니다. 회사명 또는 회사 코드로 조회 가능합니다."""
    try:
        query = supabase.table("companies").select("*")
        
        if company_name:
            query = query.eq("corp_name", company_name)
        elif corp_code:
            query = query.eq("corp_code", corp_code)
        else:
            raise ValueError("회사명 또는 회사 코드를 입력해야 합니다.")
            
        response = query.limit(1).execute()
        logger.info(f"Company 조회 응답: {response}")
        
        if not response.data:
            return None
            
        return response.data[0]
    except Exception as e:
        logger.error(f"회사 정보 조회 중 오류 발생: {str(e)}")
        return None

async def get_financial_statements(
    company_name: str = None,
    corp_code: str = None,
    year: Optional[Union[int, str]] = None,
    limit_years: int = None
) -> List[Dict[str, Any]]:
    """재무제표 데이터를 조회합니다."""
    try:
        if company_name:
            corp_code = await _get_corp_code_by_name(company_name)
            if not corp_code:
                return []
        
        if not corp_code:
            raise ValueError("회사명 또는 회사 코드를 입력해야 합니다.")
            
        query = _build_financial_query(corp_code, year, limit_years)
        response = query.execute()
        logger.info(f"Financial 조회 응답: {response}")
        
        return response.data if response.data else []
    except Exception as e:
        logger.error(f"재무제표 데이터 조회 중 오류 발생: {str(e)}")
        return []

async def save_financial_statements(statements: List[Dict[str, Any]]) -> bool:
    """재무제표 데이터를 저장합니다."""
    if not statements:
        logger.warning("저장할 재무제표 데이터가 없습니다.")
        return False
        
    try:
        # 1. statement 테이블에 재무제표 유형 저장
        await _save_statement_types(statements)
        
        # 2. companies 테이블에 회사 정보 저장
        await _save_company_info(statements[0])
        
        # 3. reports 테이블에 보고서 정보 저장
        await _save_report_info(statements[0])
        
        # 4. financials 테이블에 재무제표 데이터 저장
        await _save_financial_data(statements)
        
        return True
    except Exception as e:
        logger.error(f"재무제표 데이터 저장 중 오류 발생: {str(e)}")
        return False

async def _get_corp_code_by_name(company_name: str) -> Optional[str]:
    """회사명으로 회사 코드를 조회합니다."""
    company_response = supabase.table("companies")\
        .select("corp_code")\
        .eq("corp_name", company_name)\
        .limit(1)\
        .execute()
    logger.info(f"Company 조회 응답: {company_response}")
    
    return company_response.data[0]["corp_code"] if company_response.data else None

def _build_financial_query(corp_code: str, year: Optional[Union[int, str]] = None, limit_years: int = None):
    """재무제표 조회 쿼리를 생성합니다."""
    query = supabase.table("financials")\
        .select("*, companies!inner(corp_name, stock_code), statement!inner(sj_nm)")\
        .eq("corp_code", corp_code)
        
    if year is not None:
        query = query.eq("bsns_year", str(year))
        
    if limit_years and not year:
        years = _get_recent_years(corp_code, limit_years)
        if years:
            query = query.in_("bsns_year", years)
            
    return query.order("bsns_year", desc=True).order("sj_div").order("ord")

def _get_recent_years(corp_code: str, limit: int) -> List[str]:
    """최근 N개 연도를 조회합니다."""
    years_query = supabase.table("financials")\
        .select("bsns_year")\
        .eq("corp_code", corp_code)\
        .order("bsns_year", desc=True)\
        .limit(limit)\
        .execute()
    logger.info(f"연도 조회 응답: {years_query}")
    
    return [item["bsns_year"] for item in years_query.data] if years_query.data else []

async def _save_statement_types(statements: List[Dict[str, Any]]) -> None:
    """재무제표 유형을 저장합니다."""
    for stmt in statements:
        response = supabase.table("statement").upsert({
            "sj_div": stmt["sj_div"],
            "sj_nm": stmt["sj_nm"]
        }).execute()
        logger.info(f"Statement 저장 응답: {response}")
        if not response.data:
            raise RuntimeError(f"Statement 저장 실패: {response}")

async def _save_company_info(statement: Dict[str, Any]) -> None:
    """회사 정보를 저장합니다."""
    try:
        existing_company = await get_company_info(corp_code=statement["corp_code"])
        
        company_data = {
            "corp_code": statement["corp_code"],
            "corp_name": existing_company["corp_name"] if existing_company else statement.get("corp_name", ""),
            "stock_code": existing_company["stock_code"] if existing_company else statement.get("stock_code", "")
        }
        
        if not company_data["corp_name"]:
            from app.domain.service.dart_api_service import DartApiService
            dart_api = DartApiService()
            company_info = await dart_api.get_company_info(company_data["corp_code"])
            if company_info:
                company_data["corp_name"] = company_info.corp_name
                company_data["stock_code"] = company_info.stock_code
            else:
                raise ValueError("회사명(corp_name)을 찾을 수 없습니다.")
            
        company_response = supabase.table("companies").upsert(company_data).execute()
        logger.info(f"Company 저장 응답: {company_response}")
        
        if not company_response.data:
            raise RuntimeError(f"Company 저장 실패: {company_response}")
    except Exception as e:
        logger.error(f"회사 정보 저장 중 오류 발생: {str(e)}")
        raise

async def _save_report_info(statement: Dict[str, Any]) -> None:
    """보고서 정보를 저장합니다."""
    report_response = supabase.table("reports").upsert({
        "rcept_no": statement["rcept_no"],
        "reprt_code": statement.get("reprt_code", "11011")
    }).execute()
    logger.info(f"Report 저장 응답: {report_response}")
    if not report_response.data:
        raise RuntimeError(f"Report 저장 실패: {report_response}")

async def _save_financial_data(statements: List[Dict[str, Any]]) -> None:
    """재무제표 데이터를 저장합니다."""
    for stmt in statements:
        if await _is_duplicate_financial_data(stmt):
            continue
            
        financial_data = _prepare_financial_data(stmt)
        financial_response = supabase.table("financials").upsert(financial_data).execute()
        logger.info(f"Financial 저장 응답: {financial_response}")
        if not financial_response.data:
            raise RuntimeError(f"Financial 저장 실패: {financial_response}")

async def _is_duplicate_financial_data(stmt: Dict[str, Any]) -> bool:
    """중복된 재무제표 데이터인지 확인합니다."""
    existing_data = supabase.table("financials")\
        .select("id")\
        .eq("corp_code", stmt["corp_code"])\
        .eq("bsns_year", stmt["bsns_year"])\
        .eq("sj_div", stmt["sj_div"])\
        .eq("account_nm", stmt["account_nm"])\
        .execute()
        
    if existing_data.data:
        logger.info(f"이미 존재하는 데이터 건너뛰기: {stmt['corp_code']} - {stmt['bsns_year']} - {stmt['sj_div']} - {stmt['account_nm']}")
        return True
    return False

def _prepare_financial_data(stmt: Dict[str, Any]) -> Dict[str, Any]:
    """재무제표 데이터를 저장 형식으로 변환합니다."""
    return {
        "corp_code": stmt["corp_code"],
        "bsns_year": stmt["bsns_year"],
        "sj_div": stmt["sj_div"],
        "account_nm": stmt["account_nm"],
        "thstrm_nm": stmt.get("thstrm_nm"),
        "thstrm_amount": convert_amount(stmt.get("thstrm_amount")),
        "frmtrm_nm": stmt.get("frmtrm_nm"),
        "frmtrm_amount": convert_amount(stmt.get("frmtrm_amount")),
        "bfefrmtrm_nm": stmt.get("bfefrmtrm_nm"),
        "bfefrmtrm_amount": convert_amount(stmt.get("bfefrmtrm_amount")),
        "ord": stmt.get("ord"),
        "currency": stmt.get("currency"),
        "rcept_no": stmt.get("rcept_no")
    }

async def get_existing_years(company_name: str) -> List[str]:
    """회사의 기존 데이터 연도 목록을 조회합니다."""
    try:
        # companies 테이블에서 corp_code 조회
        company_response = supabase.table("companies")\
            .select("corp_code")\
            .eq("corp_name", company_name)\
            .limit(1)\
            .execute()
            
        if company_response.status != 200:
            raise RuntimeError(f"회사 정보 조회 실패: {company_response.status} - {company_response.data}")
            
        if not company_response.data:
            return []
            
        corp_code = company_response.data[0]["corp_code"]
        
        # financials 테이블에서 연도 조회
        response = supabase.table("financials")\
            .select("bsns_year")\
            .eq("corp_code", corp_code)\
            .order("bsns_year", desc=True)\
            .execute()
            
        if response.status != 200:
            raise RuntimeError(f"연도 조회 실패: {response.status} - {response.data}")
            
        years = list({item["bsns_year"] for item in response.data}) if response.data else []
        return sorted(years)
    except Exception as e:
        logger.error(f"기존 연도 조회 중 오류 발생: {str(e)}")
        return []

async def check_existing_data(company_name: str, year: Optional[int] = None) -> List[Dict[str, Any]]:
    """기존 데이터를 확인합니다."""
    return await get_financial_statements(company_name=company_name, year=year)

async def get_financial_data(company_name: str, year: Optional[int] = None) -> List[Dict[str, Any]]:
    """저장된 재무제표 데이터를 조회합니다."""
    return await get_financial_statements(company_name=company_name, year=year, limit_years=3 if year is None else None)

async def get_key_financial_items(company_name: str = None) -> List[Dict[str, Any]]:
    """주요 재무 항목을 조회합니다."""
    try:
        query = supabase.table("financials")\
            .select("*, companies(corp_name, stock_code), statement(sj_nm)")\
            .in_("account_nm", [
                "자산총계", "부채총계", "자본총계", "유동자산", "유동부채",
                "매출액", "영업이익", "당기순이익", "영업활동현금흐름"
            ])
            
        if company_name:
            query = query.eq("corp_name", company_name)
            
        response = query.order("corp_code").order("bsns_year", desc=True).order("sj_div").order("account_nm").execute()
        
        if response.status_code != 200:
            raise RuntimeError(f"주요 재무 항목 조회 실패: {response.status_code} - {response.data}")
            
        return response.data if response.data else []
    except Exception as e:
        logger.error(f"주요 재무 항목 조회 중 오류 발생: {str(e)}")
        return []

async def get_statement_summary() -> List[Dict[str, Any]]:
    """회사별 재무제표 종류와 데이터 수를 조회합니다."""
    try:
        response = supabase.table("financials")\
            .select("corp_code, corp_name, sj_div, sj_nm, count")\
            .group("corp_code, corp_name, sj_div, sj_nm")\
            .order("corp_code")\
            .order("sj_div")\
            .execute()
            
        if response.status_code != 200:
            raise RuntimeError(f"재무제표 요약 조회 실패: {response.status_code} - {response.data}")
            
        return response.data if response.data else []
    except Exception as e:
        logger.error(f"재무제표 요약 조회 중 오류 발생: {str(e)}")
        return []

async def get_financial_statements_by_corp_code(corp_code: str) -> List[Dict[str, Any]]:
    """회사 코드로 재무제표 데이터를 조회합니다."""
    try:
        response = supabase.table("financials")\
            .select("*")\
            .eq("corp_code", corp_code)\
            .order("bsns_year", desc=True)\
            .order("sj_div")\
            .order("ord")\
            .execute()
            
        if response.status_code != 200:
            raise RuntimeError(f"재무제표 데이터 조회 실패: {response.status_code} - {response.data}")
            
        return response.data if response.data else []
    except Exception as e:
        logger.error(f"재무제표 데이터 조회 중 오류 발생: {str(e)}")
        return []

async def save_financial_ratios(ratios: Dict[str, Any]) -> None:
    """재무비율을 저장합니다."""
    try:
        response = supabase.table("metrics").upsert({
            "corp_code": ratios["corp_code"],
            "corp_name": ratios["corp_name"],
            "bsns_year": ratios["bsns_year"],
            "debt_ratio": ratios.get("debt_ratio"),
            "current_ratio": ratios.get("current_ratio"),
            "interest_coverage_ratio": ratios.get("interest_coverage_ratio"),
            "operating_profit_ratio": ratios.get("operating_profit_ratio"),
            "net_profit_ratio": ratios.get("net_profit_ratio"),
            "roe": ratios.get("roe"),
            "roa": ratios.get("roa"),
            "debt_dependency": ratios.get("debt_dependency"),
            "cash_flow_debt_ratio": ratios.get("cash_flow_debt_ratio"),
            "sales_growth": ratios.get("sales_growth"),
            "operating_profit_growth": ratios.get("operating_profit_growth"),
            "eps_growth": ratios.get("eps_growth")
        }).execute()
        
        if response.status_code != 200:
            raise RuntimeError(f"재무비율 저장 실패: {response.status_code} - {response.data}")
    except Exception as e:
        logger.error(f"재무비율 저장 중 오류 발생: {str(e)}")
        raise