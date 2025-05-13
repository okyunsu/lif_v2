from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
import logging
from typing import Optional, List, Dict, Any, Callable

logger = logging.getLogger(__name__)

# 공통 유틸리티 함수
async def execute_query(db_session: AsyncSession, query: text, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    """쿼리를 실행하고 결과를 딕셔너리 리스트로 반환합니다."""
    result = await db_session.execute(query, params or {})
    return [dict(row) for row in result.mappings().all()]

async def execute_transaction(db_session: AsyncSession, callback: Callable) -> Any:
    """트랜잭션을 실행합니다."""
    try:
        result = await callback()
        await db_session.commit()
        return result
    except Exception as e:
        await db_session.rollback()
        logger.error(f"트랜잭션 실행 중 오류 발생: {str(e)}")
        raise

# 기본 CRUD 함수
async def get_company_by_name(db_session: AsyncSession, company_name: str) -> Optional[Dict[str, Any]]:
    """회사명으로 회사 정보를 조회합니다."""
    query = text("""
        SELECT corp_code, corp_name, stock_code
        FROM companies
        WHERE corp_name = :company_name
        LIMIT 1
    """)
    result = await db_session.execute(query, {"company_name": company_name})
    row = result.fetchone()
    if row:
        return {
            "corp_code": row[0],
            "corp_name": row[1],
            "stock_code": row[2]
        }
    return None

async def get_financial_data(db_session: AsyncSession, company_name: str, year: Optional[int] = None) -> List[Dict[str, Any]]:
    """DB에서 재무제표 데이터를 조회합니다."""
    try:
        params = {"company_name": company_name}
        
        # 연도별 조건 추가
        year_condition = ""
        if year is not None:
            year_condition = "AND f.bsns_year = :year"
            params["year"] = str(year)
        else:
            # 최근 3개 연도 데이터 조회
            year_condition = """AND f.bsns_year IN (
                SELECT DISTINCT bsns_year 
                FROM financials f2
                JOIN companies c2 ON f2.corp_code = c2.corp_code
                WHERE c2.corp_name = :company_name
                ORDER BY bsns_year DESC
                LIMIT 3
            )"""
        
        # 쿼리 수정: JOIN 구문 명확히 하고 필요한 모든 필드 포함
        query = text(f"""
            SELECT 
                f.bsns_year, 
                f.sj_div, 
                s.sj_nm, 
                f.account_nm, 
                f.thstrm_amount, 
                f.frmtrm_amount, 
                f.bfefrmtrm_amount,
                c.corp_code,
                c.corp_name,
                c.stock_code
            FROM financials f
            JOIN companies c ON f.corp_code = c.corp_code
            JOIN statement s ON f.sj_div = s.sj_div
            WHERE c.corp_name = :company_name
            {year_condition}
            ORDER BY f.bsns_year DESC, f.sj_div, f.ord
        """)
        
        result = await execute_query(db_session, query, params)
        logger.info(f"{company_name}의 재무제표 데이터 {len(result)}개 조회 완료")
        return result
    except Exception as e:
        logger.error(f"데이터 조회 중 오류 발생: {str(e)}")
        raise  # 오류를 상위로 전파하여 문제 해결 가능하도록 함

async def get_saved_financial_ratios(db_session: AsyncSession, company_name: str, years: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """저장된 재무비율을 조회합니다."""
    try:
        params = {"company_name": company_name}
        
        # 연도 조건 추가
        year_condition = ""
        if years:
            if len(years) == 1:
                year_condition = "AND bsns_year = :year"
                params["year"] = years[0]
            else:
                year_condition = "AND bsns_year = ANY(:years)"
                params["years"] = years
        
        query = text(f"""
            SELECT 
                corp_code, corp_name, bsns_year,
                debt_ratio, current_ratio, interest_coverage_ratio,
                operating_profit_ratio, net_profit_ratio, roe, roa,
                debt_dependency, cash_flow_debt_ratio,
                sales_growth, operating_profit_growth, eps_growth
            FROM financial_ratios
            WHERE corp_name = :company_name
            {year_condition}
            ORDER BY bsns_year DESC
        """)
        
        return await execute_query(db_session, query, params)
    except Exception as e:
        logger.error(f"저장된 재무비율 조회 중 오류 발생: {str(e)}")
        return []

async def save_financial_statements(db_session: AsyncSession, statements: List[Dict[str, Any]]) -> None:
    """재무제표 데이터를 저장합니다."""
    await execute_transaction(db_session, lambda: _save_financial_statements(db_session, statements))

async def _save_financial_statements(db_session: AsyncSession, statements: List[Dict[str, Any]]) -> None:
    """재무제표 데이터를 저장합니다. (내부 함수)"""
    # 1. statement 테이블에 재무제표 유형 저장
    for stmt in statements:
        insert_statement_query = text("""
            INSERT INTO statement (sj_div, sj_nm)
            VALUES (:sj_div, :sj_nm)
            ON CONFLICT (sj_div) DO NOTHING
        """)
        await db_session.execute(insert_statement_query, {
            "sj_div": stmt["sj_div"],
            "sj_nm": stmt["sj_nm"]
        })

    # 2. companies 테이블에 회사 정보 저장
    if statements:
        insert_company_query = text("""
            INSERT INTO companies (corp_code, corp_name, stock_code)
            VALUES (:corp_code, :corp_name, :stock_code)
            ON CONFLICT (corp_code) DO UPDATE SET
                corp_name = EXCLUDED.corp_name,
                stock_code = EXCLUDED.stock_code
        """)
        await db_session.execute(insert_company_query, {
            "corp_code": statements[0]["corp_code"],
            "corp_name": statements[0]["corp_name"],
            "stock_code": statements[0].get("stock_code", "")
        })

    # 3. reports 테이블에 보고서 정보 저장
    if statements and "rcept_no" in statements[0]:
        insert_report_query = text("""
            INSERT INTO reports (rcept_no, reprt_code)
            VALUES (:rcept_no, :reprt_code)
            ON CONFLICT (rcept_no) DO NOTHING
        """)
        await db_session.execute(insert_report_query, {
            "rcept_no": statements[0]["rcept_no"],
            "reprt_code": statements[0].get("reprt_code", "11011")  # 사업보고서 코드
        })

    # 4. financials 테이블에 재무제표 데이터 저장
    for stmt in statements:
        insert_financial_query = text("""
            INSERT INTO financials (
                corp_code, bsns_year, sj_div, account_nm,
                thstrm_nm, thstrm_amount,
                frmtrm_nm, frmtrm_amount,
                bfefrmtrm_nm, bfefrmtrm_amount,
                ord, currency, rcept_no
            ) VALUES (
                :corp_code, :bsns_year, :sj_div, :account_nm,
                :thstrm_nm, :thstrm_amount,
                :frmtrm_nm, :frmtrm_amount,
                :bfefrmtrm_nm, :bfefrmtrm_amount,
                :ord, :currency, :rcept_no
            )
            ON CONFLICT (corp_code, bsns_year, sj_div, account_nm) DO UPDATE SET
                thstrm_nm = EXCLUDED.thstrm_nm,
                thstrm_amount = EXCLUDED.thstrm_amount,
                frmtrm_nm = EXCLUDED.frmtrm_nm,
                frmtrm_amount = EXCLUDED.frmtrm_amount,
                bfefrmtrm_nm = EXCLUDED.bfefrmtrm_nm,
                bfefrmtrm_amount = EXCLUDED.bfefrmtrm_amount,
                ord = EXCLUDED.ord,
                currency = EXCLUDED.currency,
                rcept_no = EXCLUDED.rcept_no,
                updated_at = CURRENT_TIMESTAMP
        """)
        await db_session.execute(insert_financial_query, stmt)

async def delete_financial_statements(
    db_session: AsyncSession,
    corp_code: str,
    bsns_year: str
) -> None:
    """재무제표 데이터를 삭제합니다."""
    await execute_transaction(db_session, lambda: _delete_financial_statements(db_session, corp_code, bsns_year))

async def _delete_financial_statements(db_session: AsyncSession, corp_code: str, bsns_year: str) -> None:
    """재무제표 데이터를 삭제합니다. (내부 함수)"""
    delete_query = text("""
        DELETE FROM financials 
        WHERE corp_code = :corp_code 
        AND bsns_year = :bsns_year
    """)
    await db_session.execute(delete_query, {
        "corp_code": corp_code,
        "bsns_year": bsns_year
    })

async def save_financial_ratios(db_session: AsyncSession, ratios: Dict[str, Any]) -> None:
    """재무비율을 저장합니다."""
    await execute_transaction(db_session, lambda: _save_financial_ratios(db_session, ratios))

async def _save_financial_ratios(db_session: AsyncSession, ratios: Dict[str, Any]) -> None:
    """재무비율을 저장합니다. (내부 함수)"""
    query = text("""
        INSERT INTO financial_ratios (
            corp_code, corp_name, bsns_year,
            debt_ratio, current_ratio, interest_coverage_ratio,
            operating_profit_ratio, net_profit_ratio, roe, roa,
            debt_dependency, cash_flow_debt_ratio,
            sales_growth, operating_profit_growth, eps_growth
        ) VALUES (
            :corp_code, :corp_name, :bsns_year,
            :debt_ratio, :current_ratio, :interest_coverage_ratio,
            :operating_profit_ratio, :net_profit_ratio, :roe, :roa,
            :debt_dependency, :cash_flow_debt_ratio,
            :sales_growth, :operating_profit_growth, :eps_growth
        )
        ON CONFLICT (corp_code, bsns_year) 
        DO UPDATE SET
            debt_ratio = EXCLUDED.debt_ratio,
            current_ratio = EXCLUDED.current_ratio,
            interest_coverage_ratio = EXCLUDED.interest_coverage_ratio,
            operating_profit_ratio = EXCLUDED.operating_profit_ratio,
            net_profit_ratio = EXCLUDED.net_profit_ratio,
            roe = EXCLUDED.roe,
            roa = EXCLUDED.roa,
            debt_dependency = EXCLUDED.debt_dependency,
            cash_flow_debt_ratio = EXCLUDED.cash_flow_debt_ratio,
            sales_growth = EXCLUDED.sales_growth,
            operating_profit_growth = EXCLUDED.operating_profit_growth,
            eps_growth = EXCLUDED.eps_growth
    """)
    await db_session.execute(query, ratios)