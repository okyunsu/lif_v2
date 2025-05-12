from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
import logging
from typing import Optional, List, Dict, Any, Tuple, Union

logger = logging.getLogger(__name__)

# ===== 데이터 조회 함수 =====

async def execute_query(
    db_session: AsyncSession, 
    query: str, 
    params: Dict[str, Any] = None
) -> List[Dict[str, Any]]:
    """SQL 쿼리를 실행하고 결과를 딕셔너리 리스트로 반환합니다."""
    try:
        result = await db_session.execute(text(query), params or {})
        return [
            {column: row[idx] for idx, column in enumerate(result.keys())}
            for row in result
        ]
    except Exception as e:
        logger.error(f"쿼리 실행 중 오류 발생: {str(e)}")
        raise

async def get_company_info(
    db_session: AsyncSession, 
    company_name: str = None, 
    corp_code: str = None
) -> Optional[Dict[str, Any]]:
    """회사 정보를 조회합니다. 회사명 또는 회사 코드로 조회 가능합니다."""
    try:
        if company_name:
            query = """
                SELECT DISTINCT corp_code, corp_name, stock_code
                FROM companies
                WHERE corp_name = :company_name
                LIMIT 1
            """
            params = {"company_name": company_name}
        elif corp_code:
            query = """
                SELECT DISTINCT corp_code, corp_name, stock_code
                FROM companies
                WHERE corp_code = :corp_code
                LIMIT 1
            """
            params = {"corp_code": corp_code}
        else:
            raise ValueError("회사명 또는 회사 코드를 입력해야 합니다.")
            
        results = await execute_query(db_session, query, params)
        return results[0] if results else None
    except Exception as e:
        logger.error(f"회사 정보 조회 중 오류 발생: {str(e)}")
        return None

async def get_financial_statements(
    db_session: AsyncSession,
    company_name: str = None,
    corp_code: str = None,
    year: Optional[Union[int, str]] = None,
    limit_years: int = None
) -> List[Dict[str, Any]]:
    """재무제표 데이터를 조회합니다.
    
    Args:
        db_session: 데이터베이스 세션
        company_name: 회사명 (corp_code와 함께 사용 불가)
        corp_code: 회사 코드 (company_name과 함께 사용 불가)
        year: 특정 연도 (없으면 전체 연도)
        limit_years: 최근 몇 개 연도를 조회할지 (없으면 모든 연도)
    """
    try:
        # 기본 쿼리 부분
        base_query = """
            SELECT f.bsns_year, f.sj_div, s.sj_nm, f.account_nm, 
                   f.thstrm_amount, f.frmtrm_amount, f.bfefrmtrm_amount,
                   f.corp_code, c.corp_name, c.stock_code, f.ord
            FROM financials f
            JOIN companies c ON f.corp_code = c.corp_code
            JOIN statement s ON f.sj_div = s.sj_div
        """
        
        # WHERE 조건 및 파라미터 구성
        where_conditions = []
        params = {}
        
        if company_name:
            where_conditions.append("c.corp_name = :company_name")
            params["company_name"] = company_name
        elif corp_code:
            where_conditions.append("f.corp_code = :corp_code")
            params["corp_code"] = corp_code
        else:
            raise ValueError("회사명 또는 회사 코드를 입력해야 합니다.")
            
        if year is not None:
            where_conditions.append("f.bsns_year = :year")
            params["year"] = str(year)
            
        # WHERE 조건 추가
        if where_conditions:
            base_query += " WHERE " + " AND ".join(where_conditions)
        
        # 최근 N개 연도로 제한
        if limit_years and not year:
            base_query = f"""
                {base_query}
                AND f.bsns_year IN (
                    SELECT DISTINCT bsns_year 
                    FROM financials f2
                    JOIN companies c2 ON f2.corp_code = c2.corp_code
                    WHERE {"c2.corp_name = :company_name" if company_name else "f2.corp_code = :corp_code"}
                    ORDER BY bsns_year DESC
                    LIMIT {limit_years}
                )
            """
        
        # 정렬 추가
        base_query += " ORDER BY f.bsns_year DESC, f.sj_div, f.ord"
        
        return await execute_query(db_session, base_query, params)
    except Exception as e:
        logger.error(f"재무제표 데이터 조회 중 오류 발생: {str(e)}")
        return []

async def get_existing_years(db_session: AsyncSession, company_name: str) -> List[str]:
    """회사의 기존 데이터 연도 목록을 조회합니다."""
    try:
        query = """
            SELECT DISTINCT bsns_year 
            FROM financials f
            JOIN companies c ON f.corp_code = c.corp_code
            WHERE c.corp_name = :company_name
            ORDER BY bsns_year DESC
        """
        results = await execute_query(db_session, query, {"company_name": company_name})
        return [row["bsns_year"] for row in results]
    except Exception as e:
        logger.error(f"기존 연도 조회 중 오류 발생: {str(e)}")
        return []

async def check_existing_data(db_session: AsyncSession, company_name: str, year: Optional[int] = None) -> List[Dict[str, Any]]:
    """기존 데이터를 확인합니다."""
    return await get_financial_statements(db_session, company_name=company_name, year=year)

async def get_financial_data(db_session: AsyncSession, company_name: str, year: Optional[int] = None) -> List[Dict[str, Any]]:
    """저장된 재무제표 데이터를 조회합니다."""
    return await get_financial_statements(db_session, company_name=company_name, year=year, limit_years=3 if year is None else None)

async def get_key_financial_items(db_session: AsyncSession, company_name: str = None) -> List[Dict[str, Any]]:
    """주요 재무 항목을 조회합니다."""
    try:
        query = """
            SELECT 
                corp_code, corp_name, bsns_year, sj_div, sj_nm,
                account_nm, thstrm_amount, frmtrm_amount, bfefrmtrm_amount
            FROM financials f
            JOIN companies c ON f.corp_code = c.corp_code
            JOIN statement s ON f.sj_div = s.sj_div
            WHERE account_nm IN (
                '자산총계', '부채총계', '자본총계', '유동자산', '유동부채',
                '매출액', '영업이익', '당기순이익', '영업활동현금흐름'
            )
        """
        
        params = {}
        if company_name:
            query += " AND c.corp_name = :company_name"
            params["company_name"] = company_name
            
        query += " ORDER BY corp_code, bsns_year DESC, sj_div, account_nm"
        
        return await execute_query(db_session, query, params)
    except Exception as e:
        logger.error(f"주요 재무 항목 조회 중 오류 발생: {str(e)}")
        return []

# ===== 데이터 저장 및 삭제 함수 =====

async def execute_transaction(
    db_session: AsyncSession, 
    queries: List[Tuple[str, Dict[str, Any]]]
) -> bool:
    """여러 SQL 쿼리를 트랜잭션으로 실행합니다."""
    try:
        for query_str, params in queries:
            await db_session.execute(text(query_str), params or {})
        await db_session.commit()
        return True
    except Exception as e:
        await db_session.rollback()
        logger.error(f"트랜잭션 실행 중 오류 발생: {str(e)}")
        raise

async def delete_financial_statements(
    db_session: AsyncSession,
    corp_code: str,
    bsns_year: str
) -> bool:
    """재무제표 데이터를 삭제합니다."""
    try:
        query = """
            DELETE FROM financials 
            WHERE corp_code = :corp_code 
            AND bsns_year = :bsns_year
        """
        params = {"corp_code": corp_code, "bsns_year": bsns_year}
        await execute_transaction(db_session, [(query, params)])
        return True
    except Exception as e:
        logger.error(f"재무제표 데이터 삭제 중 오류 발생: {str(e)}")
        return False

async def save_financial_statements(db_session: AsyncSession, statements: List[Dict[str, Any]]) -> bool:
    """재무제표 데이터를 저장합니다."""
    if not statements:
        logger.warning("저장할 재무제표 데이터가 없습니다.")
        return False
        
    try:
        queries = []
        
        # 1. statement 테이블에 재무제표 유형 저장
        for stmt in statements:
            insert_statement_query = """
                INSERT INTO statement (sj_div, sj_nm)
                VALUES (:sj_div, :sj_nm)
                ON CONFLICT (sj_div) DO NOTHING
            """
            queries.append((insert_statement_query, {
                "sj_div": stmt["sj_div"],
                "sj_nm": stmt["sj_nm"]
            }))

        # 2. companies 테이블에 회사 정보 저장
        insert_company_query = """
            INSERT INTO companies (corp_code, corp_name, stock_code)
            VALUES (:corp_code, :corp_name, :stock_code)
            ON CONFLICT (corp_code) DO UPDATE SET
                corp_name = EXCLUDED.corp_name,
                stock_code = EXCLUDED.stock_code
        """
        queries.append((insert_company_query, {
            "corp_code": statements[0]["corp_code"],
            "corp_name": statements[0]["corp_name"],
            "stock_code": statements[0].get("stock_code", "")
        }))

        # 3. reports 테이블에 보고서 정보 저장
        insert_report_query = """
            INSERT INTO reports (rcept_no, reprt_code)
            VALUES (:rcept_no, :reprt_code)
            ON CONFLICT (rcept_no) DO NOTHING
        """
        queries.append((insert_report_query, {
            "rcept_no": statements[0]["rcept_no"],
            "reprt_code": statements[0].get("reprt_code", "11011")  # 사업보고서 코드
        }))

        # 4. financials 테이블에 재무제표 데이터 저장
        for stmt in statements:
            insert_financial_query = """
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
            """
            queries.append((insert_financial_query, stmt))

        await execute_transaction(db_session, queries)
        return True
    except Exception as e:
        logger.error(f"재무제표 데이터 저장 중 오류 발생: {str(e)}")
        return False

async def insert_financial_statement(db_session: AsyncSession, data: Dict[str, Any]) -> None:
    """재무제표 데이터를 저장합니다."""
    query = text("""
        INSERT INTO fin_data (
            corp_code, corp_name, stock_code, bsns_year, sj_div, sj_nm, 
            account_nm, thstrm_amount, frmtrm_amount, bfefrmtrm_amount, ord
        ) VALUES (
            :corp_code, :corp_name, :stock_code, :bsns_year, :sj_div, :sj_nm,
            :account_nm, :thstrm_amount, :frmtrm_amount, :bfefrmtrm_amount, :ord
        )
    """)
    await db_session.execute(query, data)
    await db_session.commit()

async def get_statement_summary(db_session: AsyncSession) -> List[Dict[str, Any]]:
    """회사별 재무제표 종류와 데이터 수를 조회합니다."""
    query = text("""
        SELECT corp_code, corp_name, sj_div, sj_nm, COUNT(*) as count
        FROM fin_data
        GROUP BY corp_code, corp_name, sj_div, sj_nm
        ORDER BY corp_code, sj_div
    """)
    result = await db_session.execute(query)
    return [dict(row) for row in result]

async def get_financial_statements_by_corp_code(db_session: AsyncSession, corp_code: str) -> List[Dict[str, Any]]:
    """회사 코드로 재무제표 데이터를 조회합니다."""
    query = text("""
        SELECT 
            corp_code, corp_name, stock_code, rcept_no, reprt_code,
            bsns_year, sj_div, sj_nm, account_nm, thstrm_nm,
            thstrm_amount, frmtrm_nm, frmtrm_amount, bfefrmtrm_nm,
            bfefrmtrm_amount, ord, currency
        FROM fin_data
        WHERE corp_code = :corp_code
        ORDER BY bsns_year DESC, sj_div, ord
    """)
    result = await db_session.execute(query, {"corp_code": corp_code})
    return [dict(row) for row in result]

async def save_financial_ratios(db_session: AsyncSession, ratios: Dict[str, Any]) -> None:
    """재무비율을 저장합니다."""
    query = text("""
        INSERT INTO fin_data (
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
    await db_session.commit()