from typing import List, Dict, Any, Optional
import logging
import asyncio
from app.domain.model.schema.company_schema import CompanySchema
from app.domain.model.schema.financial_schema import FinancialSchema

logger = logging.getLogger(__name__)

class FinancialDataProcessor:
    """재무제표 데이터 처리 클래스"""
    
    async def convert_amount(self, amount_str: Optional[str]) -> float:
        """금액 문자열을 숫자로 변환합니다."""
        if not amount_str:
            return 0.0
        try:
            # 무거운 변환 작업을 별도 스레드에서 실행
            return await asyncio.to_thread(
                lambda: float(amount_str.replace(",", ""))
            )
        except (ValueError, AttributeError) as e:
            logger.warning(f"금액 변환 실패: {amount_str}, 에러: {str(e)}")
            return 0.0

    async def deduplicate_statements(self, statements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """중복되는 계정과목을 제거하고 가장 최신의 금액만 남깁니다."""
        try:
            latest_statements = {}
            for stmt in statements:
                key = (stmt.get("account_nm", ""), stmt.get("sj_nm", ""))
                if key not in latest_statements or int(stmt.get("ord", 0)) < int(latest_statements[key].get("ord", 0)):
                    latest_statements[key] = stmt
            return list(latest_statements.values())
        except Exception as e:
            logger.error(f"재무제표 중복 제거 중 오류 발생: {str(e)}")
            return statements

    async def prepare_statement_data(self, statement: Dict[str, Any], company_info: CompanySchema) -> Dict[str, Any]:
        """재무제표 데이터를 DB 저장 형식으로 변환합니다."""
        try:
            # 금액 변환을 비동기로 처리
            thstrm_amount = await self.convert_amount(statement.get("thstrm_amount", ""))
            frmtrm_amount = await self.convert_amount(statement.get("frmtrm_amount", ""))
            bfefrmtrm_amount = await self.convert_amount(statement.get("bfefrmtrm_amount", ""))

            return {
                "corp_code": company_info.corp_code,
                "corp_name": company_info.corp_name,
                "stock_code": company_info.stock_code,
                "rcept_no": statement.get("rcept_no", ""),
                "reprt_code": statement.get("reprt_code", ""),
                "bsns_year": statement.get("bsns_year", ""),
                "sj_div": statement.get("sj_div", ""),
                "sj_nm": statement.get("sj_nm", ""),
                "account_nm": statement.get("account_nm", ""),
                "thstrm_nm": statement.get("thstrm_nm", ""),
                "thstrm_amount": thstrm_amount,
                "frmtrm_nm": statement.get("frmtrm_nm", ""),
                "frmtrm_amount": frmtrm_amount,
                "bfefrmtrm_nm": statement.get("bfefrmtrm_nm", ""),
                "bfefrmtrm_amount": bfefrmtrm_amount,
                "ord": int(statement.get("ord", 0)),
                "currency": statement.get("currency", "")
            }
        except Exception as e:
            logger.error(f"재무제표 데이터 변환 중 오류 발생: {str(e)}")
            raise 