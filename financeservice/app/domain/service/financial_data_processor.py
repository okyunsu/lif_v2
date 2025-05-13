from typing import List, Dict, Any, Optional
import logging
import asyncio
from app.domain.model.schema.company_schema import CompanySchema
from app.domain.model.schema.financial_schema import FinancialSchema

logger = logging.getLogger(__name__)

class FinancialDataProcessor:
    """
    재무제표 데이터 처리 클래스
    
    DART API에서 가져온 원시 재무제표 데이터를 가공하여
    DB 저장 및 클라이언트 응답에 적합한 형태로 변환합니다.
    """
    
    async def process_raw_statements(self, statements: List[Dict[str, Any]], 
                                    company_info: CompanySchema) -> List[Dict[str, Any]]:
        """
        원시 재무제표 데이터를 처리하여 DB 저장 형식으로 변환합니다.
        
        Args:
            statements: DART API에서 가져온 원시 재무제표 데이터
            company_info: 회사 정보
            
        Returns:
            List[Dict]: DB 저장 형식으로 변환된 재무제표 데이터
        """
        # 1. 중복 제거
        unique_statements = await self.deduplicate_statements(statements)
        
        # 2. DB 저장 형식으로 변환
        processed_statements = []
        for stmt in unique_statements:
            try:
                processed_stmt = await self.prepare_statement_data(stmt, company_info)
                processed_statements.append(processed_stmt)
            except Exception as e:
                logger.error(f"재무제표 항목 처리 실패: {str(e)}")
                # 개별 항목 오류는 건너뛰고 계속 진행
                continue
                
        return processed_statements
    
    async def convert_amount(self, amount_str: Optional[str]) -> float:
        """
        금액 문자열을 숫자로 변환합니다.
        
        Args:
            amount_str: 금액 문자열 (예: "1,234,567")
            
        Returns:
            float: 변환된 숫자 (변환 실패 시 0.0)
        """
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
        """
        중복되는 계정과목을 제거하고 가장 최신의 금액만 남깁니다.
        
        Args:
            statements: 원시 재무제표 데이터 리스트
            
        Returns:
            List[Dict]: 중복이 제거된 재무제표 데이터 리스트
        """
        try:
            # 계정과목과 재무제표 유형이 동일한 항목 중 가장 우선순위가 높은(ord 값이 작은) 항목만 유지
            unique_items = {}
            for stmt in statements:
                key = (stmt.get("account_nm", ""), stmt.get("sj_nm", ""))
                
                # 새 항목이거나, 기존 항목보다 우선순위가 높은 경우 업데이트
                if key not in unique_items or int(stmt.get("ord", 0)) < int(unique_items[key].get("ord", 0)):
                    unique_items[key] = stmt
                    
            return list(unique_items.values())
        except Exception as e:
            logger.error(f"재무제표 중복 제거 중 오류 발생: {str(e)}")
            # 오류 발생 시 원본 반환
            return statements

    async def prepare_statement_data(self, statement: Dict[str, Any], company_info: CompanySchema) -> Dict[str, Any]:
        """
        재무제표 데이터를 DB 저장 형식으로 변환합니다.
        
        Args:
            statement: 원시 재무제표 항목
            company_info: 회사 정보
            
        Returns:
            Dict: DB 저장 형식으로 변환된 재무제표 항목
        """
        try:
            # 금액 변환을 비동기로 처리
            thstrm_amount = await self.convert_amount(statement.get("thstrm_amount", ""))
            frmtrm_amount = await self.convert_amount(statement.get("frmtrm_amount", ""))
            bfefrmtrm_amount = await self.convert_amount(statement.get("bfefrmtrm_amount", ""))

            # DB 저장 형식으로 변환
            return {
                # 회사 정보
                "corp_code": company_info.corp_code,
                "corp_name": company_info.corp_name,
                "stock_code": company_info.stock_code,
                
                # 보고서 정보
                "rcept_no": statement.get("rcept_no", ""),
                "reprt_code": statement.get("reprt_code", ""),
                "bsns_year": statement.get("bsns_year", ""),
                
                # 재무제표 정보
                "sj_div": statement.get("sj_div", ""),
                "sj_nm": statement.get("sj_nm", ""),
                "account_nm": statement.get("account_nm", ""),
                
                # 금액 정보
                "thstrm_nm": statement.get("thstrm_nm", ""),
                "thstrm_amount": thstrm_amount,
                "frmtrm_nm": statement.get("frmtrm_nm", ""),
                "frmtrm_amount": frmtrm_amount,
                "bfefrmtrm_nm": statement.get("bfefrmtrm_nm", ""),
                "bfefrmtrm_amount": bfefrmtrm_amount,
                
                # 기타 정보
                "ord": int(statement.get("ord", 0)),
                "currency": statement.get("currency", "")
            }
        except Exception as e:
            logger.error(f"재무제표 데이터 변환 중 오류 발생: {str(e)}")
            raise 