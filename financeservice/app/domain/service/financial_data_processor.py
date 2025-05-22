from typing import List, Dict, Any, Optional
import logging
import asyncio
from app.domain.model.schema.company_schema import CompanySchema
from app.domain.model.schema.financial_schema import FinancialSchema
from app.foundation.infra.utils.convert import convert_amount

logger = logging.getLogger(__name__)

class FinancialDataProcessor:
    """
    재무제표 데이터 처리 클래스
    
    DART API에서 가져온 원시 재무제표 데이터를 가공하여
    DB 저장 및 클라이언트 응답에 적합한 형태로 변환합니다.
    
    책임:
    - 데이터 중복 제거
    - 금액 변환 (문자열 -> 숫자)
    - DB 저장 형식으로 변환
    """
    
    def __init__(self):
        self.processed_data = set()  # 중복 데이터 체크를 위한 set
        
    def prepare_statement_data(self, company_info: CompanySchema, statement: Dict[str, Any]) -> Dict[str, Any]:
        """
        재무제표 데이터를 DB 저장 형식으로 변환합니다.
        
        Args:
            company_info: 회사 정보
            statement: DART API에서 가져온 재무제표 데이터
            
        Returns:
            Dict[str, Any]: DB 저장 형식으로 변환된 데이터
        """
        # 중복 데이터 체크
        key = f"{company_info.corp_code}_{statement.get('bsns_year')}_{statement.get('sj_div')}_{statement.get('account_nm')}"
        if key in self.processed_data:
            logger.info(f"중복 데이터 건너뛰기: {key}")
            return None
        self.processed_data.add(key)
        
        # DB 저장 형식으로 변환
        return {
            # 회사 정보
            "corp_code": company_info.corp_code,
            "corp_name": company_info.corp_name,  # 필수 필드
            "stock_code": company_info.stock_code or "",
            
            # 보고서 정보
            "rcept_no": statement.get("rcept_no", ""),
            "reprt_code": statement.get("reprt_code", ""),
            "bsns_year": statement.get("bsns_year", ""),
            
            # 재무제표 정보
            "sj_div": statement.get("sj_div", ""),
            "sj_nm": statement.get("sj_nm", ""),
            "account_nm": statement.get("account_nm", ""),
            "thstrm_nm": statement.get("thstrm_nm", ""),
            "thstrm_amount": convert_amount(statement.get("thstrm_amount")),
            "frmtrm_nm": statement.get("frmtrm_nm", ""),
            "frmtrm_amount": convert_amount(statement.get("frmtrm_amount")),
            "bfefrmtrm_nm": statement.get("bfefrmtrm_nm", ""),
            "bfefrmtrm_amount": convert_amount(statement.get("bfefrmtrm_amount")),
            "ord": statement.get("ord", 0),
            "currency": statement.get("currency", "KRW")
        }
        
    def process_financial_statements(self, company_info: CompanySchema, statements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        재무제표 데이터를 일괄 처리합니다.
        
        Args:
            company_info: 회사 정보
            statements: DART API에서 가져온 재무제표 데이터 목록
            
        Returns:
            List[Dict[str, Any]]: 처리된 재무제표 데이터 목록
        """
        processed_statements = []
        for statement in statements:
            processed_data = self.prepare_statement_data(company_info, statement)
            if processed_data:
                processed_statements.append(processed_data)
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
            # 쉼표 제거 및 숫자 변환
            cleaned_str = amount_str.replace(",", "").strip()
            if not cleaned_str:  # 빈 문자열인 경우
                return 0.0
                
            # 무거운 변환 작업을 별도 스레드에서 실행
            return await asyncio.to_thread(
                lambda: float(cleaned_str)
            )
        except (ValueError, AttributeError) as e:
            logger.warning(f"금액 변환 실패: {amount_str}, 에러: {str(e)}")
            return 0.0

    async def deduplicate_statements(self, statements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        중복되는 계정과목을 제거하고 가장 최신의 금액만 남깁니다.
        
        중복 제거 기준:
        - 계정과목명(account_nm)과 재무제표 유형(sj_nm)이 동일한 항목 중
        - 우선순위(ord)가 가장 높은(값이 낮은) 항목만 유지
        
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
                processed_stmt = await self.prepare_statement_data(company_info, stmt)
                processed_statements.append(processed_stmt)
            except Exception as e:
                logger.error(f"재무제표 항목 처리 실패: {str(e)}")
                # 개별 항목 오류는 건너뛰고 계속 진행
                continue
                
        return processed_statements 