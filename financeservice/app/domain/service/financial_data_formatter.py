from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)

class FinancialDataFormatter:
    """재무제표 데이터 포맷팅 클래스"""
    
    async def format_financial_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """재무제표 데이터를 연도별로 포맷팅합니다."""
        if not data:
            return []
            
        try:
            # 연도별로 데이터 정리
            years_data = {}
            for item in data:
                year_str = item["bsns_year"]
                if year_str not in years_data:
                    years_data[year_str] = {
                        "사업연도": year_str,
                        "재무상태표": {},
                        "손익계산서": {},
                        "현금흐름표": {}
                    }
                
                # 재무제표 유형별 데이터 저장
                await self._add_statement_data(years_data, item)
            
            # 정렬된 연도 리스트 생성
            sorted_years = sorted(years_data.keys(), reverse=True)
            return [years_data[year_val] for year_val in sorted_years]
        except Exception as e:
            logger.error(f"재무제표 데이터 포맷팅 중 오류 발생: {str(e)}")
            return []
    
    async def _add_statement_data(self, years_data: Dict[str, Dict], item: Dict[str, Any]) -> None:
        """재무제표 항목을 연도별 데이터에 추가합니다."""
        year_str = item["bsns_year"]
        statement_type = item["sj_div"]
        
        # 항목별 금액 데이터
        amount_data = {
            "당기": item["thstrm_amount"],
            "전기": item["frmtrm_amount"],
            "전전기": item["bfefrmtrm_amount"]
        }
        
        # 재무제표 유형에 따라 저장
        if statement_type == "BS":
            years_data[year_str]["재무상태표"][item["account_nm"]] = amount_data
        elif statement_type == "IS":
            years_data[year_str]["손익계산서"][item["account_nm"]] = amount_data
        elif statement_type == "CF":
            years_data[year_str]["현금흐름표"][item["account_nm"]] = amount_data 