from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)

class FinancialDataFormatter:
    """
    재무제표 데이터 포맷팅 클래스
    
    재무제표 원시 데이터를 클라이언트에게 보여주기 좋은 형태로 변환합니다.
    
    책임:
    - 재무제표 데이터를 연도별로 그룹화
    - 재무제표 유형별로 분류 (재무상태표, 손익계산서, 현금흐름표)
    - 클라이언트 응답에 적합한 형태로 포맷팅
    """
    
    # 재무제표 유형별 매핑
    STATEMENT_TYPE_MAP = {
        "BS": "재무상태표",
        "IS": "손익계산서",
        "CF": "현금흐름표"
    }
    
    # 금액 유형별 매핑
    AMOUNT_TYPE_MAP = {
        "thstrm_amount": "당기",
        "frmtrm_amount": "전기",
        "bfefrmtrm_amount": "전전기"
    }
    
    async def format_financial_data(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        재무제표 데이터를 연도별로 포맷팅합니다.
        
        Args:
            data: 원시 재무제표 데이터 리스트
            
        Returns:
            Dict: 포맷팅된 재무제표 데이터
            {
                "status": "success" | "error",
                "message": str,
                "data": List[Dict] - 연도별 포맷팅된 재무제표 데이터
            }
        """
        if not data:
            logger.warning("포맷팅할 재무제표 데이터가 없습니다.")
            return {
                "status": "success",
                "message": "포맷팅할 재무제표 데이터가 없습니다.",
                "data": []
            }
            
        try:
            # 연도별로 데이터 정리
            years_data = await self._group_data_by_year(data)
            
            # 정렬된 연도 리스트 생성 (최신 연도부터)
            sorted_years = sorted(years_data.keys(), reverse=True)
            formatted_data = [years_data[year] for year in sorted_years]
            
            return {
                "status": "success",
                "message": "재무제표 데이터 포맷팅이 완료되었습니다.",
                "data": formatted_data
            }
        except Exception as e:
            logger.error(f"재무제표 데이터 포맷팅 중 오류 발생: {str(e)}")
            return {
                "status": "error",
                "message": f"재무제표 데이터 포맷팅 중 오류 발생: {str(e)}",
                "data": []
            }
    
    async def _group_data_by_year(self, data: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """
        재무제표 데이터를 연도별로 그룹화합니다.
        
        Args:
            data: 원시 재무제표 데이터 리스트
            
        Returns:
            Dict: 연도별로 그룹화된 재무제표 데이터
        """
        years_data = {}
        
        for item in data:
            year_str = item["bsns_year"]
            
            # 해당 연도 데이터가 없으면 초기화
            if year_str not in years_data:
                years_data[year_str] = await self._initialize_year_data(year_str)
            
            # 재무제표 유형별 데이터 저장
            await self._add_statement_item(years_data[year_str], item)
            
        return years_data
    
    async def _initialize_year_data(self, year: str) -> Dict[str, Any]:
        """
        연도별 데이터 구조를 초기화합니다.
        
        Args:
            year: 사업연도
            
        Returns:
            Dict: 초기화된 연도별 데이터 구조
        """
        return {
            "사업연도": year,
            "재무상태표": {},
            "손익계산서": {},
            "현금흐름표": {}
        }
    
    async def _add_statement_item(self, year_data: Dict[str, Any], item: Dict[str, Any]) -> None:
        """
        재무제표 항목을 연도별 데이터에 추가합니다.
        
        Args:
            year_data: 연도별 데이터
            item: 재무제표 항목
        """
        statement_type = item["sj_div"]
        account_name = item["account_nm"]
        
        # 항목별 금액 데이터
        amount_data = {
            "당기": item["thstrm_amount"],
            "전기": item["frmtrm_amount"],
            "전전기": item["bfefrmtrm_amount"]
        }
        
        # 재무제표 유형에 따라 저장
        statement_category = self.STATEMENT_TYPE_MAP.get(statement_type)
        if statement_category:
            year_data[statement_category][account_name] = amount_data 