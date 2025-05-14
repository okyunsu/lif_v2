from typing import Dict, Optional, List
import logging
from .ratio_data_processor import RatioDataProcessor

logger = logging.getLogger(__name__)

class RatioCalculator:
    """재무비율 계산 클래스"""
    
    def __init__(self, data_processor: Optional[RatioDataProcessor] = None):
        """
        재무비율 계산기 초기화
        
        Args:
            data_processor: 데이터 처리기 (없으면 새로 생성)
        """
        self.data_processor = data_processor or RatioDataProcessor()
    
    def calculate_all_ratios(self, years_data: Dict[str, Dict[str, Dict[str, float]]], target_years: List[str]) -> Dict[str, List[Optional[float]]]:
        """모든 재무비율을 계산합니다."""
        ratios = {
            "operating_margins": [],
            "net_margins": [],
            "roe_values": [],
            "roa_values": [],
            "debt_ratios": [],
            "current_ratios": []
        }
        
        # 재무 데이터 추출 - 모든 연도에 대해 한 번에 처리
        financial_values = self.data_processor.extract_values_by_years(years_data, target_years, "ratios")
        
        # 각 연도별로 비율 계산
        for i in range(len(target_years)):
            # 필요한 값 가져오기
            total_assets = financial_values["total_assets"][i]
            total_liabilities = financial_values["total_liabilities"][i]
            current_assets = financial_values["current_assets"][i]
            current_liabilities = financial_values["current_liabilities"][i]
            total_equity = financial_values["total_equity"][i]
            revenue = financial_values["revenue"][i]
            operating_profit = financial_values["operating_profit"][i]
            net_income = financial_values["net_income"][i]
            
            # 비율 계산
            ratios["operating_margins"].append(self._safe_divide(operating_profit, revenue) * 100)
            ratios["net_margins"].append(self._safe_divide(net_income, revenue) * 100)
            ratios["roe_values"].append(self._safe_divide(net_income, total_equity) * 100)
            ratios["roa_values"].append(self._safe_divide(net_income, total_assets) * 100)
            ratios["debt_ratios"].append(self._safe_divide(total_liabilities, total_equity) * 100)
            ratios["current_ratios"].append(self._safe_divide(current_assets, current_liabilities) * 100)
        
        return ratios

    def _safe_divide(self, numerator: float, denominator: float) -> Optional[float]:
        """안전한 나눗셈을 수행합니다."""
        try:
            if denominator == 0:
                return None
            return numerator / denominator
        except:
            return None 