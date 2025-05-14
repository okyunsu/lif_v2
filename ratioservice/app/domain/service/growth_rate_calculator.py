from typing import Dict, Optional, List
import logging
from .ratio_data_processor import RatioDataProcessor

logger = logging.getLogger(__name__)

class GrowthRateCalculator:
    """성장률 계산 클래스"""
    
    def __init__(self, data_processor: Optional[RatioDataProcessor] = None):
        """
        성장률 계산기 초기화
        
        Args:
            data_processor: 데이터 처리기 (없으면 새로 생성)
        """
        self.data_processor = data_processor or RatioDataProcessor()
    
    def calculate_growth_rates(
        self, 
        years_data: Dict[str, Dict[str, Dict[str, float]]], 
        target_years: List[str]
    ) -> Dict[str, List[float]]:
        """
        매출액과 당기순이익의 성장률을 계산합니다.
        
        Args:
            years_data: 연도별 재무제표 데이터
            target_years: 대상 연도 목록
            
        Returns:
            Dict[str, List[float]]: 성장률 데이터
            {
                "revenue_growth": [0.0, 10.5, 5.2],
                "net_income_growth": [0.0, 12.1, 3.8]
            }
        """
        # 각 연도별 재무 값 추출 (매출액, 당기순이익만)
        financial_values = self.data_processor.extract_values_by_years(years_data, target_years, "growth")
        
        # 성장률 저장 딕셔너리
        growth_rates = {
            "revenue_growth": [],
            "net_income_growth": []
        }
        
        # 각 지표별 성장률 계산
        growth_rates["revenue_growth"] = self._calculate_growth_rates_for_metric(financial_values["revenue"])
        growth_rates["net_income_growth"] = self._calculate_growth_rates_for_metric(financial_values["net_income"])
        
        return growth_rates
    
    def _calculate_growth_rates_for_metric(self, values: List[float]) -> List[float]:
        """
        특정 지표의 성장률을 계산합니다.
        
        Args:
            values: 연도별 지표 값 (최신 연도 순)
            
        Returns:
            List[float]: 성장률 리스트 (첫해는 0.0)
        """
        if not values or len(values) < 2:
            return [0.0] * len(values)
        
        growth_rates = [0.0]  # 첫 해는 성장률 계산 불가능하므로 0.0 추가
        
        # 두 번째 해부터 성장률 계산
        for i in range(1, len(values)):
            current_value = values[i]
            previous_value = values[i - 1]
            growth_rate = self._calculate_growth_rate(current_value, previous_value)
            growth_rates.append(growth_rate)
        
        return growth_rates
    
    def _calculate_growth_rate(self, current_value: float, previous_value: float) -> float:
        """단일 성장률을 계산합니다."""
        try:
            if previous_value == 0:
                return 0.0
            growth_rate = (current_value - previous_value) / abs(previous_value) * 100
            return round(growth_rate, 2)
        except (ZeroDivisionError, TypeError):
            return 0.0 