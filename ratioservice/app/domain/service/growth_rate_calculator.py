from typing import Dict, Optional, List
import logging

logger = logging.getLogger(__name__)

class GrowthRateCalculator:
    """성장률 계산 클래스"""
    
    def calculate_growth_rates(self, years_data: Dict[str, Dict[str, Dict[str, float]]], target_years: List[str], extracted_values: Dict[str, Dict[str, float]] = None) -> Dict[str, List[float]]:
        """매출액과 당기순이익의 성장률을 계산합니다.
        
        Args:
            years_data: 연도별 재무제표 데이터
            target_years: 대상 연도 목록
            extracted_values: 미리 추출된 재무 값 (없으면 내부에서 추출)
        """
        growth_rates = {
            "revenue_growth": [],
            "net_income_growth": []
        }
        
        # 첫 해는 성장률 계산 불가능하므로 0.0 추가 (None 대신)
        growth_rates["revenue_growth"].append(0.0)
        growth_rates["net_income_growth"].append(0.0)
        
        # 두 번째 해부터 성장률 계산
        for i in range(1, len(target_years)):
            current_year = target_years[i]
            previous_year = target_years[i-1]
            
            # 현재 연도 데이터
            current_revenue = years_data.get(current_year, {}).get("매출액", {}).get("thstrm", 0)
            current_net_income = years_data.get(current_year, {}).get("당기순이익", {}).get("thstrm", 0)
            
            # 이전 연도 데이터
            previous_revenue = years_data.get(previous_year, {}).get("매출액", {}).get("thstrm", 0)
            previous_net_income = years_data.get(previous_year, {}).get("당기순이익", {}).get("thstrm", 0)
            
            # 성장률 계산
            revenue_growth = self._calculate_growth_rate(current_revenue, previous_revenue)
            net_income_growth = self._calculate_growth_rate(current_net_income, previous_net_income)
            
            growth_rates["revenue_growth"].append(revenue_growth)
            growth_rates["net_income_growth"].append(net_income_growth)
        
        return growth_rates

    def _calculate_growth_rate(self, current: float, previous: float) -> float:
        """성장률을 계산합니다. 계산할 수 없는 경우 0.0을 반환합니다."""
        try:
            if previous == 0:
                return 0.0
            return ((current - previous) / abs(previous)) * 100
        except Exception as e:
            logger.warning(f"성장률 계산 중 오류 발생: {str(e)}")
            return 0.0 