from typing import Dict, Optional, List
import logging

logger = logging.getLogger(__name__)

class RatioCalculator:
    """재무비율 계산 클래스"""
    
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
        
        for year in target_years:
            year_data = years_data.get(year, {})
            
            # 필요한 값 직접 추출
            total_assets = year_data.get("자산총계", {}).get("thstrm", 0)
            total_liabilities = year_data.get("부채총계", {}).get("thstrm", 0)
            current_assets = year_data.get("유동자산", {}).get("thstrm", 0)
            current_liabilities = year_data.get("유동부채", {}).get("thstrm", 0)
            total_equity = year_data.get("자본총계", {}).get("thstrm", 0)
            revenue = year_data.get("매출액", {}).get("thstrm", 0)
            operating_profit = year_data.get("영업이익", {}).get("thstrm", 0)
            net_income = year_data.get("당기순이익", {}).get("thstrm", 0)
            
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