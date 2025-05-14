from typing import Dict, Any, List
import logging
from .ratio_data_processor import RatioDataProcessor

logger = logging.getLogger(__name__)

class FinancialDataProcessor:
    """재무제표 데이터 전처리 클래스 (레거시 - 하위 호환성 유지)"""
    
    def __init__(self):
        self.processor = RatioDataProcessor()
    
    def preprocess_financial_data(self, financial_data: List[Dict[str, Any]]) -> Dict[str, Dict[str, Dict[str, float]]]:
        """재무제표 데이터를 전처리합니다."""
        return self.processor.preprocess_financial_data(financial_data)

    def get_target_years(self, years_data: Dict[str, Dict[str, Dict[str, float]]]) -> List[str]:
        """대상 연도를 결정합니다."""
        return self.processor.get_target_years(years_data)

    def extract_financial_values(self, year_data: Dict[str, Dict[str, float]], values_type: str = "all") -> Dict[str, float]:
        """재무제표 데이터에서 필요한 값을 추출합니다.
        
        Args:
            year_data: 연도별 재무제표 데이터
            values_type: 추출할 값의 타입 ("all", "growth", "ratio")
        """
        return self.processor.extract_financial_values(year_data, values_type) 