from typing import Dict, List, Optional, Any
from app.domain.model.schema.schema import FinancialMetricsResponse, FinancialMetrics, GrowthData, DebtLiquidityData
import logging
import math

logger = logging.getLogger(__name__)

def to_float_list(lst, n, default_value=None):
    """
    안전하게 float 리스트로 변환합니다.
    
    None, NaN, 잘못된 값은 default_value로 변환하고, 
    길이가 맞지 않으면 default_value 리스트를 반환합니다.
    
    Args:
        lst: 변환할 리스트
        n: 기대하는 리스트 길이
        default_value: 변환 실패 시 기본값
        
    Returns:
        List[float]: 변환된 float 리스트
    """
    if not isinstance(lst, list) or len(lst) != n:
        logger.warning(f"리스트 형식이 잘못되었습니다. 기본값을 사용합니다. (expected: {n}, got: {len(lst) if isinstance(lst, list) else type(lst)})")
        return [default_value] * n
    
    result = []
    for x in lst:
        try:
            if x is None:
                result.append(default_value)
            elif isinstance(x, float) and math.isnan(x):
                result.append(default_value)
            else:
                result.append(float(x))
        except Exception as e:
            logger.debug(f"값 변환 중 오류: {str(e)}")
            result.append(default_value)
    return result

class ResponseBuilder:
    """응답 생성 클래스"""
    
    def build_metrics_response(
        self,
        company_name: str,
        target_years: List[str],
        ratios: Dict[str, List[Optional[float]]],
        growth_rates: Dict[str, List[Optional[float]]]
    ) -> FinancialMetricsResponse:
        """
        재무비율 응답을 생성합니다.
        
        Args:
            company_name: 회사명
            target_years: 대상 연도 목록
            ratios: 재무비율 데이터
            growth_rates: 성장률 데이터
            
        Returns:
            FinancialMetricsResponse: 응답 객체
        """
        # 데이터가 없거나 연도가 없는 경우, 오류 로그 출력
        if not target_years:
            logger.error("연도 데이터가 없습니다.")
            target_years = ["N/A"]
            
        n = len(target_years)
        
        # 결측치 처리 및 타입 변환
        revenue_growth = to_float_list(growth_rates.get("revenue_growth", []), n, default_value=0.0)
        net_income_growth = to_float_list(growth_rates.get("net_income_growth", []), n, default_value=0.0)
        debt_ratios = to_float_list(ratios.get("debt_ratios", []), n, default_value=None)
        current_ratios = to_float_list(ratios.get("current_ratios", []), n, default_value=None)
        operating_margins = to_float_list(ratios.get("operating_margins", []), n, default_value=None)
        net_margins = to_float_list(ratios.get("net_margins", []), n, default_value=None)
        roe_values = to_float_list(ratios.get("roe_values", []), n, default_value=None)
        roa_values = to_float_list(ratios.get("roa_values", []), n, default_value=None)

        # 응답 객체 생성
        metrics = FinancialMetrics(
            operatingMargin=operating_margins,
            netMargin=net_margins,
            roe=roe_values,
            roa=roa_values,
            years=target_years
        )
        growth = GrowthData(
            revenueGrowth=revenue_growth,
            netIncomeGrowth=net_income_growth,
            years=target_years
        )
        debt_liquidity = DebtLiquidityData(
            debtRatio=debt_ratios,
            currentRatio=current_ratios,
            years=target_years
        )
        return FinancialMetricsResponse(
            companyName=company_name,
            financialMetrics=metrics,
            growthData=growth,
            debtLiquidityData=debt_liquidity
        ) 