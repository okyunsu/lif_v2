from typing import Dict, Any, Optional, List
from sqlalchemy.ext.asyncio import AsyncSession
import logging

from app.domain.model.schema.schema import FinancialMetricsResponse
from app.domain.repository.ratio_repository import get_financial_data, save_financial_ratios, get_saved_financial_ratios
from .ratio_data_processor import RatioDataProcessor
from .ratio_calculator import RatioCalculator
from .growth_rate_calculator import GrowthRateCalculator
from .response_builder import ResponseBuilder

logger = logging.getLogger(__name__)

class RatioService:
    """재무비율 계산 서비스"""
    
    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session
        self.data_processor = RatioDataProcessor()
        self.ratio_calculator = RatioCalculator(self.data_processor)
        self.growth_calculator = GrowthRateCalculator(self.data_processor)
        self.response_builder = ResponseBuilder()

    async def calculate_financial_ratios(self, company_name: str, year: Optional[int] = None) -> FinancialMetricsResponse:
        """financials 테이블에서 데이터 조회 후 재무비율 계산"""
        try:
            # 1. Repository 함수를 통해 재무 데이터 조회
            financial_data = await get_financial_data(self.db_session, company_name, year)
            
            if not financial_data:
                logger.error(f"재무제표 데이터가 없습니다: {company_name}")
                raise ValueError(f"재무제표 데이터가 없습니다: {company_name}")
            
            # 회사 코드 확인
            corp_code = self._extract_corp_code(financial_data)
            
            # 2. 데이터 전처리 (연도별, 계정명별로 정리)
            years_data = self.data_processor.preprocess_financial_data(financial_data)

            # 3. 대상 연도 결정
            target_years = self.data_processor.get_target_years(years_data)
            
            if not target_years:
                logger.error(f"분석 가능한 연도가 없습니다: {company_name}")
                raise ValueError(f"분석 가능한 연도가 없습니다: {company_name}")
            
            # 4. 저장된 재무비율 확인 - 이미 계산된 데이터가 있으면 바로 반환
            saved_ratios = await get_saved_financial_ratios(self.db_session, company_name, target_years)
            if saved_ratios and len(saved_ratios) == len(target_years):
                logger.info(f"{company_name}의 저장된 재무비율 데이터를 반환합니다 (연도: {', '.join(target_years)})")
                return self._build_response_from_saved_ratios(company_name, target_years, saved_ratios)

            # 5. 재무비율 계산 - RatioCalculator에 위임
            ratios = self.ratio_calculator.calculate_all_ratios(years_data, target_years)

            # 6. 성장률 계산 - GrowthRateCalculator에 위임
            growth_rates = self.growth_calculator.calculate_growth_rates(years_data, target_years)

            # 7. 재무비율 저장
            await self._save_calculated_ratios(corp_code, company_name, target_years, ratios, growth_rates)

            # 8. 응답 생성
            return self.response_builder.build_metrics_response(
                company_name=company_name,
                target_years=target_years,
                ratios=ratios,
                growth_rates=growth_rates
            )
        except ValueError as e:
            logger.error(f"재무비율 계산 값 오류: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"재무비율 계산 중 오류 발생: {str(e)}")
            raise
    
    def _extract_corp_code(self, financial_data: List[Dict[str, Any]]) -> str:
        """재무 데이터에서 회사 코드를 추출합니다."""
        for item in financial_data:
            if "corp_code" in item and item["corp_code"]:
                return item["corp_code"]
        
        logger.error("회사 코드를 찾을 수 없습니다")
        raise ValueError("회사 코드를 찾을 수 없습니다")
    
    def _build_response_from_saved_ratios(self, company_name: str, target_years: List[str], 
                                         saved_ratios: List[Dict[str, Any]]) -> FinancialMetricsResponse:
        """저장된 재무비율 데이터로부터 응답을 생성합니다."""
        # 저장된 데이터를 응답 형식에 맞게 변환
        ratios = {
            "debt_ratios": [],
            "current_ratios": [],
            "operating_margins": [],
            "net_margins": [],
            "roe_values": [],
            "roa_values": []
        }
        
        growth_rates = {
            "revenue_growth": [],
            "net_income_growth": []
        }
        
        # 연도 순서대로 데이터 정렬
        year_to_ratio = {ratio["bsns_year"]: ratio for ratio in saved_ratios}
        
        for year in target_years:
            ratio = year_to_ratio.get(year, {})
            ratios["debt_ratios"].append(ratio.get("debt_ratio"))
            ratios["current_ratios"].append(ratio.get("current_ratio"))
            ratios["operating_margins"].append(ratio.get("operating_profit_ratio"))
            ratios["net_margins"].append(ratio.get("net_profit_ratio"))
            ratios["roe_values"].append(ratio.get("roe"))
            ratios["roa_values"].append(ratio.get("roa"))
            
            growth_rates["revenue_growth"].append(ratio.get("sales_growth"))
            growth_rates["net_income_growth"].append(ratio.get("eps_growth"))
        
        return self.response_builder.build_metrics_response(
            company_name=company_name,
            target_years=target_years,
            ratios=ratios,
            growth_rates=growth_rates
        )
            
    async def _save_calculated_ratios(self, corp_code: str, company_name: str, 
                                     target_years: List[str], 
                                     ratios: Dict[str, List[Optional[float]]], 
                                     growth_rates: Dict[str, List[Optional[float]]]) -> None:
        """계산된 재무비율을 DB에 저장합니다."""
        try:
            for i, year in enumerate(target_years):
                ratio_data = {
                    "corp_code": corp_code,
                    "corp_name": company_name,
                    "bsns_year": year,
                    "debt_ratio": ratios["debt_ratios"][i],
                    "current_ratio": ratios["current_ratios"][i],
                    "interest_coverage_ratio": None,  # 계산하지 않음
                    "operating_profit_ratio": ratios["operating_margins"][i],
                    "net_profit_ratio": ratios["net_margins"][i],
                    "roe": ratios["roe_values"][i],
                    "roa": ratios["roa_values"][i],
                    "debt_dependency": None,  # 계산하지 않음
                    "cash_flow_debt_ratio": None,  # 계산하지 않음
                    "sales_growth": growth_rates["revenue_growth"][i],
                    "operating_profit_growth": None,  # 계산하지 않음
                    "eps_growth": growth_rates["net_income_growth"][i]
                }
                await save_financial_ratios(self.db_session, ratio_data)
            logger.info(f"{company_name}의 재무비율 저장 완료 (연도: {', '.join(target_years)})")
        except Exception as e:
            logger.error(f"재무비율 저장 중 오류 발생: {str(e)}")
            # 저장 실패해도 계산 결과는 반환 