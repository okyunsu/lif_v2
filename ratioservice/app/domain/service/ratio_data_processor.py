from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)

class RatioDataProcessor:
    """
    재무 데이터 처리 클래스
    
    재무제표 데이터를 전처리하고 필요한 값을 추출하는 기능을 제공합니다.
    모든 계산기에서 공통으로 사용되는 데이터 처리 로직을 집중화합니다.
    """
    
    # 계정과목 매핑 (다양한 표현 대응)
    ACCOUNT_MAPPING = {
        # 자산
        "자산총계": ["자산총계", "총자산"],
        "유동자산": ["유동자산"],
        # 부채
        "부채총계": ["부채총계", "총부채"],
        "유동부채": ["유동부채"],
        # 자본
        "자본총계": ["자본총계", "총자본", "자본", "지배기업소유주지분"],
        # 손익계산서
        "매출액": ["매출액", "영업수익", "매출", "수익(매출액)"],
        "영업이익": ["영업이익", "영업이익(손실)"],
        "당기순이익": ["당기순이익", "당기순이익(손실)", "당기순손익", "지배기업소유주지분순이익"]
    }
    
    # 재무 항목 그룹
    METRIC_GROUPS = {
        "all": ["total_assets", "total_liabilities", "current_assets", "current_liabilities", 
                "total_equity", "revenue", "operating_profit", "net_income"],
        "growth": ["revenue", "net_income"],
        "ratios": ["total_assets", "total_liabilities", "current_assets", "current_liabilities", 
                  "total_equity", "revenue", "operating_profit", "net_income"]
    }
    
    def preprocess_financial_data(self, financial_data: List[Dict[str, Any]]) -> Dict[str, Dict[str, Dict[str, float]]]:
        """
        재무제표 데이터를 전처리합니다.
        
        Args:
            financial_data: DB에서 조회한 원시 재무제표 데이터
            
        Returns:
            Dict[str, Dict[str, Dict[str, float]]]: 연도별, 계정과목별, 기간별 금액
            {
                "2023": {
                    "매출액": {"thstrm": 100, "frmtrm": 90, "bfefrmtrm": 80},
                    "당기순이익": {"thstrm": 10, "frmtrm": 9, "bfefrmtrm": 8},
                    ...
                },
                "2022": { ... }
            }
        """
        years_data = {}
        
        for item in financial_data:
            year = item["bsns_year"]
            if year not in years_data:
                years_data[year] = {}
            
            account_nm = item["account_nm"]
            years_data[year][account_nm] = {
                "thstrm": float(item["thstrm_amount"]) if item["thstrm_amount"] else 0,
                "frmtrm": float(item["frmtrm_amount"]) if item["frmtrm_amount"] else 0,
                "bfefrmtrm": float(item["bfefrmtrm_amount"]) if item["bfefrmtrm_amount"] else 0
            }
        
        return years_data

    def get_target_years(self, years_data: Dict[str, Dict[str, Dict[str, float]]]) -> List[str]:
        """
        대상 연도를 결정합니다.
        
        Args:
            years_data: 전처리된 재무제표 데이터
            
        Returns:
            List[str]: 대상 연도 리스트 (최근 3개년도, 최신 순)
        """
        all_years = sorted(years_data.keys(), reverse=True)
        return all_years[:3]  # 최근 3개년도만

    def extract_financial_values(
        self, 
        year_data: Dict[str, Dict[str, float]], 
        values_type: str = "all"
    ) -> Dict[str, float]:
        """
        재무제표 데이터에서 필요한 값을 추출합니다.
        
        Args:
            year_data: 연도별 재무제표 데이터
            values_type: 추출할 값의 타입 ("all", "growth", "ratios")
            
        Returns:
            Dict[str, float]: 추출된 재무 값
            {
                "total_assets": 1000,
                "revenue": 100,
                ...
            }
        """
        # 추출할 항목 결정
        metrics_to_extract = self.METRIC_GROUPS.get(values_type, self.METRIC_GROUPS["all"])
        
        # 결과 저장 딕셔너리
        values = {}
        
        # 각 항목 추출
        if "total_assets" in metrics_to_extract:
            values["total_assets"] = self._find_account_value(year_data, "자산총계")
        
        if "total_liabilities" in metrics_to_extract:
            values["total_liabilities"] = self._find_account_value(year_data, "부채총계")
        
        if "current_assets" in metrics_to_extract:
            values["current_assets"] = self._find_account_value(year_data, "유동자산")
        
        if "current_liabilities" in metrics_to_extract:
            values["current_liabilities"] = self._find_account_value(year_data, "유동부채")
        
        if "total_equity" in metrics_to_extract:
            values["total_equity"] = self._find_account_value(year_data, "자본총계")
        
        if "revenue" in metrics_to_extract:
            values["revenue"] = self._find_account_value(year_data, "매출액")
        
        if "operating_profit" in metrics_to_extract:
            values["operating_profit"] = self._find_account_value(year_data, "영업이익")
        
        if "net_income" in metrics_to_extract:
            values["net_income"] = self._find_account_value(year_data, "당기순이익")
        
        return values
    
    def _find_account_value(self, year_data: Dict[str, Dict[str, float]], account_key: str) -> float:
        """
        다양한 표현의 계정과목을 찾아 값을 반환합니다.
        
        Args:
            year_data: 연도별 재무제표 데이터
            account_key: 계정과목 키 (매핑 테이블의 키)
            
        Returns:
            float: 찾은 계정과목의 값 (없으면 0)
        """
        # 매핑 테이블에서 가능한 계정과목명 목록 가져오기
        possible_names = self.ACCOUNT_MAPPING.get(account_key, [account_key])
        
        # 각 가능한 이름에 대해 검색
        for name in possible_names:
            if name in year_data:
                return year_data[name].get("thstrm", 0)
        
        # 못 찾으면 0 반환
        logger.warning(f"계정과목 '{account_key}'을(를) 찾을 수 없습니다.")
        return 0
    
    def extract_values_by_years(
        self, 
        years_data: Dict[str, Dict[str, Dict[str, float]]], 
        target_years: List[str],
        values_type: str = "all"
    ) -> Dict[str, List[float]]:
        """
        여러 연도에 대해 재무 값을 추출합니다.
        
        Args:
            years_data: 전처리된 재무제표 데이터
            target_years: 대상 연도 리스트
            values_type: 추출할 값의 타입
            
        Returns:
            Dict[str, List[float]]: 연도별 재무 값
            {
                "total_assets": [1000, 900, 800],
                "revenue": [100, 90, 80],
                ...
            }
        """
        # 결과 저장 딕셔너리
        all_values = {}
        
        # 추출할 항목 결정
        metrics_to_extract = self.METRIC_GROUPS.get(values_type, self.METRIC_GROUPS["all"])
        
        # 각 항목에 대한 리스트 초기화
        for metric in metrics_to_extract:
            all_values[metric] = []
        
        # 각 연도에 대해 값 추출
        for year in target_years:
            year_data = years_data.get(year, {})
            values = self.extract_financial_values(year_data, values_type)
            
            # 결과 저장
            for metric in metrics_to_extract:
                all_values[metric].append(values.get(metric, 0))
        
        return all_values 