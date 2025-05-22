from datetime import datetime
from typing import Dict, Any, Optional
from app.foundation.infra.database.base import BaseModel

class Metric(BaseModel):
    """재무비율 엔티티"""
    
    def __init__(
        self,
        corp_code: str,
        bsns_year: str,
        debt_ratio: Optional[float] = None,
        current_ratio: Optional[float] = None,
        interest_coverage_ratio: Optional[float] = None,
        operating_profit_ratio: Optional[float] = None,
        net_profit_ratio: Optional[float] = None,
        roe: Optional[float] = None,
        roa: Optional[float] = None,
        debt_dependency: Optional[float] = None,
        cash_flow_debt_ratio: Optional[float] = None,
        sales_growth: Optional[float] = None,
        operating_profit_growth: Optional[float] = None,
        eps_growth: Optional[float] = None,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None
    ):
        self.corp_code = corp_code
        self.bsns_year = bsns_year
        self.debt_ratio = debt_ratio
        self.current_ratio = current_ratio
        self.interest_coverage_ratio = interest_coverage_ratio
        self.operating_profit_ratio = operating_profit_ratio
        self.net_profit_ratio = net_profit_ratio
        self.roe = roe
        self.roa = roa
        self.debt_dependency = debt_dependency
        self.cash_flow_debt_ratio = cash_flow_debt_ratio
        self.sales_growth = sales_growth
        self.operating_profit_growth = operating_profit_growth
        self.eps_growth = eps_growth
        self.created_at = created_at or datetime.now()
        self.updated_at = updated_at or datetime.now()
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Metric':
        """딕셔너리로부터 Metric 객체를 생성합니다."""
        return cls(
            corp_code=data["corp_code"],
            bsns_year=data["bsns_year"],
            debt_ratio=data.get("debt_ratio"),
            current_ratio=data.get("current_ratio"),
            interest_coverage_ratio=data.get("interest_coverage_ratio"),
            operating_profit_ratio=data.get("operating_profit_ratio"),
            net_profit_ratio=data.get("net_profit_ratio"),
            roe=data.get("roe"),
            roa=data.get("roa"),
            debt_dependency=data.get("debt_dependency"),
            cash_flow_debt_ratio=data.get("cash_flow_debt_ratio"),
            sales_growth=data.get("sales_growth"),
            operating_profit_growth=data.get("operating_profit_growth"),
            eps_growth=data.get("eps_growth"),
            created_at=datetime.fromisoformat(data["created_at"]) if data.get("created_at") else None,
            updated_at=datetime.fromisoformat(data["updated_at"]) if data.get("updated_at") else None
        ) 