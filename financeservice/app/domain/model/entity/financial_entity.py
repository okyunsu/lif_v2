from datetime import datetime
from typing import Dict, Any, Optional
from app.foundation.infra.database.base import BaseModel

class Financial(BaseModel):
    """재무제표 데이터 엔티티"""
    
    def __init__(
        self,
        corp_code: str,
        bsns_year: str,
        sj_div: str,
        account_nm: str,
        thstrm_nm: Optional[str] = None,
        thstrm_amount: Optional[float] = None,
        frmtrm_nm: Optional[str] = None,
        frmtrm_amount: Optional[float] = None,
        bfefrmtrm_nm: Optional[str] = None,
        bfefrmtrm_amount: Optional[float] = None,
        ord: Optional[int] = None,
        currency: Optional[str] = None,
        rcept_no: Optional[str] = None,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None
    ):
        self.corp_code = corp_code
        self.bsns_year = bsns_year
        self.sj_div = sj_div
        self.account_nm = account_nm
        self.thstrm_nm = thstrm_nm
        self.thstrm_amount = thstrm_amount
        self.frmtrm_nm = frmtrm_nm
        self.frmtrm_amount = frmtrm_amount
        self.bfefrmtrm_nm = bfefrmtrm_nm
        self.bfefrmtrm_amount = bfefrmtrm_amount
        self.ord = ord
        self.currency = currency
        self.rcept_no = rcept_no
        self.created_at = created_at or datetime.now()
        self.updated_at = updated_at or datetime.now()
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Financial':
        """딕셔너리로부터 Financial 객체를 생성합니다."""
        return cls(
            corp_code=data["corp_code"],
            bsns_year=data["bsns_year"],
            sj_div=data["sj_div"],
            account_nm=data["account_nm"],
            thstrm_nm=data.get("thstrm_nm"),
            thstrm_amount=data.get("thstrm_amount"),
            frmtrm_nm=data.get("frmtrm_nm"),
            frmtrm_amount=data.get("frmtrm_amount"),
            bfefrmtrm_nm=data.get("bfefrmtrm_nm"),
            bfefrmtrm_amount=data.get("bfefrmtrm_amount"),
            ord=data.get("ord"),
            currency=data.get("currency"),
            rcept_no=data.get("rcept_no"),
            created_at=datetime.fromisoformat(data["created_at"]) if data.get("created_at") else None,
            updated_at=datetime.fromisoformat(data["updated_at"]) if data.get("updated_at") else None
        ) 