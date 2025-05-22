from datetime import datetime
from typing import Dict, Any, Optional
from app.foundation.infra.database.base import BaseModel

class Company(BaseModel):
    """회사 정보 엔티티"""
    
    def __init__(
        self,
        corp_code: str,
        corp_name: str,
        stock_code: Optional[str] = None,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None
    ):
        self.corp_code = corp_code
        self.corp_name = corp_name
        self.stock_code = stock_code or ""
        self.created_at = created_at or datetime.now()
        self.updated_at = updated_at or datetime.now()
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Company':
        """딕셔너리로부터 Company 객체를 생성합니다."""
        return cls(
            corp_code=data["corp_code"],
            corp_name=data["corp_name"],
            stock_code=data.get("stock_code"),
            created_at=datetime.fromisoformat(data["created_at"]) if data.get("created_at") else None,
            updated_at=datetime.fromisoformat(data["updated_at"]) if data.get("updated_at") else None
        ) 