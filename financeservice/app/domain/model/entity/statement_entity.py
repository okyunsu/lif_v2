from datetime import datetime
from typing import Dict, Any, Optional
from app.foundation.infra.database.base import BaseModel

class Statement(BaseModel):
    """재무제표 유형 엔티티"""
    
    def __init__(
        self,
        sj_div: str,
        sj_nm: str,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None
    ):
        self.sj_div = sj_div
        self.sj_nm = sj_nm
        self.created_at = created_at or datetime.now()
        self.updated_at = updated_at or datetime.now()
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Statement':
        """딕셔너리로부터 Statement 객체를 생성합니다."""
        return cls(
            sj_div=data["sj_div"],
            sj_nm=data["sj_nm"],
            created_at=datetime.fromisoformat(data["created_at"]) if data.get("created_at") else None,
            updated_at=datetime.fromisoformat(data["updated_at"]) if data.get("updated_at") else None
        ) 