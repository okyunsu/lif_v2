from datetime import datetime
from typing import Dict, Any, Optional
from app.foundation.infra.database.base import BaseModel

class Report(BaseModel):
    """보고서 정보 엔티티"""
    
    def __init__(
        self,
        rcept_no: str,
        reprt_code: str,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None
    ):
        self.rcept_no = rcept_no
        self.reprt_code = reprt_code
        self.created_at = created_at or datetime.now()
        self.updated_at = updated_at or datetime.now()
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Report':
        """딕셔너리로부터 Report 객체를 생성합니다."""
        return cls(
            rcept_no=data["rcept_no"],
            reprt_code=data["reprt_code"],
            created_at=datetime.fromisoformat(data["created_at"]) if data.get("created_at") else None,
            updated_at=datetime.fromisoformat(data["updated_at"]) if data.get("updated_at") else None
        ) 