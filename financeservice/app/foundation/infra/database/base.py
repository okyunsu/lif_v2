from typing import Dict, Any, List, Optional
from datetime import datetime

class BaseModel:
    """기본 모델 클래스"""
    
    def to_dict(self) -> Dict[str, Any]:
        """객체를 딕셔너리로 변환합니다."""
        return {
            key: value for key, value in self.__dict__.items()
            if not key.startswith('_')
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BaseModel':
        """딕셔너리로부터 객체를 생성합니다."""
        return cls(**data) 