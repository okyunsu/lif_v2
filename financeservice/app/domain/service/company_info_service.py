from typing import Dict, Any, Optional
import logging
from datetime import datetime
from app.foundation.infra.database.supabase_client import supabase

from app.domain.service.dart_api_service import DartApiService
from app.domain.model.schema.company_schema import CompanySchema

logger = logging.getLogger(__name__)

class CompanyInfoService:
    """
    회사 정보 조회 서비스
    
    회사 정보를 DB에서 조회하고, 없으면 DART API를 통해 조회합니다.
    
    의존성:
    - DartApiService: DART API 통신을 위한 서비스
    """
    
    def __init__(self, dart_api_service: Optional[DartApiService] = None):
        """
        서비스 초기화
        
        Args:
            dart_api_service: DART API 서비스 (없으면 새로 생성)
        """
        self.dart_api = dart_api_service or DartApiService()
        logger.info("CompanyInfoService가 초기화되었습니다.")

    async def get_company_info(self, company_name: str) -> CompanySchema:
        """
        회사 정보를 조회합니다.
        
        Args:
            company_name: 회사명
            
        Returns:
            CompanySchema: 회사 정보
            
        Raises:
            ValueError: 회사 정보를 찾을 수 없는 경우
        """
        # 1. DB에서 회사 정보 조회
        db_company = await self._get_company_from_db(company_name)
        if db_company:
            return self._create_company_schema_from_db(db_company)
            
        # 2. DART API에서 회사 정보 조회
        dart_company = await self.dart_api.get_company_info(company_name)
        if not dart_company:
            raise ValueError(f"회사 정보를 찾을 수 없습니다: {company_name}")
            
        # 3. 회사 정보 저장
        await self._save_company_info(dart_company)
        
        return CompanySchema(**dart_company)

    async def _get_company_from_db(self, company_name: str) -> Optional[Dict[str, Any]]:
        """
        DB에서 회사 정보를 조회합니다.
        
        Args:
            company_name: 회사명
            
        Returns:
            Optional[Dict]: 회사 정보 (없으면 None)
        """
        try:
            response = supabase.table("companies")\
                .select("corp_code, corp_name, stock_code")\
                .eq("corp_name", company_name)\
                .limit(1)\
                .execute()
                
            if response.status_code != 200:
                raise RuntimeError(f"회사 정보 조회 실패: {response.status_code} - {response.data}")
                
            if response.data:
                return response.data[0]
            return None
        except Exception as e:
            logger.error(f"회사 정보 조회 중 오류 발생: {str(e)}")
            return None
    
    def _create_company_schema_from_db(self, db_company: Dict[str, Any]) -> CompanySchema:
        """
        DB 데이터로부터 CompanySchema 객체를 생성합니다.
        
        Args:
            db_company: DB에서 조회한 회사 정보
            
        Returns:
            CompanySchema: 회사 정보 객체
        """
        now = datetime.now().isoformat()
        return CompanySchema(
            corp_code=db_company["corp_code"],
            corp_name=db_company["corp_name"],
            stock_code=db_company["stock_code"],
            created_at=now,
            updated_at=now
        )
        
    async def _save_company_info(self, company_info: Dict[str, Any]) -> None:
        """
        회사 정보를 DB에 저장합니다.
        
        Args:
            company_info: 회사 정보
        """
        try:
            response = supabase.table("companies")\
                .upsert({
                    "corp_code": company_info["corp_code"],
                    "corp_name": company_info["corp_name"],
                    "stock_code": company_info.get("stock_code", "")
                })\
                .execute()
                
            if response.status_code not in (200, 201):
                raise RuntimeError(f"회사 정보 저장 실패: {response.status_code} - {response.data}")
        except Exception as e:
            logger.error(f"회사 정보 저장 중 오류 발생: {str(e)}")
            raise 