from typing import Dict, Any, Optional
import logging
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

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
    
    def __init__(self, db_session: AsyncSession, dart_api_service: Optional[DartApiService] = None):
        """
        서비스 초기화
        
        Args:
            db_session: 데이터베이스 세션
            dart_api_service: DART API 서비스 (없으면 새로 생성)
        """
        self.db_session = db_session
        self.dart_api = dart_api_service or DartApiService()
        logger.info("CompanyInfoService가 초기화되었습니다.")

    async def get_company_info(self, company_name: str) -> CompanySchema:
        """
        회사 정보를 조회합니다.
        
        1. DB에서 먼저 조회
        2. DB에 없으면 DART API에서 조회
        
        Args:
            company_name: 회사명
            
        Returns:
            CompanySchema: 회사 정보
            
        Raises:
            ValueError: 회사 정보를 찾을 수 없는 경우
        """
        logger.info(f"회사 정보 조회: {company_name}")
        
        try:
            # 1. DB에서 먼저 조회
            db_company = await self._get_company_from_db(company_name)
            if db_company:
                logger.info(f"DB에서 '{company_name}' 정보를 찾았습니다.")
                return self._create_company_schema_from_db(db_company)
            
            # 2. API에서 조회
            logger.info(f"DB에서 '{company_name}' 정보를 찾지 못해 DART API에서 조회합니다.")
            return await self.dart_api.fetch_company_info(company_name)
            
        except Exception as e:
            logger.error(f"회사 정보 조회 실패: {str(e)}")
            raise ValueError(f"회사 '{company_name}' 정보 조회에 실패했습니다: {str(e)}")

    async def _get_company_from_db(self, company_name: str) -> Optional[Dict[str, Any]]:
        """
        DB에서 회사 정보를 조회합니다.
        
        Args:
            company_name: 회사명
            
        Returns:
            Optional[Dict]: 회사 정보 (없으면 None)
        """
        query = text("""
            SELECT corp_code, corp_name, stock_code 
            FROM companies 
            WHERE corp_name = :company_name 
            LIMIT 1
        """)
        
        result = await self.db_session.execute(query, {"company_name": company_name})
        row = result.fetchone()
        
        if row:
            return {
                "corp_code": row[0],
                "corp_name": row[1],
                "stock_code": row[2]
            }
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