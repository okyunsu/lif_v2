from supabase import create_client
import os
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# Supabase 클라이언트 초기화
supabase = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_ANON_KEY")
) 