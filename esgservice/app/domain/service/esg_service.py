import os
from fastapi import UploadFile
import aiofiles

class UploadService:
    def __init__(self):
        self.upload_dir = os.getenv("UPLOAD_DIR", "./uploads")
        os.makedirs(self.upload_dir, exist_ok=True)

    async def save_file(self, file: UploadFile) -> dict:
        file_path = os.path.join(self.upload_dir, file.filename)

        try:
            async with aiofiles.open(file_path, "wb") as out_file:
                content = await file.read()
                await out_file.write(content)

            return {
                "filename": file.filename,
                "status": "✅ 파일 업로드 성공",
                "path": file_path
            }
        except Exception as e:
            return {
                "filename": file.filename,
                "status": "❌ 파일 업로드 실패",
                "error": str(e)
            }