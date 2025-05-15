from fastapi import UploadFile
from fastapi.responses import JSONResponse
from app.domain.service.esg_service import UploadService

class UploadController:
    def __init__(self):
        self.upload_service = UploadService()

    async def upload_pdf(self, file: UploadFile):
        result = await self.upload_service.save_file(file)
        return JSONResponse(content=result)