from fastapi import APIRouter, UploadFile, File
from app.domain.controller.appendix_parser_controller import AppendixParserController
import tempfile
import shutil

router = APIRouter(prefix="/appendix", tags=["AppendixParser"])
controller = AppendixParserController()

@router.post("/parse", summary="Appendix 표 기반 ESG 지표 추출")
async def parse_appendix_pdf(file: UploadFile = File(...)):
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        shutil.copyfileobj(file.file, tmp)
        tmp_path = tmp.name

    result = controller.parse(tmp_path)
    return {"status": "success", "data": result}
