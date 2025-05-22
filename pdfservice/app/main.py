from fastapi import FastAPI
from app.api.appendix_parser_router import router as appendix_router


app = FastAPI(title="ESG Appendix Parser Service")
app.include_router(appendix_router)
