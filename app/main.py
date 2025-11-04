from fastapi import FastAPI
from fastapi import APIRouter
from app.api.v1 import file_router

app = FastAPI()

router = APIRouter()

@router.get("/health-check")
async def health_check():
    return {"status": "healthy"}

app.include_router(file_router, prefix="/api")
