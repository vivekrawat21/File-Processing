from fastapi import APIRouter,UploadFile,File

router = APIRouter(
    prefix="/file",
    tags=["file"])

@router.post("/upload_file")
async def upload_file(file: UploadFile = File(..., description="Upload a file to process")):
    return {"filename": file.filename}
