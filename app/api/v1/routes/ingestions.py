from fastapi import APIRouter,UploadFile,File,Depends,HTTPException
from app.core.config import settings
import os
from datetime import datetime
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.deps import get_db,get_redis
from app.crud.crud_job import job_crud 
from app.schemas.file import (
    CreateIngestionQuery,CreateIngestionResponse,JobDetail
)
from app.db.models import JobStatus
from app.core.celery_app import celery_app
from app.workers.progress import publish_progress
router = APIRouter(
    prefix="/ingestions",
    tags=["Ingestions"],)

upload_dir = settings.UPLOAD_DIR
os.makedirs(upload_dir,exist_ok=True)

@router.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    query: CreateIngestionQuery = Depends(),
    db: AsyncSession = Depends(get_db),
):
    filename = file.filename
    
    if not filename.lower().endswith(('.csv','.json')):
        raise HTTPException(status_code=400, detail="Unsupported file type. Only CSV and JSON are allowed.")
    
    
    # Generate unique filename with timestamp
    file_uri = f"{int(datetime.utcnow().timestamp())}_{filename}"
    file_path = os.path.join(upload_dir, file_uri)
    
    # Save the uploaded file
    with open(file_path,"wb") as f:
        content = await file.read()
        f.write(content)
    
    job = await job_crud.create_job(
        db,
        filename=filename,
        file_type="csv" if filename.lower().endswith('.csv') else "json",
        batch_size=query.batch_size,
        file_uri=file_uri  # Pass the file_uri to the job
    )
    await db.commit()
    await db.refresh(job)
    
    celery_app.send_task('process_job_task', args=[job.id])
    return CreateIngestionResponse(job_id=job.id,status=job.status)


@router.get("/jobs/{job_id}",response_model=JobDetail)
async def get_job(job_id:int,db:AsyncSession=Depends(get_db)):
    job = await job_crud.get_job(db,job_id)
    if not job:
        raise HTTPException(status_code=404,detail="Job not found")
    
    eta = await job_crud.compute_eta(db, job_id)
    job.eta_seconds = eta
    
    # Get batches explicitly
    from app.crud.crud_batch import batch_crud
    batches = await batch_crud.list_by_job(db, job_id)
    
    # Convert to dict for response
    job_dict = {
        "id": job.id,
        "filename": job.filename,
        "file_type": job.file_type,
        "file_uri": job.file_uri,
        "batch_size": job.batch_size,
        "total_items": job.total_items,
        "processed_items": job.processed_items,
        "failed_items": job.failed_items,
        "invalid_items": job.invalid_items,
        "status": job.status,
        "eta_seconds": job.eta_seconds,
        "error": job.error,
        "created_at": job.created_at,
        "started_at": job.started_at,
        "finished_at": job.finished_at,
        "batches": batches
    }
    
    return job_dict