from pydantic import BaseModel, EmailStr,Field
from typing import Optional,Literal,Any
from datetime import datetime

JobStatus = Literal["queued", "parsing", "completed", "failed", "canceled"]
BatchStatus = Literal["queued", "running", "completed", "failed"]
ItemStatus = Literal["pending", "processed", "invalid", "failed"]

class CreateIngestionResponse(BaseModel):
    job_id: int
    status: JobStatus
    
class CreateIngestionQuery(BaseModel):
    batch_size: int  = Field(..., ge=1, le=1000)

class JobSummary(BaseModel):
    id:int
    filename: str
    file_type: str
    batch_size: int
    status: JobStatus
    total_items: int
    processed_items: int
    failed_items: int
    invalid_items: int
    created_at: datetime
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    eta_seconds: Optional[int]
    
class BatchSummary(BaseModel):
    id:int
    index: int
    status: BatchStatus
    total_items: int
    processed_items: int
    failed_items: int
    invalid_items: int
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    error: Optional[str]
    
class ItemSummary(BaseModel):
    id:int
    email: EmailStr
    error: Optional[str]=None
    status: ItemStatus
    processed_at: Optional[datetime]= None
    payload: Optional[dict[str, Any]] = None
    
class JobDetail(JobSummary):
    batches: list[BatchSummary] = []

class ProgressEvent(BaseModel):
   #used over websockets to send progress updates
    type: Literal["job_progress","batch_progress","item_processed"]
    job_id: int
    data: dict