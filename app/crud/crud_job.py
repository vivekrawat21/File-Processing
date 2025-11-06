from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select,update
from app.db.models import Job, JobStatus
from app.db.models import Job,JobStatus
from app.schemas.file import CreateIngestionResponse
from datetime import datetime
from typing import Optional,List


class CRUDJob:
    async def create_job(self, db: AsyncSession, filename: str, file_type: str, batch_size: int, file_uri: str) -> Job:
        job = Job(
            filename=filename,
            file_type=file_type,
            file_uri=file_uri,
            batch_size=batch_size,
            status=JobStatus.queued,
            created_at=datetime.utcnow()
        )
        db.add(job)
        await db.commit()
        await db.refresh(job)
        return job
    async def get_job(self, db: AsyncSession, job_id: int) -> Optional[Job]:
        result = await db.execute(select(Job).where(Job.id == job_id))
        return result.scalars().first()
    
    async def list(self,db: AsyncSession, skip: int = 0, limit: int = 100) -> List[Job]:
        result = await db.execute(select(Job).offset(skip).limit(limit))
        return result.scalars().all()
    
    async def update_status(self,db:AsyncSession,job_id:int, status:JobStatus,error:Optional[str]=None, finished:bool=False)->Optional[Job]:
        job = await self.get_job(db,job_id)
        if not job:
            return None
        job.status = status
        if error:
            job.error = error
        if finished:
            job.finished_at = datetime.utcnow()
        await db.commit()
        await db.refresh(job)
        return job
    
    async def compute_eta(self, db: AsyncSession, job_id: int) -> Optional[int]:
        job = await self.get_job(db, job_id)
        if not job or not job.started_at or job.processed_items == 0:
            return None
        elapsed = (datetime.utcnow() - job.started_at).total_seconds()
        rate = job.processed_items / elapsed if elapsed > 0 else 0
        remaining_items = job.total_items - job.processed_items
        eta = int(remaining_items / rate) if rate > 0 else None
        return eta
    
    async def increment_counters(self, db: AsyncSession, job_id: int, processed: int = 0, failed: int = 0, invalid: int = 0):
        """Increment the counters for a job's items."""
        query = (
            update(Job)
            .where(Job.id == job_id)
            .values(
                processed_items=Job.processed_items + processed,
                failed_items=Job.failed_items + failed,
                invalid_items=Job.invalid_items + invalid
            )
            .returning(Job)
        )
        result = await db.execute(query)
        job = result.scalar_one_or_none()
        if job:
            await db.commit()
            await db.refresh(job)
        return job
    
job_crud = CRUDJob()