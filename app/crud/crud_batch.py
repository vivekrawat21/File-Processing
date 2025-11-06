# app/crud/crud_batch.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update
from typing import Optional, List
from datetime import datetime
from app.db.models import Batch, BatchStatus

class CRUDBatch:
    async def create(
        self,
        db: AsyncSession,
        job_id: int,
        index: int,
        total_items: int = 0,
    ) -> Batch:
        batch = Batch(
            job_id=job_id,
            index=index,
            total_items=total_items,
            status=BatchStatus.queued,
            created_at=datetime.utcnow(),
        )
        db.add(batch)
        await db.commit()
        await db.refresh(batch)
        return batch

    async def list_by_job(self, db: AsyncSession, job_id: int) -> List[Batch]:
        result = await db.execute(
            select(Batch).filter(Batch.job_id == job_id).order_by(Batch.index)
        )
        return result.scalars().all()

    async def update_status(
        self,
        db: AsyncSession,
        batch_id: int,
        status: BatchStatus,
        processed: int = 0,
        failed: int = 0,
        invalid: int = 0,
        error: Optional[str] = None,
    ) -> Optional[Batch]:
        result = await db.execute(select(Batch).filter(Batch.id == batch_id))
        batch = result.scalar_one_or_none()
        if not batch:
            return None
        batch.status = status
        batch.processed_items += processed
        batch.failed_items += failed
        batch.invalid_items += invalid
        batch.error = error
        if status == BatchStatus.completed:
            batch.finished_at = datetime.utcnow()
        await db.commit()
        await db.refresh(batch)
        return batch

batch_crud = CRUDBatch()
