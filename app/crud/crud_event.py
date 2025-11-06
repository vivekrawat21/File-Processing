# app/crud/crud_event.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import List
from datetime import datetime
from app.db.models import ProcessingEvents

class CRUDProcessingEvent:
    async def log_event(
        self,
        db: AsyncSession,
        job_id: int,
        batch_id: int | None,
        level: str,
        message: str,
        data: dict | None = None,
        code: str | None = None,
    ):
        event = ProcessingEvents(
            job_id=job_id,
            batch_id=batch_id,
            level=level,
            message=message,
            code=code,
            data=data or {},
            created_at=datetime.utcnow(),
        )
        db.add(event)
        await db.commit()

    async def list_by_job(self, db: AsyncSession, job_id: int) -> List[ProcessingEvents]:
        result = await db.execute(
            select(ProcessingEvents)
            .filter(ProcessingEvents.job_id == job_id)
            .order_by(ProcessingEvents.created_at.desc())
        )
        return result.scalars().all()

event_crud = CRUDProcessingEvent()
