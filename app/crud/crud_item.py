from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from typing import List, Dict, Optional
from datetime import datetime
from app.db.models import Item, ItemStatus

class CRUDItem:
    async def bulk_create(
        self, db: AsyncSession, job_id: int, batch_id: int, items: List[Dict]
    ):
        objects = [
            Item(
                job_id=job_id,
                batch_id=batch_id,
                email=item["email"],
                payload=item.get("payload", {}),
                status=ItemStatus.pending,
                created_at=datetime.utcnow(),
            )
            for item in items
        ]
        db.add_all(objects)
        await db.commit()

    async def list_by_batch(
        self, db: AsyncSession, batch_id: int, skip=0, limit=100
    ) -> List[Item]:
        result = await db.execute(
            select(Item)
            .filter(Item.batch_id == batch_id)
            .order_by(Item.id)
            .offset(skip)
            .limit(limit)
        )
        return result.scalars().all()

    async def list_by_job(
        self, db: AsyncSession, job_id: int, status: Optional[ItemStatus] = None, skip=0, limit=100
    ) -> List[Item]:
        """List items by job_id with optional status filter"""
        query = select(Item).filter(Item.job_id == job_id)
        
        if status:
            query = query.filter(Item.status == status)
            
        query = query.order_by(Item.id).offset(skip).limit(limit)
        
        result = await db.execute(query)
        return result.scalars().all()

    async def count_by_job(
        self, db: AsyncSession, job_id: int, status: Optional[ItemStatus] = None
    ) -> int:
        """Count items by job_id with optional status filter"""
        query = select(func.count(Item.id)).filter(Item.job_id == job_id)
        
        if status:
            query = query.filter(Item.status == status)
            
        result = await db.execute(query)
        return result.scalar()

    async def get_validation_stats(self, db: AsyncSession, job_id: int) -> Dict:
        """Get validation statistics for a job"""
        result = await db.execute(
            select(
                Item.status,
                func.count(Item.id).label('count')
            )
            .filter(Item.job_id == job_id)
            .group_by(Item.status)
        )
        
        stats = {status.value: 0 for status in ItemStatus}
        for status, count in result:
            stats[status.value] = count
            
        return stats

    async def update_status(
        self,
        db: AsyncSession,
        item_id: int,
        status: ItemStatus,
        error: Optional[str] = None,
    ) -> Optional[Item]:
        result = await db.execute(select(Item).filter(Item.id == item_id))
        item = result.scalar_one_or_none()
        if not item:
            return None
        item.status = status
        if error:
            item.error = error
        item.processed_at = datetime.utcnow()
        await db.commit()
        await db.refresh(item)
        return item

item_crud = CRUDItem()
