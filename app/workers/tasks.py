import asyncio
import os
from datetime import datetime

from app.core.celery_app import celery_app as celery
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from app.crud.crud_job import job_crud
from app.crud.crud_batch import batch_crud
from app.core.config import settings
from app.db.models import BatchStatus, Item, ItemStatus, JobStatus
from app.storage.local_storage import open_local_file
from app.workers.progress import publish_progress
from redis.asyncio import Redis

@celery.task(name="process_job_task", bind=True, 
             max_retries=3, 
             default_retry_delay=5,
             autoretry_for=(ValueError, ConnectionError, RuntimeError),
             retry_backoff=True,
             retry_jitter=True,
             acks_late=True,  # Only acknowledge after task completes
             reject_on_worker_lost=True)  # Requeue if worker dies
def process_job_task(self, job_id: int):
    print(f"[DEBUG] Starting process_job_task for job_id: {job_id}")  # Debug log
    if not isinstance(job_id, int):
        print(f"[ERROR] Invalid job_id type: {type(job_id)}")  # Debug log
        raise ValueError(f"Invalid job_id: {job_id}, must be an integer")

    loop = None
    try:
        print(f"[DEBUG] Setting up event loop for job_id: {job_id}")  # Debug log
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            result = loop.run_until_complete(process_job_async(job_id))
            
            if not result or not result.get("success"):  # Check explicit success flag
                error_msg = result.get("error") if result else "Unknown error"
                print(f"[ERROR] Failed to process job {job_id}: {error_msg}")  # Debug log
                raise ValueError(error_msg)
                
            print(f"[DEBUG] Successfully processed job_id: {job_id}")  # Debug log
            return result
            
        except asyncio.CancelledError as e:
            error_msg = f"Job {job_id} was cancelled: {str(e)}"
            print(f"[ERROR] {error_msg}")  # Debug log
            raise ValueError(error_msg)
            
        except RuntimeError as e:
            error_msg = f"Runtime error in job {job_id}: {str(e)}"
            print(f"[ERROR] {error_msg}")  # Debug log
            raise ValueError(error_msg)
            
    except ValueError as e:
        error = {
            'exc_type': 'ValueError',
            'exc_message': str(e),
            'job_id': job_id
        }
        print(f"[ERROR] ValueError in process_job_task: {str(e)}")  # Debug log
        self.update_state(state='RETRY', meta=error)
        raise ValueError(error)  # Re-raise with serializable error info
        
    except Exception as e:
        error = {
            'exc_type': e.__class__.__name__,
            'exc_message': str(e),
            'job_id': job_id
        }
        print(f"[ERROR] Unexpected error in process_job_task: {str(e)}")  # Debug log
        self.update_state(state='FAILURE', meta=error)
        raise ValueError(error)  # Convert to ValueError for consistent handling
        
    finally:
        if loop:
            try:
                # Cancel pending tasks
                for task in asyncio.all_tasks(loop):
                    task.cancel()
                # Run loop until all tasks are cancelled
                loop.run_until_complete(asyncio.gather(*asyncio.all_tasks(loop), return_exceptions=True))
            except Exception as e:
                print(f"[ERROR] Error during task cleanup: {str(e)}")
            finally:
                try:
                    loop.stop()
                    loop.close()
                except Exception as e:
                    print(f"[ERROR] Error closing loop: {str(e)}")
                asyncio.set_event_loop(None)
    
async def process_job_async(job_id: int):
    print(f"[DEBUG] Starting process_job_async for job_id: {job_id}")  # Debug log
    result = {"success": False, "error": None}
    engine = None
    redis = None
    job = None

    try:
        engine = create_async_engine(
            settings.DATABASE_URL,
            echo=settings.DEBUG,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
        )
        session_factory = sessionmaker(
            bind=engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

        redis = Redis.from_url(settings.REDIS_URL, decode_responses=True)

        async with session_factory() as session:
            try:
                job = await job_crud.get_job(session, job_id)
                if not job:
                    error_msg = f"Job {job_id} not found"
                    if redis:
                        await publish_progress(redis, job_id, kind="job_failed", error=error_msg)
                    raise ValueError(error_msg)

                if not job.started_at:
                    job.status = JobStatus.parsing
                    job.started_at = datetime.utcnow()

                await session.commit()
                await session.refresh(job)
            except Exception:
                if session.in_transaction():
                    await session.rollback()
                raise

        file_path = os.path.join(settings.UPLOAD_DIR, job.file_uri)

        try:
            async for batch_data in open_local_file(file_path, job.batch_size):
                batch_index = batch_data["index"]
                batch_rows = batch_data["rows"]
                batch = await create_batch(session_factory, job.id, batch_index, len(batch_rows))
                await process_batch(session_factory, redis, job.id, batch.id, batch_rows)

            await finalize_job(session_factory, job_id)
            result["success"] = True
            return result

        except Exception as exc:
            error_msg = f"Error processing job {job_id}: {exc}"
            if redis:
                await publish_progress(redis, job_id, kind="job_failed", error=error_msg)
            raise ValueError(error_msg)

    except Exception as exc:
        error_msg = f"Error in process_job_async: {exc}"
        print(f"[ERROR] {error_msg}")  # Debug log
        result["error"] = error_msg
        raise ValueError(error_msg)

    finally:
        if redis:
            try:
                await redis.close()
            except Exception as exc:
                print(f"[ERROR] Error closing redis: {exc}")

        if engine:
            try:
                await engine.dispose()
            except Exception as exc:
                print(f"[ERROR] Error disposing engine: {exc}")


async def create_batch(session_factory, job_id: int, index: int, total_items: int):
    """Create a batch for the given job."""
    async with session_factory() as session:
        try:
            batch = await batch_crud.create(session, job_id=job_id, index=index, total_items=total_items)
            return batch
        except Exception:
            if session.in_transaction():
                await session.rollback()
            raise


async def finalize_job(session_factory, job_id: int):
    """Mark a job as completed."""
    async with session_factory() as session:
        try:
            job = await job_crud.get_job(session, job_id)
            if job:
                job.status = JobStatus.completed
                job.finished_at = datetime.utcnow()
                await session.commit()
        except Exception:
            if session.in_transaction():
                await session.rollback()
            raise


async def process_batch(session_factory, redis, job_id, batch_id, rows):
    """Process a batch of rows and persist results."""
    processed, failed, invalid = 0, 0, 0
    items_to_create = []

    for row in rows:
        email = row.get("email", "")
        payload = row.get("data", {})

        if "@" not in email:
            status = ItemStatus.invalid
            invalid += 1
        else:
            status = ItemStatus.processed
            processed += 1

        items_to_create.append(
            Item(
                job_id=job_id,
                batch_id=batch_id,
                email=email,
                payload=payload,
                status=status,
                processed_at=datetime.utcnow() if status == ItemStatus.processed else None,
            )
        )
        await asyncio.sleep(0.05)  # simulate IO work

    async with session_factory() as session:
        try:
            session.add_all(items_to_create)
            await session.commit()
        except Exception:
            if session.in_transaction():
                await session.rollback()
            raise

    async with session_factory() as session:
        try:
            await batch_crud.update_status(
                session,
                batch_id,
                BatchStatus.completed,
                processed=processed,
                failed=failed,
                invalid=invalid,
            )
        except Exception:
            if session.in_transaction():
                await session.rollback()
            raise

    async with session_factory() as session:
        try:
            await job_crud.increment_counters(
                session,
                job_id,
                processed=processed,
                failed=failed,
                invalid=invalid,
            )
        except Exception:
            if session.in_transaction():
                await session.rollback()
            raise

    if redis:
        await publish_progress(
            redis,
            job_id,
            kind="batch_progress",
            batch_id=batch_id,
            processed=processed,
        )

    return True