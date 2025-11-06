# app/workers/progress.py
import orjson
from redis.asyncio import Redis

CHANNEL_TEMPLATE = "job:{job_id}:progress"

async def publish_progress(redis: Redis, job_id: int, kind: str, **data):
    payload = orjson.dumps({"type": kind, "job_id": job_id, "data": data})
    await redis.publish(CHANNEL_TEMPLATE.format(job_id=job_id), payload)
