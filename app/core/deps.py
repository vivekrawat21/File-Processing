from app.db.db_connection import AsyncSessionLocal
from app.core.config import settings
from redis.asyncio import Redis
from typing import AsyncGenerator

async def get_db() -> AsyncGenerator:
    try:
        async with AsyncSessionLocal() as session:
            yield session
            await session.commit()
            
    except Exception as e:
        await session.rollback()
        raise e
    finally:
        await session.close()
        

def get_settings():
    return settings



async def get_redis()-> AsyncGenerator:
    redis = Redis.from_url(settings.REDIS_URL)
    try:
        yield redis
    finally:
        await redis.close()
    
        
        