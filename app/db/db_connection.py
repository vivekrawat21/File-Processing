from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from app.core.config import settings

engine = create_async_engine(settings.DATABASE_URL, echo=settings.DEBUG)

AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

async def get_db():
    try:
        async with AsyncSessionLocal() as session:
            yield session
            await session.commit()
            
    except Exception as e:
        await session.rollback()
        raise e
    finally:
        await session.close()