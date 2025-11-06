from celery import Celery
from app.core.config import settings

celery_app = Celery(
    "worker",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_BROKER_URL,
    include=["app.workers.tasks"],
)



celery_app.conf.update(
    task_track_started=True,
    broker_transport_options={'visibility_timeout': 3600},
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],  
    timezone='UTC',
    enable_utc=True, 
)

@celery_app.task(name="debug.ping")
def ping():
    return "pong"