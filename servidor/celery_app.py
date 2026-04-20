import os
from celery import Celery
from dotenv import load_dotenv

load_dotenv()

broker = os.getenv("CELERY_BROKER", "redis://redis:6379/0")

app = Celery("monitoreo", broker=broker, include=["servidor.tasks"])

app.conf.update(
    result_backend=None,
    task_serializer="json",
    accept_content=["json"],
)
