import os

from celery import Celery
from dotenv import load_dotenv

load_dotenv()

# Broker Redis — se puede sobreescribir con la variable de entorno CELERY_BROKER
broker = os.getenv("CELERY_BROKER", "redis://redis:6379/0")

app = Celery("monitoreo", broker=broker, include=["servidor.tasks"])

app.conf.update(
    result_backend=None,   # no necesitamos guardar resultados de tareas
    task_serializer="json",
    accept_content=["json"],
)
