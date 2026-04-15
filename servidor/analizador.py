import logging
import os
import sqlite3
import time
from argparse import Namespace
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Queue, Lock
from queue import Empty


# Base de datos
def inicializar_db(db_metricas: str) -> None:
    # Crea el directorio y las tablas si no existen
    os.makedirs(os.path.dirname(db_metricas), exist_ok=True)
    conn = sqlite3.connect(db_metricas)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS metricas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nodo TEXT NOT NULL,
            cpu REAL NOT NULL,
            ram REAL NOT NULL,
            disco REAL NOT NULL,
            temperatura REAL,
            timestamp TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS alertas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nodo TEXT NOT NULL,
            tipo TEXT NOT NULL,
            valor REAL NOT NULL,
            timestamp TEXT NOT NULL
        );
    """)
    conn.commit()
    conn.close()


def guardar_metrica(metrica: dict, db_metricas: str) -> None:
    # Inserta una métrica en la base de datos
    conn = sqlite3.connect(db_metricas)
    conn.execute(
        "INSERT INTO metricas (nodo, cpu, ram, disco, temperatura, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
        (metrica["nodo"], metrica["cpu"], metrica["ram"],
         metrica["disco"], metrica.get("temperatura"), metrica["timestamp"]),
    )
    conn.commit()
    conn.close()


def guardar_alerta(alerta: dict, db_metricas: str) -> None:
    # Inserta una alerta en la base de datos
    conn = sqlite3.connect(db_metricas)
    conn.execute(
        "INSERT INTO alertas (nodo, tipo, valor, timestamp) VALUES (?, ?, ?, ?)",
        (alerta["nodo"], alerta["tipo"], alerta["valor"], alerta["timestamp"]),
    )
    conn.commit()
    conn.close()


# Análisis de métricas y generación de alertas
def evaluar_umbrales(metrica: dict, umbrales: dict) -> list[dict]:
    # Compara cada métrica contra su umbral y retorna las alertas generadas
    alertas = []
    campos = ["cpu", "ram", "disco", "temperatura"]
    for campo in campos:
        valor = metrica.get(campo)
        umbral = umbrales.get(campo)
        if valor is not None and umbral is not None and valor > umbral:
            alertas.append({
                "nodo": metrica["nodo"],
                "tipo": campo,
                "valor": valor,
                "timestamp": metrica["timestamp"],
            })
    return alertas


def analizar(metrica: dict, db_lock: Lock, umbrales: dict, db_metricas: str) -> list[dict]:
    # Analiza una métrica: evalúa umbrales, guarda en DB y loguea alertas
    alertas = evaluar_umbrales(metrica, umbrales)

    with db_lock:
        guardar_metrica(metrica, db_metricas)
        for alerta in alertas:
            guardar_alerta(alerta, db_metricas)

    for alerta in alertas:
        nivel = logging.CRITICAL if alerta["valor"] > 90 else logging.WARNING
        logging.log(nivel, "ALERTA [%s] nodo=%s valor=%.1f",
                    alerta["tipo"].upper(), alerta["nodo"], alerta["valor"])

    return alertas

    # TODO: encolar tarea Celery para envío de email

# Detección de nodos caídos
def verificar_nodos_caidos(heartbeats: dict, timeout: float, db_lock: Lock, db_metricas: str) -> None:
    # Detecta nodos que no enviaron heartbeat en más de `timeout` segundos
    ahora = time.monotonic()
    for nodo in list(heartbeats):
        if ahora - heartbeats[nodo] > timeout:
            logging.critical("ALERTA [NODO_CAIDO] nodo=%s sin heartbeat por %.0fs", nodo, timeout)
            alerta = {
                "nodo": nodo,
                "tipo": "nodo_caido",
                "valor": 0.0,
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
            }
            with db_lock:
                guardar_alerta(alerta, db_metricas)
            del heartbeats[nodo]

    # TODO: encolar tarea Celery para envío de email cuando tasks.py esté listo 

# Loop principal
def correr_analizador(queue: Queue, db_lock: Lock, args: Namespace) -> None:
    # Consume mensajes de la Queue y los procesa con un ProcessPoolExecutor
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    logging.info("Analizador iniciado con %d workers", args.workers)

    inicializar_db(args.db_metricas)

    umbrales = {
        "cpu": args.threshold_cpu,
        "ram": args.threshold_ram,
        "disco": args.threshold_disco,
        "temperatura": args.threshold_temperatura,
    }

    heartbeats: dict[str, float] = {}

    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        while True:
            try:
                mensaje = queue.get(timeout=5)
                tipo = mensaje.get("tipo")

                if tipo == "heartbeat":
                    heartbeats[mensaje["nodo"]] = time.monotonic()
                    logging.debug("Heartbeat de %s", mensaje["nodo"])

                elif tipo == "metrica":
                    executor.submit(analizar, mensaje, db_lock, umbrales, args.db_metricas)

            except Empty:
                pass

            verificar_nodos_caidos(heartbeats, args.heartbeat_timeout, db_lock, args.db_metricas)
