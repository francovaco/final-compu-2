import logging
import sqlite3
import time
from argparse import Namespace
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Queue, Lock
from queue import Empty


#Base de datos
def inicializar_db(db_metricas: str) -> None:
    # Crea las tablas si no existen
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


#Análisis de métricas y generación de alertas
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


#Detección de nodos caídos
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