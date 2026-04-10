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