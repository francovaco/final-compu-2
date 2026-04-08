import argparse
import json
import logging
import socket
import time
from datetime import datetime

import psutil


def configurar_logging(nivel: str) -> None:
    # Configura el formato y nivel de logs
    logging.basicConfig(
        level=getattr(logging, nivel.upper()),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def obtener_temperatura() -> float | None:
    # Retorna la temperatura de la CPU o None si no está disponible
    try:
        temps = psutil.sensors_temperatures()
        if temps:
            for entradas in temps.values():
                if entradas:
                    return entradas[0].current
    except (AttributeError, NotImplementedError):
        pass
    return None