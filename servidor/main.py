import argparse
import asyncio
import logging
import multiprocessing
from servidor.analizador import correr_analizador
from servidor.asyncio_server import correr_servidor


def configurar_logging(nivel: str) -> None:
    # Configura el formato y nivel de logs
    logging.basicConfig(
        level=getattr(logging, nivel.upper()),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Servidor central de monitoreo")
    parser.add_argument("--host", default="0.0.0.0", help="Interfaz donde escuchar")
    parser.add_argument("--port-tcp", type=int, default=9000, help="Puerto TCP para métricas")
    parser.add_argument("--port-udp", type=int, default=9001, help="Puerto UDP para heartbeats")
    parser.add_argument("--threshold-cpu", type=float, default=80, help="Umbral de alerta CPU en %%")
    parser.add_argument("--threshold-ram", type=float, default=80, help="Umbral de alerta RAM en %%")
    parser.add_argument("--threshold-disco", type=float, default=90, help="Umbral de alerta disco en %%")
    parser.add_argument("--threshold-temperatura", type=float, default=85, help="Umbral de alerta temperatura en °C")
    parser.add_argument("--heartbeat-timeout", type=float, default=30, help="Segundos sin heartbeat para considerar nodo caído")
    parser.add_argument("--workers", type=int, default=4, help="Workers del ProcessPoolExecutor en el analizador")
    parser.add_argument("--db-metricas", default="db/metricas.db", help="Ruta del SQLite de métricas")
    parser.add_argument("--db-reportes", default="db/reportes.db", help="Ruta del SQLite de reportes")
    parser.add_argument("--db-retencion", type=float, default=72, help="Horas máximas de retención de métricas")
    parser.add_argument("--celery-broker", default="redis://redis:6379/0", help="URL del broker Redis para Celery")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    configurar_logging(args.log_level)
    logging.info("Iniciando servidor central")

    # Cola compartida entre asyncio y el analizador
    queue = multiprocessing.Queue()
    manager = multiprocessing.Manager()
    db_lock = manager.Lock()

    # Lanzar el analizador como proceso hijo (sin daemon para que pueda crear workers)
    proceso_analizador = multiprocessing.Process(
        target=correr_analizador,
        args=(queue, db_lock, args),
    )
    proceso_analizador.start()
    logging.info("Proceso analizador iniciado (PID %d)", proceso_analizador.pid)

    # Correr el servidor asyncio en el proceso principal
    try:
        asyncio.run(correr_servidor(queue, args))
    except KeyboardInterrupt:
        logging.info("Servidor detenido")
    finally:
        proceso_analizador.terminate()
        proceso_analizador.join()
        manager.shutdown()
        logging.info("Proceso analizador terminado")


if __name__ == "__main__":
    main()
