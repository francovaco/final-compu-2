import logging
import os
import smtplib
import sqlite3
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
from dotenv import load_dotenv
from servidor.celery_app import app

load_dotenv()

# Email
def _enviar_email(asunto: str, cuerpo: str) -> None:
    # Envía un email usando las variables de entorno configuradas.
    # Si EMAIL_DESTINATARIO no está configurado, no hace nada.
    remitente    = os.getenv("EMAIL_REMITENTE")
    password     = os.getenv("EMAIL_PASSWORD")
    destinatario = os.getenv("EMAIL_DESTINATARIO")
    smtp_host    = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port    = int(os.getenv("SMTP_PORT", 587))

    if not destinatario:
        return  # envío desactivado silenciosamente

    msg = MIMEText(cuerpo)
    msg["Subject"] = asunto
    msg["From"]    = remitente
    msg["To"]      = destinatario

    with smtplib.SMTP(smtp_host, smtp_port) as servidor:
        servidor.ehlo()
        servidor.starttls()
        servidor.login(remitente, password)
        servidor.sendmail(remitente, [destinatario], msg.as_string())


@app.task
def enviar_alerta_email(nodo: str, tipo: str, valor: float, timestamp: str) -> None:
    # Envía un email de alerta cuando una métrica supera su umbral.
    asunto = f"[ALERTA] {tipo.upper()} en {nodo}"
    cuerpo = (
        f"Se detectó una alerta en el nodo {nodo}.\n\n"
        f"Tipo:      {tipo.upper()}\n"
        f"Valor:     {valor:.1f}\n"
        f"Timestamp: {timestamp}\n"
    )
    try:
        _enviar_email(asunto, cuerpo)
        logging.info("Email de alerta enviado: nodo=%s tipo=%s", nodo, tipo)
    except Exception as exc:
        logging.error("Error al enviar email de alerta: %s", exc)


@app.task
def enviar_alerta_nodo_caido(nodo: str, timestamp: str) -> None:
    # Envía un email cuando un nodo deja de enviar heartbeats.
    asunto = f"[ALERTA] Nodo caído: {nodo}"
    cuerpo = (
        f"El nodo {nodo} dejó de responder.\n\n"
        f"Último heartbeat registrado antes de: {timestamp}\n"
    )
    try:
        _enviar_email(asunto, cuerpo)
        logging.info("Email de nodo caído enviado: nodo=%s", nodo)
    except Exception as exc:
        logging.error("Error al enviar email de nodo caído: %s", exc)


# Reportes históricos
@app.task
def generar_reporte(db_metricas: str, db_reportes: str, nodo: str, horas: int = 24) -> None:
    # Lee métricas de las últimas `horas` horas para `nodo` y escribe un reporte en reportes.db.
    periodo_fin   = datetime.now(timezone.utc)
    periodo_inicio = periodo_fin - timedelta(hours=horas)

    # Leer métricas del periodo
    conn_metricas = sqlite3.connect(db_metricas)
    conn_metricas.row_factory = sqlite3.Row
    filas = conn_metricas.execute(
        """
        SELECT cpu, ram, disco, temperatura
        FROM metricas
        WHERE nodo = ? AND timestamp BETWEEN ? AND ?
        """,
        (nodo, periodo_inicio.isoformat(), periodo_fin.isoformat()),
    ).fetchall()
    conn_metricas.close()

    if not filas:
        logging.info("Sin métricas para generar reporte: nodo=%s", nodo)
        return

    def stats(valores):
        validos = [v for v in valores if v is not None]
        if not validos:
            return None, None, None
        return sum(validos) / len(validos), max(validos), min(validos)

    cpu_prom,   cpu_max,   cpu_min   = stats([f["cpu"]         for f in filas])
    ram_prom,   ram_max,   ram_min   = stats([f["ram"]         for f in filas])
    disco_prom, disco_max, disco_min = stats([f["disco"]       for f in filas])
    temp_prom,  temp_max,  temp_min  = stats([f["temperatura"] for f in filas])

    # Inicializar reportes.db si no existe
    conn_reportes = sqlite3.connect(db_reportes)
    conn_reportes.execute("""
        CREATE TABLE IF NOT EXISTS reportes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nodo TEXT NOT NULL,
            periodo_inicio TEXT NOT NULL,
            periodo_fin TEXT NOT NULL,
            cpu_promedio REAL, cpu_maximo REAL, cpu_minimo REAL,
            ram_promedio REAL, ram_maximo REAL, ram_minimo REAL,
            disco_promedio REAL, disco_maximo REAL, disco_minimo REAL,
            temperatura_promedio REAL, temperatura_maxima REAL, temperatura_minima REAL,
            timestamp TEXT NOT NULL
        )
    """)
    conn_reportes.execute(
        """
        INSERT INTO reportes (
            nodo, periodo_inicio, periodo_fin,
            cpu_promedio, cpu_maximo, cpu_minimo,
            ram_promedio, ram_maximo, ram_minimo,
            disco_promedio, disco_maximo, disco_minimo,
            temperatura_promedio, temperatura_maxima, temperatura_minima,
            timestamp
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            nodo,
            periodo_inicio.isoformat(), periodo_fin.isoformat(),
            cpu_prom,   cpu_max,   cpu_min,
            ram_prom,   ram_max,   ram_min,
            disco_prom, disco_max, disco_min,
            temp_prom,  temp_max,  temp_min,
            datetime.now(timezone.utc).isoformat(),
        ),
    )
    conn_reportes.commit()
    conn_reportes.close()

    logging.info("Reporte generado: nodo=%s periodo=%dh", nodo, horas)


# Limpieza de métricas antiguas
@app.task
def limpiar_metricas(db_metricas: str, retencion_horas: int) -> None:
    # Borra métricas más antiguas que `retencion_horas` horas de metricas.db.
    limite = datetime.now(timezone.utc) - timedelta(hours=retencion_horas)
    conn = sqlite3.connect(db_metricas)
    cursor = conn.execute(
        "DELETE FROM metricas WHERE timestamp < ?",
        (limite.isoformat(),),
    )
    eliminadas = cursor.rowcount
    conn.commit()
    conn.close()
    logging.info("Limpieza completada: %d métricas eliminadas (retención %dh)", eliminadas, retencion_horas)
