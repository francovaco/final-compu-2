# Final computación 2
## Sistema de monitoreo de servidores distribuido

**Franco Vaccarezza — Legajo 63179**

Sistema cliente-servidor que monitorea el estado de múltiples servidores en tiempo real.

## Funcionamiento

En cada servidor a monitorear corre un **agente** (cliente) que recolecta métricas del sistema — CPU, RAM, disco y temperatura — y las envía cada N segundos al servidor central por **TCP**. Paralelamente, el agente envía un **heartbeat** por **UDP** para indicar que sigue activo.

El **servidor central** recibe las métricas y las delega a un proceso analizador. El analizador las compara contra los umbrales configurados y, si alguna los supera, genera una alerta. Si un agente deja de enviar heartbeats por más de N segundos, se lo considera caído y se genera una alerta de nodo caído.

Todas las métricas y alertas se persisten en **SQLite**. Las alertas se envían por **email** a través de Celery, que corre como proceso separado y usa Redis como broker de tareas. Periódicamente, Celery también genera **reportes históricos** con promedios, máximos y mínimos de cada nodo.

## Levantar el sistema

Requiere Docker y Docker Compose. Antes de levantar, completar el archivo `.env` con las credenciales de email (ver `.env.example`).

```bash
# Levantar todo con N clientes simulados
docker compose up --build --scale cliente=N 

# Ver logs del servidor
docker compose logs -f servidor

# Ver logs de los clientes
docker compose logs -f cliente

# Bajar todo
docker compose down
```

## Parámetros del servidor

Se pueden sobreescribir en el `docker-compose.yml` bajo el campo `command`.

| Parámetro | Default | Descripción |
|---|---|---|
| `--host` | `0.0.0.0` | Interfaz donde escuchar |
| `--port-tcp` | `9000` | Puerto TCP para métricas |
| `--port-udp` | `9001` | Puerto UDP para heartbeats |
| `--threshold-cpu` | `80` | Umbral de alerta CPU (%) |
| `--threshold-ram` | `80` | Umbral de alerta RAM (%) |
| `--threshold-disco` | `90` | Umbral de alerta disco (%) |
| `--threshold-temperatura` | `85` | Umbral de alerta temperatura (°C) |
| `--heartbeat-timeout` | `30` | Segundos sin heartbeat para considerar nodo caído |
| `--workers` | `4` | Workers del analizador |
| `--db-metricas` | `db/metricas.db` | Ruta del SQLite de métricas |
| `--db-reportes` | `db/reportes.db` | Ruta del SQLite de reportes |
| `--db-retencion` | `72` | Horas de retención de métricas |
| `--reporte-intervalo` | `24` | Horas entre generación de reportes |
| `--log-level` | `INFO` | Nivel de logging (DEBUG, INFO, WARNING, ERROR) |

## Parámetros del cliente

| Parámetro | Default | Descripción |
|---|---|---|
| `--servidor` | `localhost` | Host del servidor central |
| `--port-tcp` | `9000` | Puerto TCP del servidor |
| `--port-udp` | `9001` | Puerto UDP del servidor |
| `--intervalo` | `10` | Segundos entre envíos de métricas |
| `--intervalo-heartbeat` | `5` | Segundos entre heartbeats UDP |
| `--log-level` | `INFO` | Nivel de logging |

## Alertas

Cuando una métrica supera su umbral, el servidor la imprime en consola y encola una tarea Celery para enviar un email al destinatario configurado en el `.env`.

```
2026-04-21 20:01:13 [WARNING]  ALERTA [CPU] nodo=servidor-1 valor=85.3
2026-04-21 20:01:13 [CRITICAL] ALERTA [RAM] nodo=servidor-1 valor=95.1
2026-04-21 20:01:13 [CRITICAL] ALERTA [NODO_CAIDO] nodo=servidor-2 sin heartbeat por 30s
```

- `WARNING` — el valor supera el umbral configurado
- `CRITICAL` — el valor supera el 90% o el nodo dejó de responder

Si `EMAIL_DESTINATARIO` no está configurado en el `.env`, el envío de emails se desactiva silenciosamente y las alertas solo se loguean en consola.
