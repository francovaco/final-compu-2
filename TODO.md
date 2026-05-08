# Mejoras futuras

## Funcionalidades nuevas

- **Dashboard web** — interfaz visual en tiempo real para monitorear métricas, alertas y estado de los nodos (ej. Flask + Chart.js)
- **Configuración via interfaz web** — panel para modificar umbrales, intervalos y credenciales de email sin necesidad de reiniciar el servidor ni editar archivos
- **Configuración de email por argparse** — poder pasar `EMAIL_REMITENTE`, `EMAIL_PASSWORD` y `EMAIL_DESTINATARIO` como argumentos al iniciar el servidor, sin depender del `.env`
- **Métricas reales con psutil** — reemplazar los valores random del agente por métricas reales del SO del contenedor
- **Más métricas** — incorporar uso de red, uptime del sistema y cantidad de procesos corriendo
- **API REST** — endpoint para consultar métricas y reportes almacenados en SQLite desde herramientas externas

## Mejoras al sistema de alertas

- **Umbrales configurables por nodo** — actualmente los umbrales son globales para todos los nodos
- **Reportes para nodos caídos** — actualmente `generar_reporte` solo procesa nodos activos en el dict de heartbeats

## Seguridad

- **Autenticación de agentes** — actualmente cualquier cliente puede conectarse al servidor sin validación
