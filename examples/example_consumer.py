# example_consumer.py
import asyncio
import logging
import random
import signal
import time

# Importar SÓLO la clase Consumer refactorizada
# YA NO importamos 'config'
from qbull.consumer import Consumer

# --- Configuración de Logging ---
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [%(levelname)s] %(name)s (%(process)d): %(message)s")
logger = logging.getLogger("WhatsappConsumer")

# --- Constantes (Definidas directamente en el script) ---
STREAM_NAME = "whatsapp_messages"
REDIS_URL = "redis://localhost:6379" # URL de tu Redis
GROUP_NAME = "qbull_workers"         # Nombre del grupo de consumidores
TOTAL_PARTITIONS = 1                 # Número TOTAL de particiones en el sistema (debe coincidir con el Publisher)
PARTITIONS_TO_ACQUIRE = 1            # Cuántas particiones intentará adquirir ESTA instancia
JOB_LOCK_TTL_MS = 60000              # 60 segundos en milisegundos
PARTITION_LOCK_TTL_SEC = 90          # 90 segundos
PARTITION_REFRESH_INTERVAL_SEC = 30  # Debe ser menor que PARTITION_LOCK_TTL_SEC
READ_BLOCK_MS = 5000                 # 5 segundos

# --- Instancia del Consumidor ---
# Creamos la instancia usando los valores definidos arriba, sin usar 'config'.
logger.info(f"Creando instancia de Consumer con:")
logger.info(f"  Total Partitions: {TOTAL_PARTITIONS}")
logger.info(f"  Partitions to Acquire: {PARTITIONS_TO_ACQUIRE}")
logger.info(f"  Redis URL: {REDIS_URL}")
logger.info(f"  Group Name: {GROUP_NAME}")

consumer = Consumer(
    redis_url=REDIS_URL,
    group_name=GROUP_NAME,
    total_partitions=TOTAL_PARTITIONS,           # <- Valor fijo
    partitions_to_acquire=PARTITIONS_TO_ACQUIRE, # <- Valor fijo
    job_lock_ttl_ms=JOB_LOCK_TTL_MS,             # <- Valor fijo
    partition_lock_ttl_sec=PARTITION_LOCK_TTL_SEC,       # <- Valor fijo
    partition_refresh_interval_sec=PARTITION_REFRESH_INTERVAL_SEC, # <- Valor fijo
    read_block_ms=READ_BLOCK_MS,                 # <- Valor fijo
)

# --- Definición del Handler ---
@consumer.handler(STREAM_NAME)
async def handle_whatsapp_message(job: dict):
    """
    Procesa un trabajo recibido del stream 'whatsapp_messages'.
    """
    job_id = job.get('job_id', 'N/A')
    recipient = job.get('to', 'N/A')
    message_body = job.get('message_body', '')
    published_at_str = job.get('published_at', 'N/A')

    logger.info(f"<- [Job: {job_id}] Recibido para {recipient}. Procesando...")

    try:
        processing_time = random.uniform(0.3, 1.2)
        logger.debug(f"   [Job: {job_id}] Simulando trabajo por {processing_time:.2f}s...")
        await asyncio.sleep(processing_time)

        if random.random() < 0.05:
             raise ValueError("Error simulado al enviar mensaje a la API de Whatsapp!")

        logger.info(f"<- [Job: {job_id}] Procesado exitosamente para {recipient}.")

    except Exception as e:
        logger.error(f"!! [Job: {job_id}] Fallo al procesar para {recipient}: {e}")
        raise

# --- Lógica Principal ---
async def main():
    logger.info("--- Iniciando Consumidor de Mensajes Whatsapp ---")
    # Actualizamos este log para usar las constantes definidas aquí, no 'config'
    logger.info(f"Intentará adquirir {PARTITIONS_TO_ACQUIRE} particiones de {TOTAL_PARTITIONS} totales.")
    logger.info(f"Usando grupo: {GROUP_NAME}")
    logger.info(f"Conectando a Redis: {REDIS_URL}")
    logger.info(f"Handler registrado para el stream: '{STREAM_NAME}'")
    logger.info("Presiona Ctrl+C para detener.")

    await consumer.run()

    logger.info("--- Consumidor Detenido Limpiamente ---")

# --- Punto de Entrada ---
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Cierre solicitado por KeyboardInterrupt externo.")
    except Exception as e:
         logger.exception("Error fatal en el nivel principal del consumidor.")