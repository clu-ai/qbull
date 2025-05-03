# example_publisher.py
import asyncio
import logging
import random
import time

# Asume que tu librería está instalada o en el PYTHONPATH como 'qbull'
# Importa las funciones para manejar el publisher global y la configuración
from qbull.qbull_client import connect_publisher, get_publisher, close_publisher
# Podrías importar config si necesitaras PARTITIONS aquí, pero el publisher ya lo tiene
# from qbull import config

# --- Configuración de Logging ---
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("WhatsappPublisher")

# --- Constantes ---
STREAM_NAME = "whatsapp_messages"
PHONE_NUMBERS = [
    "3205104418",
    "3205104419",
    "3205104410",
    "3111111111", # Añadir otros para más variedad
    "3222222222",
]
MESSAGES_TO_SEND = 3 # Cuántos mensajes enviar en total
PUBLISH_INTERVAL_SEC = 0.5 # Tiempo entre envíos

# --- Lógica Principal ---
async def main():
    logger.info("--- Iniciando Publicador de Mensajes Whatsapp ---")
    start_time = time.monotonic()
    published_count = 0

    try:
        # 1. Conectar el publisher global (se hace una sola vez)
        logger.info("Conectando al publisher global...")
        await connect_publisher(
            redis_url_override="redis://localhost:6379",
            total_partitions_override=1
        )
        logger.info("Publisher conectado.")

        # 2. Obtener la instancia del publisher
        publisher = get_publisher()

        # 3. Bucle de publicación
        logger.info(f"Enviando {MESSAGES_TO_SEND} mensajes al stream '{STREAM_NAME}'...")
        for i in range(MESSAGES_TO_SEND):
            # Elegir un número de teléfono al azar como clave de partición
            target_phone = random.choice(PHONE_NUMBERS)

            # Crear el diccionario de datos del trabajo
            job_data = {
                "to": target_phone,
                "message_body": f"Este es el mensaje #{i+1} para {target_phone}",
                "type": random.choice(["text", "image", "audio"]), # Añadir algo de variedad
                "send_timestamp": time.time(),
                # El 'job_id' y 'published_at' se añaden automáticamente por el publisher
            }

            # Publicar el trabajo especificando el stream y la clave de partición
            # La librería calculará la partición correcta basada en target_phone y config.PARTITIONS
            logger.debug(f"Intentando publicar trabajo para {target_phone}...")
            job_id = await publisher.publish_job(
                stream_name=STREAM_NAME,
                data=job_data,
                partition_key=target_phone # Usamos el número como clave explícita
            )

            if job_id:
                published_count += 1
                logger.info(f"-> Publicado Job ID: {job_id} para {target_phone} (Mensaje {i+1}/{MESSAGES_TO_SEND})")
            else:
                logger.error(f"Fallo al publicar mensaje #{i+1} para {target_phone}")

            # Esperar antes de enviar el siguiente
            await asyncio.sleep(PUBLISH_INTERVAL_SEC)

    except RuntimeError as e:
        logger.error(f"Error crítico del publisher: {e}. Asegúrate de llamar a connect_publisher().")
    except Exception as e:
        logger.exception(f"Ocurrió un error inesperado en el publicador: {e}")
    finally:
        # 4. Cerrar la conexión del publisher al finalizar
        logger.info("Cerrando conexión del publisher...")
        await close_publisher()
        end_time = time.monotonic()
        logger.info(f"--- Publicador Finalizado ---")
        logger.info(f"Total de mensajes publicados: {published_count}/{MESSAGES_TO_SEND}")
        logger.info(f"Tiempo total: {end_time - start_time:.2f} segundos")

# --- Punto de Entrada ---
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Publicación interrumpida por el usuario.")