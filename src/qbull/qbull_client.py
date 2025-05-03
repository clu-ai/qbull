import asyncio
import logging
from .publisher import Publisher # Import the modified Publisher class
from .config import REDIS_URL, PARTITIONS # Import necessary config

logger = logging.getLogger(__name__)

# Global instance variable
_publisher_instance = None
_publisher_lock = asyncio.Lock()

async def connect_publisher(
    redis_url_override: str | None = None,
    total_partitions_override: int | None = None
):
    """
    Crea (si no existe) y conecta la instancia global del publisher.
    Permite sobrescribir la configuración leída desde el módulo 'config'.

    Args:
        redis_url_override: Si se proporciona, usa esta URL de Redis. Si no, usa config.REDIS_URL.
        total_partitions_override: Si se proporciona, usa este número total de particiones.
                                     Si no, usa config.PARTITIONS.
    """
    global _publisher_instance
    async with _publisher_lock:
        if _publisher_instance is None:
            logger.info("Creating and connecting global publisher instance...")
            try:
                _publisher_instance = Publisher(
                    redis_url=redis_url_override,
                    total_partitions=total_partitions_override
                )
                await _publisher_instance.connect()
                logger.info("Global publisher instance connected.")
            except Exception as e:
                logger.exception("Failed to connect global publisher instance.")
                _publisher_instance = None # Reset on failure
                raise # Re-raise the exception
        elif not _publisher_instance._connected:
             # Instance exists but is not connected (e.g., after connection drop)
             logger.warning("Global publisher instance exists but is not connected. Attempting reconnect...")
             try:
                  await _publisher_instance.connect()
                  logger.info("Global publisher instance reconnected.")
             except Exception as e:
                  logger.exception("Failed to reconnect global publisher instance.")
                  # Keep instance object, but it remains disconnected
                  raise


def get_publisher() -> Publisher:
    """Returns the connected global publisher instance."""
    if _publisher_instance is None or not _publisher_instance._connected:
        # Raise an error or log warning if accessed before connect_publisher is successfully called
        # For simplicity, raising error is cleaner. User must call connect_publisher first.
        raise RuntimeError("Publisher instance is not available or not connected. Call connect_publisher() first during application startup.")
    return _publisher_instance

async def close_publisher():
     """Closes the global publisher instance connection."""
     global _publisher_instance
     async with _publisher_lock:
          if _publisher_instance and _publisher_instance._connected:
               logger.info("Closing global publisher instance...")
               await _publisher_instance.close()
               # Optionally set _publisher_instance = None here if you want it fully reset
               logger.info("Global publisher instance closed.")
          elif _publisher_instance:
               logger.info("Global publisher instance exists but was not connected.")
          else:
               logger.info("Global publisher instance does not exist.")