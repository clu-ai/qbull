from functools import lru_cache
import logging
# Importar xxhash directamente, fallará la instalación si no está
from xxhash import xxh64

logger = logging.getLogger(__name__)
logger.info("Using xxhash (required dependency) for partitioning.")

# Ya no necesitamos _md5hash ni la lógica condicional para hash_fn

@lru_cache()
def _fasthash(x):
    # repr(x) para manejar diferentes tipos de forma consistente
    # encode() para obtener bytes, necesario para xxhash
    return xxh64(repr(x).encode("utf-8")).intdigest()

# Asignar directamente la función hash rápida
hash_fn = _fasthash

def get_partition(key: str | int | None, partitions: int) -> int:
    """
    Calculates the target partition for a given key using xxhash.

    Args:
        key: The key to partition on (will be converted to string).
        partitions: The total number of available partitions.

    Returns:
        The calculated partition number (integer from 0 to partitions-1).
    """
    if not isinstance(partitions, int) or partitions <= 0:
         # Si el número de particiones no es válido, devuelve 0 o lanza error
         # Devolver 0 es más simple pero puede ocultar problemas.
         logger.warning(f"Invalid number of partitions ({partitions}), defaulting to partition 0.")
         return 0

    # Asegurar que la clave sea string para hash consistente, manejar None
    str_key = str(key) if key is not None else ""

    # Calcula el hash y aplica módulo para obtener la partición
    return hash_fn(str_key) % partitions