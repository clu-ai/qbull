from functools import lru_cache
from hashlib import md5
import logging

try:
    from xxhash import xxh64

    logging.info("Using xxhash for partitioning.")
except ImportError:
    xxh64 = None
    logging.info("xxhash not found, using md5 for partitioning.")


@lru_cache()
def _md5hash(x):
    return int(md5(repr(x).encode("utf-8")).hexdigest(), 16)


@lru_cache()
def _fasthash(x):
    if xxh64 is None:
        raise RuntimeError(
            "xxhash function called but library not available."
        )  # Should not happen due to hash_fn logic
    return xxh64(repr(x)).intdigest()


hash_fn = _fasthash if xxh64 else _md5hash


def get_partition(key: str, partitions: int) -> int:
    if partitions <= 0:
        return 0
    # Ensure key is string for consistent hashing
    str_key = str(key) if key is not None else ""
    return hash_fn(str_key) % partitions
