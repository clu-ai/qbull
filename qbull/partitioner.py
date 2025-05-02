from functools import lru_cache
from hashlib import md5

try:
    from xxhash import xxh64
except ImportError:
    xxh64 = None


@lru_cache()
def md5hash(x):
    return int(md5(repr(x).encode("utf-8")).hexdigest(), 16)


@lru_cache()
def fasthash(x):
    return xxh64(repr(x)).intdigest()


hash_fn = fasthash if xxh64 else md5hash


def get_partition(key: str, partitions: int) -> int:
    return hash_fn(key) % partitions