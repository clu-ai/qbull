from functools import lru_cache # Import Least Recently Used cache decorator
import logging
# Import xxhash directly; installation will fail if not present due to pyproject.toml dependency
from xxhash import xxh64

# Get a logger specific to this module
logger = logging.getLogger(__name__)
# Log confirmation that the required xxhash is being used
logger.info("Using xxhash (required dependency) for partitioning.")

# No longer need _md5hash or conditional logic for hash_fn

@lru_cache() # Cache results for frequently used keys to avoid re-computation
def _fasthash(x) -> int:
    """Calculates a 64-bit integer hash using xxhash."""
    # Use repr(x) to handle different input types (int, str, etc.) consistently.
    # Use encode('utf-8') to get bytes, which xxhash requires.
    return xxh64(repr(x).encode("utf-8")).intdigest()

# Directly assign the fast hash function (xxh64)
# This is the function that will be used by get_partition
hash_fn = _fasthash

def get_partition(key: str | int | None, partitions: int) -> int:
    """
    Calculates the target partition number for a given key using the configured hash function (xxhash).

    Args:
        key: The key to partition on. It will be converted to its string
             representation for consistent hashing. Can be str, int, or None.
        partitions: The total number of available partitions. Must be a positive integer.

    Returns:
        The calculated partition number (an integer between 0 and partitions-1).
        Returns 0 if the number of partitions is invalid.
    """
    # Input validation for the number of partitions
    if not isinstance(partitions, int) or partitions <= 0:
         # Log a warning and return 0 if the partition count isn't valid.
         # Returning 0 might hide configuration errors, raising an error could be an alternative.
         logger.warning(f"Invalid number of partitions specified ({partitions}), defaulting to partition 0.")
         return 0

    # Ensure the key is consistently represented as a string for hashing.
    # Handle None keys by converting them to an empty string.
    str_key = str(key) if key is not None else ""

    # Calculate the hash of the string key and use the modulo operator (%)
    # to map the hash value to a valid partition index (0 to partitions-1).
    return hash_fn(str_key) % partitions