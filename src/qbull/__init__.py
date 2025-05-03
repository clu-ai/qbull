# src/qbull/__init__.py

import logging # Import standard logging library

# --- Import relative modules to expose their classes/functions ---
# Use '.' for relative imports within the same package
from .publisher import Publisher, Consumer
from .partition_allocator import PartitionAllocator
from .partitioner import get_partition, hash_fn # Expose helper functions too
from .lock import RedisLock

# --- Define __all__ (Optional but good practice) ---
# This list defines what symbols are imported when a user does 'from qbull import *'.
# It also helps static analysis tools understand the public API.
__all__ = [
    "Publisher",
    "Consumer",
    "PartitionAllocator",
    "get_partition",
    "hash_fn",
    "RedisLock",
]

# --- Define Package Version ---
# It's standard practice to define the version here.
# This should match the version in your pyproject.toml
__version__ = "0.1.0" # Or your current version

# --- Configure Library Logging (Null Handler) ---
# Add a default NullHandler to the library's root logger.
# This prevents "No handler found" warnings if the application using
# the library doesn't configure logging itself. The application's logging
# configuration will override this if set up properly.
logging.getLogger(__name__).addHandler(logging.NullHandler())

# Optional: Log when the package is initialized (can be noisy)
# logging.getLogger(__name__).info(f"qbull package version {__version__} initialized.")