import asyncio
import uuid
import redis.asyncio as aioredis
import logging  # Importar logging
import redis.exceptions

class PartitionAllocator:
    """
    Manages acquiring and refreshing exclusive locks on partitions using Redis.
    Ensures that only one worker instance holds the lock for a given partition
    at any time.
    """
    def __init__(self, redis_url: str, total_partitions: int, ttl_sec: int):
        """
        Initializes the PartitionAllocator.

        Args:
            redis_url: The connection URL for the Redis server.
            total_partitions: The total number of partitions available in the system.
            ttl_sec: The time-to-live (in seconds) for the partition locks.
                     Locks must be refreshed within this time.
        """
        if total_partitions <= 0:
             raise ValueError("total_partitions must be greater than zero")

        self.redis_url = redis_url
        self.total_partitions = total_partitions
        self.ttl_sec = ttl_sec
        self.token = str(uuid.uuid4())  # Unique token for this allocator instance
        self.redis = None
        self._connected = False
        self.logger = logging.getLogger("partition_allocator")
        # ---------------------------------------------------
        self.logger.debug(f"PartitionAllocator initialized. Token: {self.token[:8]}..., Total Partitions: {self.total_partitions}, TTL: {self.ttl_sec}s")

    async def connect(self):
        """
        Establishes connection to the Redis server.
        """
        if self._connected:
            self.logger.debug(f"Allocator (Token: {self.token[:8]}...) already connected.")
            return

        # Usamos self.logger ahora
        self.logger.info(f"Allocator (Token: {self.token[:8]}...): Attempting Redis connection to {self.redis_url}...")
        try:
            self.redis = await aioredis.from_url(self.redis_url, decode_responses=True)
            await self.redis.ping() # Verify connection works
            self._connected = True
            self.logger.info(f"✅ Allocator (Token: {self.token[:8]}...): Redis connection successful.") # <<< LOG DE CONEXIÓN EXITOSA
        except (redis.exceptions.ConnectionError, OSError) as e:
            self.logger.error(f"❌ Allocator (Token: {self.token[:8]}...): Redis connection failed: {e}")
            self.redis = None
            self._connected = False
            raise

    def _get_lock_key(self, partition: int) -> str:
        """
        Generates the Redis key used for the lock of a specific partition.

        Args:
            partition: The partition number.

        Returns:
            The Redis key string.
        """
        return f"qbull:partition_lock:{partition}"

    async def acquire_n(self, count: int, wait_interval_sec: int = 3) -> list[int]:
        """
        Attempts to acquire exclusive locks on 'count' number of available partitions.
        Retries periodically if not enough partitions are immediately available.

        Args:
            count: The number of partitions to acquire.
            wait_interval_sec: Seconds to wait between retry attempts.

        Returns:
            A list of integers representing the acquired partition numbers.

        Raises:
            ConnectionError: If the allocator is not connected to Redis.
            ValueError: If 'count' is greater than 'total_partitions'.
        """
        self.logger.info(f"Allocator (Token: {self.token[:8]}...) attempting to acquire {count} partitions...")
        if not self._connected or not self.redis:
            self.logger.error("Allocator not connected. Cannot acquire partitions.")
            raise ConnectionError("PartitionAllocator not connected.")
        if count > self.total_partitions:
            self.logger.error(f"Cannot acquire {count} partitions, only {self.total_partitions} exist.")
            raise ValueError(f"Cannot acquire {count} partitions, only {self.total_partitions} exist.")
        if count <=0:
             self.logger.warning(f"Requested to acquire {count} partitions. Returning empty list.")
             return []

        acquired_partitions = []
        round_num = 0
        while len(acquired_partitions) < count:
            round_num += 1
            self.logger.info(f"--- Acquisition Round {round_num} (Need {count}, Have {len(acquired_partitions)}) ---")
            acquired_in_round = 0
            # Iterate through all possible partitions to find available ones
            for i in range(self.total_partitions):
                # Skip if already acquired by this instance in a previous round
                if i in acquired_partitions:
                    continue

                key = self._get_lock_key(i)
                # Log con DEBUG para no saturar si hay muchas particiones
                self.logger.debug(f"Round {round_num}: Attempting lock for partition {i} (Key: {key})...")
                try:
                    # Attempt to set the key with NX (Not Exists) and EX (Expire) options
                    # This is atomic in Redis.
                    ok = await self.redis.set(key, self.token, ex=self.ttl_sec, nx=True)

                    if ok:
                        # Successfully acquired the lock for partition i
                        self.logger.info(f"✅ SUCCESS! Acquired partition {i} (TTL: {self.ttl_sec}s) in round {round_num}.")
                        acquired_partitions.append(i)
                        acquired_in_round += 1
                        # Check if we have acquired enough partitions in this round
                        if len(acquired_partitions) == count:
                            self.logger.info(f"Target count ({count}) reached in round {round_num}.")
                            break  # Exit the inner 'for' loop
                    else:
                        # Failed to acquire lock (likely already held by another instance)
                        self.logger.debug(f"Round {round_num}: Failed acquire partition {i} (Key: {key}) - Likely locked.")

                except redis.exceptions.RedisError as e:
                     self.logger.error(f"Round {round_num}: RedisError trying acquire partition {i}: {e}")
                except Exception as e:
                     # Catch any other unexpected errors during acquisition attempt
                     self.logger.exception(f"Round {round_num}: Unexpected error trying acquire partition {i}: {e}")


            # Check if we need to wait and retry after iterating through all partitions
            if len(acquired_partitions) < count:
                self.logger.warning(
                    f"Round {round_num} finished. Still need {count - len(acquired_partitions)} partitions "
                    f"({len(acquired_partitions)}/{count} acquired total). "
                    f"Waiting {wait_interval_sec}s before next round..."
                )
                # Wait before starting the next round
                await asyncio.sleep(wait_interval_sec)

        # Log final confirmation when the while loop exits (all partitions acquired)
        self.logger.info(f"🏆 Successfully acquired all {count} requested partitions: {acquired_partitions}")
        return acquired_partitions

    async def refresh_n(self, partitions: list[int]) -> bool:
        """
        Refreshes the TTL for the locks on the specified partitions.
        This should be called periodically by the worker holding the locks.

        Args:
            partitions: A list of partition numbers whose locks need refreshing.

        Returns:
            True if all refresh commands were sent successfully (or if list was empty),
            False if there was a connection issue or Redis error. Note: This doesn't
            guarantee the keys were actually refreshed if they expired or were
            taken by another process between acquire and refresh.
        """
        if not self._connected or not self.redis:
            self.logger.error("Cannot refresh partition locks: Allocator not connected.")
            return False
        if not partitions:
            self.logger.debug("refresh_n called with empty partition list, nothing to do.")
            return True # Nothing to refresh

        self.logger.info(f"Attempting to refresh locks for partitions: {partitions} (TTL: {self.ttl_sec}s)")
        all_sent_ok = True
        try:
            # Use pipeline for efficiency, though EXPIRE isn't truly atomic in bulk here
            async with self.redis.pipeline(transaction=False) as pipe:
                for p in partitions:
                    if not isinstance(p, int) or p < 0:
                        self.logger.warning(f"Skipping invalid partition number during refresh: {p}")
                        continue
                    key = self._get_lock_key(p)
                    # Refresh TTL using EXPIRE. Assumes we still hold the lock.
                    # We don't check the token here for simplicity, relying on frequent refreshes.
                    # A more robust (but complex) approach might use Lua to check token before expiring.
                    pipe.expire(key, self.ttl_sec)

                results = await pipe.execute()

            # Check results: EXPIRE returns 1 if TTL was set, 0 if key doesn't exist (or possibly other errors)
            failed_refresh = [partitions[i] for i, res in enumerate(results) if not res] # res is 0 or False if failed
            if failed_refresh:
                 # This might happen if the lock expired just before refresh
                 self.logger.warning(f"Refresh command potentially failed (returned 0) for partitions: {failed_refresh}. Lock might have expired.")
                 # We don't set all_sent_ok to False here, as the command itself didn't cause a RedisError
            else:
                 self.logger.info(f"Refresh command sent successfully for partitions: {partitions}")

        except redis.exceptions.RedisError as e:
            self.logger.error(f"RedisError during lock refresh for partitions {partitions}: {e}")
            all_sent_ok = False
        except Exception as e:
             self.logger.exception(f"Unexpected error during lock refresh for partitions {partitions}: {e}")
             all_sent_ok = False

        return all_sent_ok


    async def release_n(self, partitions: list[int]):
        """
        Releases the locks on the specified partitions, but only if they are still
        held by this allocator instance (identified by its unique token).

        Args:
            partitions: A list of partition numbers to release.
        """
        if not self._connected or not self.redis:
            self.logger.warning("Cannot release partition locks: Allocator not connected.")
            return
        if not partitions:
            self.logger.debug("release_n called with empty partition list, nothing to do.")
            return

        self.logger.info(f"Attempting release locks for partitions: {partitions} using token {self.token[:8]}...")
        released_count = 0
        # Lua script ensures atomicity: Check token and delete if it matches.
        release_script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"

        try:
            # Load script SHA only once if possible (though redis-py might cache this)
            # For simplicity, loading it every time here. Caching could be added.
            release_sha = await self.redis.script_load(release_script)

            # Use pipeline to send all release commands efficiently
            async with self.redis.pipeline(transaction=False) as pipe:
                 valid_partitions_to_release = []
                 for p in partitions:
                      if not isinstance(p, int) or p < 0:
                          self.logger.warning(f"Skipping invalid partition number during release: {p}")
                          continue
                      valid_partitions_to_release.append(p)
                      key = self._get_lock_key(p)
                      # Evaluate the Lua script for each partition
                      pipe.evalsha(release_sha, 1, key, self.token)

                 if not valid_partitions_to_release:
                      self.logger.warning("No valid partitions provided to release.")
                      return

                 # Execute all commands in the pipeline
                 results = await pipe.execute()

            # Process results
            for i, result in enumerate(results):
                 partition_num = valid_partitions_to_release[i]
                 if result == 1:
                     # Lua script returned 1, meaning deletion was successful
                     self.logger.info(f"✅ Released partition {partition_num}")
                     released_count += 1
                 else:
                     # Lua script returned 0, meaning key didn't exist or token didn't match
                     self.logger.warning(f"Could not release partition {partition_num} (maybe expired or not owned by token {self.token[:8]}?)")

            self.logger.info(f"Finished release attempt for {len(valid_partitions_to_release)} partitions. Successfully released {released_count}.")

        except redis.exceptions.NoScriptError:
             self.logger.error("LUA script for release not found in Redis cache. Needs reloading.")
             # Handle potential script flush on Redis server (retry loading?)
        except redis.exceptions.RedisError as e:
            self.logger.error(f"RedisError during conditional release for partitions {partitions}: {e}")
        except Exception as e:
             self.logger.exception(f"Unexpected error during conditional release for partitions {partitions}: {e}")


    async def close(self):
        """
        Closes the connection to the Redis server.
        Note: This does NOT automatically release acquired locks. Release should
        be handled explicitly by the caller using release_n before closing.
        """
        self.logger.info(f"Closing Allocator (Token: {self.token[:8]}...) connection...")
        # Explicitly ensure release is called elsewhere if needed.
        # await self.release_n(self.currently_held_partitions) # Example if tracking partitions internally

        if self.redis:
            try:
                await self.redis.close()
                self.logger.info(f"Allocator (Token: {self.token[:8]}...) connection closed.")
            except redis.exceptions.RedisError as e:
                self.logger.error(f"Error closing Allocator connection: {e}")
            except Exception as e:
                 self.logger.exception(f"Unexpected error closing Allocator connection: {e}")
            finally:
                # Ensure state is updated even if close fails
                self.redis = None
                self._connected = False
        else:
            self.logger.info(f"Allocator (Token: {self.token[:8]}...) connection was already closed or not established.")
            self._connected = False # Ensure state consistency