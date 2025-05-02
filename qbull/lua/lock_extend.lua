-- Lua script to acquire or extend a distributed lock in Redis.
--
-- Attempts to acquire the lock if it doesn't exist or has expired.
-- If the lock exists and belongs to the same token (ID), extends its TTL.
--
-- KEYS[1]: The Redis key used for the lock.
-- ARGV[1]: The token (unique ID) of the requester attempting to acquire/extend.
-- ARGV[2]: The new TTL (time to live) in milliseconds (PX).
--
-- Returns:
--   1 if the lock was successfully acquired or extended by ARGV[1].
--   0 if the lock belongs to another token.

local current_token = redis.call('GET', KEYS[1])

if not current_token then
    -- The lock does not exist or has expired, attempt to acquire it.
    redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
    return 1 -- Successfully acquired
end

if current_token ~= ARGV[1] then
    -- The lock exists but belongs to another requester.
    return 0 -- Failed, not acquired
end

-- The lock exists and belongs to the current requester, extend the TTL.
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1 -- Successfully extended