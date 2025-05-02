-- Lua script to safely release a distributed lock in Redis.
--
-- Releases (deletes) the lock only if the current token matches the one provided.
-- This prevents a requester from accidentally releasing another's lock.
--
-- KEYS[1]: The Redis key used for the lock.
-- ARGV[1]: The token (unique ID) of the requester attempting to release the lock.
--
-- Returns:
--   1 if the lock was successfully released by ARGV[1].
--   0 if the lock did not exist or belonged to a different token.

local current_token = redis.call('GET', KEYS[1])

if not current_token or current_token ~= ARGV[1] then
    -- The lock does not exist or belongs to another requester.
    return 0 -- Failed, nothing was released or it wasn't ours
end

-- The lock exists and belongs to the requester, delete it.
redis.call('DEL', KEYS[1])
return 1 -- Successfully released