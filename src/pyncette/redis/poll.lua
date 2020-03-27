local mode, utc_now, incoming_version, incoming_execute_after, incoming_locked_until, incoming_locked_by = unpack(ARGV)
local version, execute_after, locked_until, locked_by, task_spec = unpack(redis.call('hmget', KEYS[1], 'version', 'execute_after', 'locked_until', 'locked_by', 'task_spec'))

local function getTasksetKey(key, a1, a2)
    if not a1 or a1 < a2 then
    return a2 .. '_' .. key
    else
    return a1 .. '_' .. key
    end
end

if not version then
    version, execute_after = incoming_version, incoming_execute_after
    redis.call('hmset', KEYS[1], 'version', version, 'execute_after', execute_after)
    redis.call('zadd', KEYS[2], 0, getTasksetKey(KEYS[1], locked_until, execute_after))
end

local result

if locked_until and utc_now < locked_until and not (version == incoming_version and locked_by == incoming_locked_by) then
    result = "LOCKED"
elseif execute_after < utc_now and version ~= incoming_version then
    result = "LEASE_MISMATCH"
elseif execute_after < utc_now and mode == 'AT_MOST_ONCE' then
    redis.call('zrem', KEYS[2], getTasksetKey(KEYS[1], locked_until, execute_after))
    version, execute_after, locked_until, locked_by = version + 1, incoming_execute_after, nil, nil
    redis.call('hmset', KEYS[1], 'version', version, 'execute_after', execute_after)
    redis.call('hdel', KEYS[1], 'locked_until', 'locked_by')
    redis.call('zadd', KEYS[2], 0, getTasksetKey(KEYS[1], locked_until, execute_after))
    result = "READY"
elseif execute_after < utc_now and mode == 'AT_LEAST_ONCE' then
    redis.call('zrem', KEYS[2], getTasksetKey(KEYS[1], locked_until, execute_after))
    version, locked_until, locked_by = version + 1, incoming_locked_until, incoming_locked_by
    redis.call('hmset', KEYS[1], 'version', version, 'locked_until', locked_until, 'locked_by', locked_by)
    redis.call('zadd', KEYS[2], 0, getTasksetKey(KEYS[1], locked_until, execute_after))
    result = "READY"
else
    result = "PENDING"
end

return { result, version, execute_after, locked_until, locked_by, task_spec}