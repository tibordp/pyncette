local incoming_version, incoming_locked_by, incoming_execute_after = unpack(ARGV)
local version, execute_after, locked_until, locked_by, task_spec  = unpack(redis.call('hmget', KEYS[1], 'version', 'execute_after', 'locked_until', 'locked_by', 'task_spec'))
local taskset_suffix = '_' .. KEYS[1]

local function getTasksetKey(key, a1, a2)
    if not a1 or a1 < a2 then
    return a2 .. '_' .. key
    else
    return a1 .. '_' .. key
    end
end

local result
if incoming_execute_after and version == incoming_version and locked_by == incoming_locked_by then
    redis.call('zrem', KEYS[2], getTasksetKey(KEYS[1], locked_until, execute_after))
    version, execute_after, locked_until, locked_by = version + 1, incoming_execute_after, nil, nil
    redis.call('hmset', KEYS[1], 'version', version, 'execute_after', execute_after)
    redis.call('hdel', KEYS[1], 'locked_until', 'locked_by')
    redis.call('zadd', KEYS[2], 0, getTasksetKey(KEYS[1], locked_until, execute_after))
    result = "READY"
elseif not incoming_execute_after and version == incoming_version and locked_by == incoming_locked_by then
    redis.call('zrem', KEYS[2], getTasksetKey(KEYS[1], locked_until, execute_after))
    version, locked_until, locked_by = version + 1, nil, nil
    redis.call('hset', KEYS[1], 'version', version)
    redis.call('hdel', KEYS[1], 'locked_until', 'locked_by')
    redis.call('zadd', KEYS[2], 0, getTasksetKey(KEYS[1], locked_until, execute_after))
    result = "READY"
else
    result = "LEASE_MISMATCH"
end

return { result, version, execute_after, locked_until, locked_by, task_spec }