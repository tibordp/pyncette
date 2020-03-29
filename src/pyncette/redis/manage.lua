local version, execute_after, locked_until, locked_by, task_spec = unpack(redis.call('hmget', KEYS[1], 'version', 'execute_after', 'locked_until', 'locked_by', 'task_spec'))
local key_exists = version ~= false
local result

local function getIndexKey()
    if not locked_until or locked_until < execute_after then
        return execute_after .. '_' .. KEYS[1]
    else
        return locked_until .. '_' .. KEYS[1]
    end
end


if ARGV[1] == 'POLL' then
    local _, mode, task_type, utc_now, incoming_version, incoming_execute_after, incoming_locked_until, incoming_locked_by = unpack(ARGV)

    if not key_exists and task_type == "REGULAR"  then
        version, execute_after = incoming_version, incoming_execute_after
        redis.call('hmset', KEYS[1], 'version', version, 'execute_after', execute_after)
        redis.call('zadd', KEYS[2], 0, getIndexKey())
    end

    if not key_exists and task_type == "DYNAMIC"  then
        result = "MISSING"
    elseif locked_until and utc_now < locked_until and not (version == incoming_version and locked_by == incoming_locked_by) then
        result = "LOCKED"
    elseif execute_after <= utc_now and version ~= incoming_version then
        result = "LEASE_MISMATCH"
    elseif execute_after <= utc_now and mode == 'AT_MOST_ONCE' then
        redis.call('zrem', KEYS[2], getIndexKey())
        version, execute_after, locked_until, locked_by = version + 1, incoming_execute_after, nil, nil
        redis.call('hmset', KEYS[1], 'version', version, 'execute_after', execute_after)
        redis.call('hdel', KEYS[1], 'locked_until', 'locked_by')
        redis.call('zadd', KEYS[2], 0, getIndexKey())
        result = "READY"
    elseif execute_after <= utc_now and mode == 'AT_LEAST_ONCE' then
        redis.call('zrem', KEYS[2], getIndexKey())
        version, locked_until, locked_by = version + 1, incoming_locked_until, incoming_locked_by
        redis.call('hmset', KEYS[1], 'version', version, 'locked_until', locked_until, 'locked_by', locked_by)
        redis.call('zadd', KEYS[2], 0, getIndexKey())
        result = "READY"
    else
        result = "PENDING"
    end
elseif ARGV[1] == 'COMMIT' then
    local _, incoming_version, incoming_locked_by, incoming_execute_after = unpack(ARGV)

    if incoming_execute_after and version == incoming_version and locked_by == incoming_locked_by then
        redis.call('zrem', KEYS[2], getIndexKey())
        version, execute_after, locked_until, locked_by = version + 1, incoming_execute_after, nil, nil
        redis.call('hmset', KEYS[1], 'version', version, 'execute_after', execute_after)
        redis.call('hdel', KEYS[1], 'locked_until', 'locked_by')
        redis.call('zadd', KEYS[2], 0, getIndexKey())
        result = "READY"
    elseif not incoming_execute_after and version == incoming_version and locked_by == incoming_locked_by then
        redis.call('zrem', KEYS[2], getIndexKey())
        version, locked_until, locked_by = version + 1, nil, nil
        redis.call('hset', KEYS[1], 'version', version)
        redis.call('hdel', KEYS[1], 'locked_until', 'locked_by')
        redis.call('zadd', KEYS[2], 0, getIndexKey())
        result = "READY"
    else
        result = "LEASE_MISMATCH"
    end
elseif ARGV[1] == 'REGISTER' then
    local _, incoming_execute_after, incoming_task_spec = unpack(ARGV)

    if not key_exists then
        version = 0
    else
        redis.call('zrem', KEYS[2], getIndexKey())
    end

    version, execute_after, locked_until, locked_by, task_spec = version + 1, incoming_execute_after, nil, nil, incoming_task_spec
    redis.call('hmset', KEYS[1], 'version', version, 'execute_after', execute_after, 'task_spec', task_spec)
    redis.call('hdel', KEYS[1], 'locked_until', 'locked_by')
    redis.call('zadd', KEYS[2], 0, getIndexKey())
    result = "READY"
elseif ARGV[1] == 'UNREGISTER' then
    if key_exists then
        redis.call('zrem', KEYS[2], getIndexKey())
    end

    version, execute_after, locked_until, locked_by, task_spec = nil, nil, nil, nil, nil
    redis.call('del', KEYS[1])
    result = "READY"
end

return { result, version, execute_after, locked_until, locked_by, task_spec}