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

local function setKey(attr, val)
    if val == false then
        redis.call('hdel', KEYS[1], attr)
    else
        redis.call('hset', KEYS[1], attr, val)
    end
end

-- Update the task data while also updating the index
local function updateRecord(new_execute_after, new_locked_until, new_locked_by, new_task_spec)
    redis.call('zrem', KEYS[2], getIndexKey())
    version, execute_after, locked_until, locked_by, task_spec = version + 1, new_execute_after, new_locked_until, new_locked_by, new_task_spec
    setKey('version', version)
    setKey('execute_after', execute_after)
    setKey('locked_until', locked_until)
    setKey('locked_by', locked_by)
    setKey('task_spec', task_spec)
    redis.call('zadd', KEYS[2], 0, getIndexKey())
end

local function deleteRecord()
    redis.call('zrem', KEYS[2], getIndexKey())
    version, execute_after, locked_until, locked_by, task_spec = false, false, false, false, false
    redis.call('del', KEYS[1])
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
    elseif execute_after <= utc_now and mode == 'AT_MOST_ONCE' and incoming_execute_after == '' then
        deleteRecord()
        result = "READY"
    elseif execute_after <= utc_now and mode == 'AT_MOST_ONCE' then
        updateRecord(incoming_execute_after, false, false, task_spec)
        result = "READY"
    elseif execute_after <= utc_now and mode == 'AT_LEAST_ONCE' then
        updateRecord(execute_after, incoming_locked_until, incoming_locked_by, task_spec)
        result = "READY"
    else
        result = "PENDING"
    end
elseif ARGV[1] == 'COMMIT' then
    local _, incoming_version, incoming_locked_by, incoming_execute_after = unpack(ARGV)

    if not (version == incoming_version and locked_by == incoming_locked_by) then
        result = "LEASE_MISMATCH"
    elseif incoming_execute_after == '' then
        deleteRecord()
        result = "READY"
    else
        updateRecord(incoming_execute_after, false, false, task_spec)
        result = "READY"
    end
elseif ARGV[1] == 'UNLOCK' then
    local _, incoming_version, incoming_locked_by = unpack(ARGV)

    if version == incoming_version and locked_by == incoming_locked_by then
        updateRecord(execute_after, false, false, task_spec)
        result = "READY"
    else
        result = "LEASE_MISMATCH"
    end
elseif ARGV[1] == 'REGISTER' then
    local _, incoming_execute_after, incoming_task_spec = unpack(ARGV)

    if not key_exists then
        version, execute_after, task_spec = 0, incoming_execute_after, incoming_task_spec
        redis.call('hmset', KEYS[1], 'version', version, 'execute_after', execute_after, 'task_spec', task_spec)
        redis.call('zadd', KEYS[2], 0, getIndexKey())
    else
        updateRecord(incoming_execute_after, false, false, incoming_task_spec)
    end

    result = "READY"
elseif ARGV[1] == 'UNREGISTER' then
    if key_exists then
        deleteRecord()
    end

    result = "READY"
end

return { result, version, execute_after, locked_until, locked_by, task_spec}