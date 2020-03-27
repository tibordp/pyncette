local version, execute_after, locked_until, locked_by, task_spec = unpack(redis.call('hmget', KEYS[1], 'version', 'execute_after', 'locked_until', 'locked_by', 'task_spec'))
local function getTasksetKey(key, a1, a2)
    if not a1 or a1 < a2 then
    return a2 .. '_' .. key
    else
    return a1 .. '_' .. key
    end
end

local result
if execute_after then
    redis.call('zrem', KEYS[2], getTasksetKey(KEYS[1], locked_until, execute_after))
    redis.call('del', KEYS[1])
    result = "READY"
else
    result = "MISSING"
end

return { result, version, execute_after, locked_until, locked_by, task_spec }