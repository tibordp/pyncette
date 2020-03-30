local utc_now, limit, incoming_locked_until, incoming_locked_by = unpack(ARGV)
limit = tonumber(limit)

local tasksets = redis.call('zrangebylex', KEYS[1], '-', '(' .. utc_now .. '`', 'LIMIT', 0, limit + 1)
local results = { "READY" }

for key,value in pairs(tasksets) do
    local task_name = value:gmatch('_(.*)')()
    local version, execute_after, locked_until, locked_by, task_spec = unpack(redis.call('hmget', task_name, 'version', 'execute_after', 'locked_until', 'locked_by', 'task_spec'))

    local function getIndexKey()
        if not locked_until or locked_until < execute_after then
            return execute_after .. '_' .. task_name
        else
            return locked_until .. '_' .. task_name
        end
    end

    redis.call('zrem', KEYS[1], getIndexKey())
    version, locked_until, locked_by = version + 1, incoming_locked_until, incoming_locked_by
    redis.call('hmset', task_name, 'version', version, 'locked_until', locked_until, 'locked_by', locked_by)
    redis.call('zadd', KEYS[1], 0, getIndexKey())

    results[key + 1] = { "READY", version, execute_after, locked_until, locked_by, task_spec }
    if key == limit then
        results[1] = "HAS_MORE"
        break
    end
end

return results