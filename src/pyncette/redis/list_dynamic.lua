-- List all dynamic task instances for a given parent task
-- KEYS[1]: index key (sorted set containing all task instances)
-- ARGV[1]: limit (number of tasks to return)
-- ARGV[2]: cursor (for pagination, "0" to start, or offset as string)

local limit = tonumber(ARGV[1])
local offset = tonumber(ARGV[2])

-- Use ZRANGE with offset-based pagination
-- Get limit+1 items to check if there are more
local task_keys = redis.call('zrange', KEYS[1], offset, offset + limit)

-- Determine next cursor
local next_cursor = "0"
if #task_keys > limit then
    next_cursor = tostring(offset + limit)
    -- Remove the extra item we fetched
    table.remove(task_keys, #task_keys)
end

local results = {next_cursor}

for _, task_key_with_prefix in ipairs(task_keys) do
    local task_name = task_key_with_prefix:gmatch('_(.*)')()
    local version, execute_after, locked_until, locked_by, task_spec = unpack(redis.call('hmget', task_name, 'version', 'execute_after', 'locked_until', 'locked_by', 'task_spec'))

    if version then
        table.insert(results, {task_name, execute_after, locked_until, locked_by, task_spec})
    end
end

return results
