-- argv: ns, status, graceAtMs, limit
local ns = ARGV[1]
local status = ARGV[2]
local graceAt = tonumber(ARGV[3]) or 0
local limit = tonumber(ARGV[4]) or 1000

local setKey = nil
if status == 'completed' then
  setKey = ns .. ':completed'
elseif status == 'failed' then
  setKey = ns .. ':failed'
elseif status == 'delayed' then
  setKey = ns .. ':delayed'
else
  -- unsupported status for clean
  return 0
end

-- Fetch up to 'limit' job ids with score <= graceAt
local ids = redis.call('ZRANGEBYSCORE', setKey, '-inf', graceAt, 'LIMIT', 0, limit)

local removed = 0
for i = 1, #ids do
  local id = ids[i]
  local jobKey = ns .. ':job:' .. id

  -- Remove from the primary set first to avoid reprocessing
  redis.call('ZREM', setKey, id)

  -- Remove from group and update ready queue for ALL statuses
  -- This prevents poisoned groups when completed/failed jobs are cleaned
  local groupId = redis.call('HGET', jobKey, 'groupId')
  if groupId then
    local gZ = ns .. ':g:' .. groupId
    local readyKey = ns .. ':ready'
    redis.call('ZREM', gZ, id)
    local jobCount = redis.call('ZCARD', gZ)
    if jobCount == 0 then
      redis.call('ZREM', readyKey, groupId)
      -- Clean up empty group
      redis.call('DEL', gZ)
      redis.call('SREM', ns .. ':groups', groupId)
    elseif status == 'delayed' then
      -- Only update ready queue score for delayed jobs
      -- (completed/failed jobs shouldn't affect ready queue)
      local head = redis.call('ZRANGE', gZ, 0, 0, 'WITHSCORES')
      if head and #head >= 2 then
        local headScore = tonumber(head[2])
        redis.call('ZADD', readyKey, headScore, groupId)
      end
    end
  end

  -- Delete job hash and idempotence key
  redis.call('DEL', jobKey)
  redis.call('DEL', ns .. ':unique:' .. id)

  removed = removed + 1
end

return removed


