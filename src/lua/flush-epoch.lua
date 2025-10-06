-- Flush epoch to group ready queue (v2 - expert's design)
-- KEYS: arrivalsZ, byOrderZ, readyL, epochEndK, stateWaitingZ, dueEpochsZ
-- ARGV: gid, nowMs
-- Returns: number of jobs flushed

local gid = ARGV[1]
local nowMs = tonumber(ARGV[2])

-- Get all jobs in the arrivals set (since epoch is due, flush all jobs)
local jobIds = redis.call('ZRANGE', KEYS[1], 0, -1)
if #jobIds == 0 then
  -- Clean up epoch end key if it exists
  redis.call('DEL', KEYS[4])
  return 0
end

-- Sort jobs by orderMs (using byOrder ZSET)
local orderList = redis.call('ZRANGE', KEYS[2], 0, -1)
local toPush = {}
local set = {}

-- Create a set of job IDs in this epoch
for _, id in ipairs(jobIds) do
  set[id] = true
end

-- Add jobs in orderMs order
for _, id in ipairs(orderList) do
  if set[id] then
    table.insert(toPush, id)
  end
end

-- Push sorted jobs to ready queue (as job IDs, not keys)
if #toPush > 0 then
  local jobKeys = {}
  for _, id in ipairs(toPush) do
    local jobKey = 'job:' .. gid .. ':' .. id
    table.insert(jobKeys, jobKey)
    -- Add to waiting state when epoch is flushed
    redis.call('ZADD', KEYS[5], nowMs, jobKey)
  end
  -- Push job IDs (not keys) to group ready queue
  redis.call('RPUSH', KEYS[3], unpack(toPush))
  
  -- Clean up epoch data
  redis.call('ZREM', KEYS[1], unpack(toPush))
  redis.call('ZREM', KEYS[2], unpack(toPush))
  redis.call('DEL', KEYS[4])
  -- Remove from due epochs ZSET
  redis.call('ZREM', KEYS[6], gid)
end

return #toPush
