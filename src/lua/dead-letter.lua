-- argv: ns, jobId, groupId
local ns = ARGV[1]
local jobId = ARGV[2]
local groupId = ARGV[3]
local gZ = ns .. ":g:" .. groupId
local readyKey = ns .. ":ready"

-- Remove job from group
redis.call("ZREM", gZ, jobId)

-- Remove from processing if it's there
redis.call("DEL", ns .. ":processing:" .. jobId)
redis.call("ZREM", ns .. ":processing", jobId)

-- No counter operations - use ZCARD for counts

-- Remove idempotence mapping to allow reuse
redis.call("DEL", ns .. ":unique:" .. jobId)

-- BullMQ-style: Remove from group active list if present
local groupActiveKey = ns .. ":g:" .. groupId .. ":active"
redis.call("LREM", groupActiveKey, 1, jobId)

-- Check if group is now empty or should be removed from ready queue
local jobCount = redis.call("ZCARD", gZ)
if jobCount == 0 then
  -- Group is empty, remove from ready queue and clean up
  redis.call("ZREM", readyKey, groupId)
  redis.call("DEL", gZ)
  redis.call("DEL", groupActiveKey)
  redis.call("SREM", ns .. ":groups", groupId)
else
  -- Group still has jobs, update ready queue with new head
  local head = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
  if head and #head >= 2 then
    local headScore = tonumber(head[2])
    redis.call("ZADD", readyKey, headScore, groupId)
  end
end

-- Optionally store in dead letter queue (uncomment if needed)
-- redis.call("LPUSH", ns .. ":dead", jobId)

return 1

