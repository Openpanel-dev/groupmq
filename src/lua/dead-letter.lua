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

-- Decrement active counter (job is being dead-lettered, not re-queued)
local activeCountKey = ns .. ":count:active"
redis.call("DECR", activeCountKey)

-- Remove idempotence mapping to allow reuse
redis.call("DEL", ns .. ":unique:" .. jobId)

-- Remove group lock if this job holds it
local lockKey = ns .. ":lock:" .. groupId
local lockValue = redis.call("GET", lockKey)
if lockValue == jobId then
  redis.call("DEL", lockKey)
end

-- Check if group is now empty or should be removed from ready queue
local jobCount = redis.call("ZCARD", gZ)
if jobCount == 0 then
  -- Group is empty, remove from ready queue and clean up
  redis.call("ZREM", readyKey, groupId)
  redis.call("DEL", gZ)
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

