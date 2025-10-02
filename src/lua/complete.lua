-- argv: ns, jobId, groupId, keepCompleted
local ns = ARGV[1]
local jobId = ARGV[2]
local gid = ARGV[3]
local keepCompleted = tonumber(ARGV[4]) or 0

redis.call("DEL", ns .. ":processing:" .. jobId)
redis.call("ZREM", ns .. ":processing", jobId)
-- Mark job as completed and add to completed set for retention/cleanup to work even if the
-- process dies before higher-level bookkeeping runs
local jobKey = ns .. ":job:" .. jobId
local completedKey = ns .. ":completed"
local nowMs = tonumber(redis.call("TIME")[1]) * 1000

-- If keepCompleted retention is configured, check count BEFORE adding this job
local toRemove = 0
if keepCompleted >= 0 then
  local zcount = redis.call("ZCARD", completedKey)
  toRemove = zcount - keepCompleted + 1  -- +1 because we're about to add this job
end

-- Mark job as completed and add to completed set
redis.call("HSET", jobKey, "status", "completed", "finishedOn", tostring(nowMs))
redis.call("ZADD", completedKey, nowMs, jobId)

-- Trim old entries if we exceed the limit
if toRemove > 0 then
  local oldIds = redis.call("ZRANGE", completedKey, 0, toRemove - 1)
  if #oldIds > 0 then
    redis.call("ZREMRANGEBYRANK", completedKey, 0, toRemove - 1)
    for i = 1, #oldIds do
      local oldId = oldIds[i]
      redis.call("DEL", ns .. ":job:" .. oldId)
      redis.call("DEL", ns .. ":unique:" .. oldId)
    end
  end
end
-- Note: unique keys are only deleted when job hashes are trimmed above
local lockKey = ns .. ":lock:" .. gid
local val = redis.call("GET", lockKey)
if val == jobId then
  redis.call("DEL", lockKey)
  
  -- Clean up empty groups
  local gZ = ns .. ":g:" .. gid
  local jobCount = redis.call("ZCARD", gZ)
  if jobCount == 0 then
    -- Remove empty group zset and from groups tracking set
    redis.call("DEL", gZ)
    redis.call("SREM", ns .. ":groups", gid)
  end
  
  return 1
end
return 0


