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

-- Handle retention based on keepCompleted
if keepCompleted > 0 then
  -- Add to completed set and handle retention
  redis.call("HSET", jobKey, "status", "completed", "finishedOn", tostring(nowMs))
  redis.call("ZADD", completedKey, nowMs, jobId)
  
  -- Trim old entries if we exceed the limit
  local zcount = redis.call("ZCARD", completedKey)
  local toRemove = zcount - keepCompleted
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
else
  -- keepCompleted == 0: Delete job immediately, don't add to completed set
  redis.call("DEL", jobKey)
  redis.call("DEL", ns .. ":unique:" .. jobId)
end
local lockKey = ns .. ":lock:" .. gid
local val = redis.call("GET", lockKey)
if val == jobId then
  redis.call("DEL", lockKey)
  
  -- Check if there are more jobs in this group
  local gZ = ns .. ":g:" .. gid
  local jobCount = redis.call("ZCARD", gZ)
  if jobCount == 0 then
    -- Remove empty group zset and from groups tracking set
    redis.call("DEL", gZ)
    redis.call("SREM", ns .. ":groups", gid)
  else
    -- Re-add group to ready set if there are more jobs
    local nextHead = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
    if nextHead and #nextHead >= 2 then
      local nextScore = tonumber(nextHead[2])
      local readyKey = ns .. ":ready"
      redis.call("ZADD", readyKey, nextScore, gid)
    end
  end
  
  return 1
end
return 0


