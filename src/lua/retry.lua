-- argv: ns, jobId, backoffMs
local ns = ARGV[1]
local jobId = ARGV[2]
local backoffMs = tonumber(ARGV[3]) or 0

local jobKey = ns .. ":job:" .. jobId
local gid = redis.call("HGET", jobKey, "groupId")
local attempts = tonumber(redis.call("HINCRBY", jobKey, "attempts", 1))
local maxAttempts = tonumber(redis.call("HGET", jobKey, "maxAttempts"))

redis.call("DEL", ns .. ":processing:" .. jobId)
redis.call("ZREM", ns .. ":processing", jobId)

-- No counter operations - use ZCARD for counts

-- BullMQ-style: Remove from group active list
local groupActiveKey = ns .. ":g:" .. gid .. ":active"
redis.call("LREM", groupActiveKey, 1, jobId)

if attempts > maxAttempts then
  return -1
end

local score = tonumber(redis.call("HGET", jobKey, "score"))
local gZ = ns .. ":g:" .. gid

-- Re-add job to group
redis.call("ZADD", gZ, score, jobId)

-- If backoffMs > 0, delay the retry
if backoffMs > 0 then
  local now = tonumber(redis.call("TIME")[1]) * 1000
  local delayUntil = now + backoffMs
  
  -- Move to delayed set
  local delayedKey = ns .. ":delayed"
  redis.call("ZADD", delayedKey, delayUntil, jobId)
  redis.call("HSET", jobKey, "runAt", tostring(delayUntil), "status", "delayed")
  
  -- Don't add to ready yet - will be added when promoted
  -- (delayed jobs block their group)
else
  -- No backoff - immediate retry
  redis.call("HSET", jobKey, "status", "waiting")
  
  -- Add group to ready queue
  local head = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
  if head and #head >= 2 then
    local headScore = tonumber(head[2])
    local readyKey = ns .. ":ready"
    redis.call("ZADD", readyKey, headScore, gid)
  end
end

return attempts
