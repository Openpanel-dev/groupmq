-- argv: ns, jobId
local ns = KEYS[1]
local jobId = ARGV[1]

local jobKey = ns .. ":job:" .. jobId
local delayedKey = ns .. ":delayed"
local readyKey = ns .. ":ready"
local processingKey = ns .. ":processing"

-- If job does not exist, return 0
if redis.call("EXISTS", jobKey) == 0 then
  return 0
end

local groupId = redis.call("HGET", jobKey, "groupId")

-- Remove from delayed and processing structures
redis.call("ZREM", delayedKey, jobId)
redis.call("DEL", ns .. ":processing:" .. jobId)
redis.call("ZREM", processingKey, jobId)

-- Remove from completed/failed retention sets if present
redis.call("ZREM", ns .. ":completed", jobId)
redis.call("ZREM", ns .. ":failed", jobId)

-- Delete idempotence mapping
redis.call("DEL", ns .. ":unique:" .. jobId)

-- If we have a group, update group zset and ready queue accordingly
if groupId then
  local gZ = ns .. ":g:" .. groupId
  redis.call("ZREM", gZ, jobId)

  local jobCount = redis.call("ZCARD", gZ)
  if jobCount == 0 then
    redis.call("ZREM", readyKey, groupId)
    -- Clean up empty group
    redis.call("DEL", gZ)
    redis.call("SREM", ns .. ":groups", groupId)
  else
    local head = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
    if head and #head >= 2 then
      local headScore = tonumber(head[2])
      redis.call("ZADD", readyKey, headScore, groupId)
    end
  end
end

-- Finally, delete the job hash
redis.call("DEL", jobKey)

return 1


