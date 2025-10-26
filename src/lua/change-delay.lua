-- argv: ns, jobId, newDelayUntil, now
local ns = ARGV[1]
local jobId = ARGV[2]
local newDelayUntil = tonumber(ARGV[3])
local now = tonumber(ARGV[4])

local jobKey = ns .. ":job:" .. jobId
local delayedKey = ns .. ":delayed"
local readyKey = ns .. ":ready"

-- Check if job exists
local exists = redis.call("EXISTS", jobKey)
if exists == 0 then
  return 0
end

local groupId = redis.call("HGET", jobKey, "groupId")
if not groupId then
  return 0
end

local gZ = ns .. ":g:" .. groupId

-- Update job's delayUntil field
redis.call("HSET", jobKey, "delayUntil", tostring(newDelayUntil))

-- Check if job is currently in delayed set
local inDelayed = redis.call("ZSCORE", delayedKey, jobId)

if newDelayUntil > 0 and newDelayUntil > now then
  -- Job should be delayed
  redis.call("HSET", jobKey, "status", "delayed")
  if inDelayed then
    -- Update existing delay
    redis.call("ZADD", delayedKey, newDelayUntil, jobId)
  else
    -- Move to delayed
    redis.call("ZADD", delayedKey, newDelayUntil, jobId)
    -- If this is the head job, remove group from ready
    local head = redis.call("ZRANGE", gZ, 0, 0)
    if head and #head > 0 and head[1] == jobId then
      redis.call("ZREM", readyKey, groupId)
    end
  end
else
  -- Job should be ready immediately
  redis.call("HSET", jobKey, "status", "waiting")
  if inDelayed then
    -- Remove from delayed
    redis.call("ZREM", delayedKey, jobId)
    -- If this is the head job, ensure group is in ready
    local head = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
    if head and #head >= 2 and head[1] == jobId then
      local headScore = tonumber(head[2])
      redis.call("ZADD", readyKey, headScore, groupId)
    end
  end
end

return 1


