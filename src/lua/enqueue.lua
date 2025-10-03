-- argv: ns, groupId, dataJson, maxAttempts, orderMs, delayUntil, jobId, keepCompleted
local ns = ARGV[1]
local groupId = ARGV[2]
local data = ARGV[3]
local maxAttempts = tonumber(ARGV[4])
local orderMs = tonumber(ARGV[5])
local delayUntil = tonumber(ARGV[6])
local jobId = ARGV[7]
local keepCompleted = tonumber(ARGV[8]) or 0

local readyKey = ns .. ":ready"
local delayedKey = ns .. ":delayed"
local jobKey = ns .. ":job:" .. jobId
local groupsKey = ns .. ":groups"

-- Idempotence: ensure unique jobId per queue namespace with stale-key recovery
local uniqueKey = ns .. ":unique:" .. jobId
local uniqueSet = redis.call("SET", uniqueKey, jobId, "NX")
if not uniqueSet then
  -- Duplicate detected. Check for stale unique mapping
  local exists = redis.call("EXISTS", jobKey)
  if exists == 0 then
    -- Job doesn't exist but unique key does (stale), clean up and proceed
    redis.call("DEL", uniqueKey)
    redis.call("SET", uniqueKey, jobId)
  else
    -- Job exists, check its status and location
    local gid = redis.call("HGET", jobKey, "groupId")
    local inProcessing = redis.call("ZSCORE", ns .. ":processing", jobId)
    local inDelayed = redis.call("ZSCORE", ns .. ":delayed", jobId)
    local inGroup = nil
    if gid then
      inGroup = redis.call("ZSCORE", ns .. ":g:" .. gid, jobId)
    end
    if (not inProcessing) and (not inDelayed) and (not inGroup) then
      if keepCompleted == 0 then
        redis.call("DEL", jobKey)
        redis.call("DEL", uniqueKey)
        redis.call("SET", uniqueKey, jobId)
      else
        -- Job hash exists and we're keeping completed jobs, ensure unique key exists
        redis.call("SET", uniqueKey, jobId)
        return jobId
      end
    else
      if keepCompleted == 0 then
        local status = redis.call("HGET", jobKey, "status")
        if status == "completed" then
          redis.call("DEL", jobKey)
          redis.call("DEL", uniqueKey)
          redis.call("SET", uniqueKey, jobId)
        else
          -- Job is still active, ensure unique key exists
          redis.call("SET", uniqueKey, jobId)
          return jobId
        end
      end
      local activeAgain = redis.call("ZSCORE", ns .. ":processing", jobId)
      local delayedAgain = redis.call("ZSCORE", ns .. ":delayed", jobId)
      local inGroupAgain = nil
      if gid then
        inGroupAgain = redis.call("ZSCORE", ns .. ":g:" .. gid, jobId)
      end
      local jobStillExists = redis.call("EXISTS", jobKey)
      if jobStillExists == 1 and (activeAgain or delayedAgain or inGroupAgain) then
        return jobId
      end
    end
  end
end

local gZ = ns .. ":g:" .. groupId

if not orderMs then
  orderMs = tonumber(redis.call("TIME")[1]) * 1000
end
local baseEpoch = 1704067200000
local relativeMs = orderMs - baseEpoch

-- Use date-based sequence key to auto-reset daily (prevents max int overflow)
local daysSinceEpoch = math.floor(orderMs / 86400000)
local seqKey = ns .. ":seq:" .. daysSinceEpoch
local seq = redis.call("INCR", seqKey)
local score = relativeMs * 1000 + seq

-- Get accurate timestamp with milliseconds from Redis TIME
local timeResult = redis.call("TIME")
local now = tonumber(timeResult[1]) * 1000 + math.floor(tonumber(timeResult[2]) / 1000)

redis.call("HMSET", jobKey,
  "id", jobId,
  "groupId", groupId,
  "data", data,
  "attempts", "0",
  "maxAttempts", tostring(maxAttempts),
  "seq", tostring(seq),
  "timestamp", tostring(now),
  "orderMs", tostring(orderMs),
  "score", tostring(score),
  "delayUntil", tostring(delayUntil)
)

-- Track group membership (idempotent)
redis.call("SADD", groupsKey, groupId)

if delayUntil > 0 and delayUntil > now then
  redis.call("ZADD", delayedKey, delayUntil, jobId)
  redis.call("ZADD", gZ, score, jobId)
  return jobId
else
  redis.call("ZADD", gZ, score, jobId)
  local head = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
  if head and #head >= 2 then
    local headScore = tonumber(head[2])
    redis.call("ZADD", readyKey, headScore, groupId)
  end
  return jobId
end


