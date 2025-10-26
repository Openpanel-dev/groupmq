-- argv: ns, groupId, dataJson, maxAttempts, orderMs, delayUntil, jobId, keepCompleted, clientTimestamp, orderingDelayMs
local ns = ARGV[1]
local groupId = ARGV[2]
local data = ARGV[3]
local maxAttempts = tonumber(ARGV[4])
local orderMs = tonumber(ARGV[5])
local delayUntil = tonumber(ARGV[6])
local jobId = ARGV[7]
local keepCompleted = tonumber(ARGV[8]) or 0
local clientTimestamp = tonumber(ARGV[9])
local orderingDelayMs = tonumber(ARGV[10]) or 0

local readyKey = ns .. ":ready"
local delayedKey = ns .. ":delayed"
local stageKey = ns .. ":stage"
local timerKey = ns .. ":stage:timer"
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

-- Get Redis server time for buffering logic (to be consistent with server time)
local timeResult = redis.call("TIME")
local now = tonumber(timeResult[1]) * 1000 + math.floor(tonumber(timeResult[2]) / 1000)

-- Use client timestamp for the job hash so timing calculations are accurate from client perspective
local timestamp = clientTimestamp or now

redis.call("HMSET", jobKey,
  "id", jobId,
  "groupId", groupId,
  "data", data,
  "attempts", "0",
  "maxAttempts", tostring(maxAttempts),
  "seq", tostring(seq),
  "timestamp", tostring(timestamp),
  "orderMs", tostring(orderMs),
  "score", tostring(score),
  "delayUntil", tostring(delayUntil)
)

-- Track group membership (idempotent)
redis.call("SADD", groupsKey, groupId)

-- Determine job status and placement
local jobStatus = "waiting"

if delayUntil > 0 and delayUntil > now then
  -- Job is delayed, add to delayed set and group set
  redis.call("ZADD", gZ, score, jobId)
  redis.call("ZADD", delayedKey, delayUntil, jobId)
  jobStatus = "delayed"
  redis.call("HSET", jobKey, "status", jobStatus)
elseif orderMs and orderingDelayMs > 0 then
  -- Job should be staged for ordering (orderMs provided and orderingDelayMs > 0)
  -- NOTE: Do NOT add to group ZSET yet - only to staging
  local releaseAt = orderMs + orderingDelayMs
  redis.call("ZADD", stageKey, releaseAt, jobId)
  jobStatus = "staged"
  redis.call("HSET", jobKey, "status", jobStatus)
  
  -- Update/set timer to earliest staged job
  local currentHead = redis.call("ZRANGE", stageKey, 0, 0, "WITHSCORES")
  if currentHead and #currentHead >= 2 then
    local headReleaseAt = tonumber(currentHead[2])
    -- Set timer to expire when the earliest job is ready
    local ttlMs = math.max(1, headReleaseAt - now)
    redis.call("SET", timerKey, "1", "PX", ttlMs)
  end
else
  -- Job is not delayed and not staged, add to group set and make ready
  redis.call("ZADD", gZ, score, jobId)
  jobStatus = "waiting"
  redis.call("HSET", jobKey, "status", jobStatus)
  local head = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
  if head and #head >= 2 then
    local headScore = tonumber(head[2])
    redis.call("ZADD", readyKey, headScore, groupId)
  end
end

-- Return job data to avoid race condition where job might be processed & cleaned up
-- before getJob() is called
return {jobId, groupId, data, "0", tostring(maxAttempts), tostring(timestamp), tostring(orderMs), tostring(delayUntil), jobStatus}


