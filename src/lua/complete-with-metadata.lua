-- Complete a job: unlock group AND record metadata atomically in one call
-- argv: ns, jobId, groupId, status, timestamp, resultOrError, keepCompleted, keepFailed,
--       processedOn, finishedOn, attempts, maxAttempts
local ns = ARGV[1]
local jobId = ARGV[2]
local gid = ARGV[3]
local status = ARGV[4]
local timestamp = tonumber(ARGV[5])
local resultOrError = ARGV[6]
local keepCompleted = tonumber(ARGV[7])
local keepFailed = tonumber(ARGV[8])
local processedOn = ARGV[9]
local finishedOn = ARGV[10]
local attempts = ARGV[11]
local maxAttempts = ARGV[12]

-- Part 1: Remove from processing and unlock group
redis.call("DEL", ns .. ":processing:" .. jobId)
redis.call("ZREM", ns .. ":processing", jobId)

local lockKey = ns .. ":lock:" .. gid
local val = redis.call("GET", lockKey)
local hadLock = (val == jobId)

if hadLock then
  redis.call("DEL", lockKey)
  
  -- Check if there are more jobs in this group
  local gZ = ns .. ":g:" .. gid
  local jobCount = redis.call("ZCARD", gZ)
  if jobCount == 0 then
    -- Remove empty group
    redis.call("DEL", gZ)
    redis.call("SREM", ns .. ":groups", gid)
    redis.call("ZREM", ns .. ":ready", gid)
    redis.call("DEL", ns .. ":buffer:" .. gid)
    redis.call("ZREM", ns .. ":buffering", gid)
  else
    -- Group has more jobs, re-add to ready if not buffering
    local groupBufferKey = ns .. ":buffer:" .. gid
    local isBuffering = redis.call("EXISTS", groupBufferKey)
    
    if isBuffering == 0 then
      local nextHead = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
      if nextHead and #nextHead >= 2 then
        local nextScore = tonumber(nextHead[2])
        local readyKey = ns .. ":ready"
        redis.call("ZADD", readyKey, nextScore, gid)
      end
    end
  end
end

-- Part 2: Record job metadata (completed or failed)
local jobKey = ns .. ":job:" .. jobId

if status == "completed" then
  local completedKey = ns .. ":completed"
  
  if keepCompleted > 0 then
    -- Store job metadata and add to completed set
    redis.call("HSET", jobKey, 
      "status", "completed",
      "processedOn", processedOn,
      "finishedOn", finishedOn,
      "attempts", attempts,
      "maxAttempts", maxAttempts,
      "returnvalue", resultOrError
    )
    redis.call("ZADD", completedKey, timestamp, jobId)
    
    -- Trim old entries atomically
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
    -- keepCompleted == 0: Delete immediately
    redis.call("DEL", jobKey)
    redis.call("DEL", ns .. ":unique:" .. jobId)
  end
  
elseif status == "failed" then
  local failedKey = ns .. ":failed"
  local errorInfo = cjson.decode(resultOrError)
  
  if keepFailed > 0 then
    redis.call("HSET", jobKey,
      "status", "failed",
      "failedReason", errorInfo.message or "Error",
      "failedName", errorInfo.name or "Error",
      "stacktrace", errorInfo.stack or "",
      "processedOn", processedOn,
      "finishedOn", finishedOn,
      "attempts", attempts,
      "maxAttempts", maxAttempts
    )
    redis.call("ZADD", failedKey, timestamp, jobId)
  else
    redis.call("DEL", jobKey)
    redis.call("DEL", ns .. ":unique:" .. jobId)
  end
end

return 1

