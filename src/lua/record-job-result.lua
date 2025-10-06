-- Record job completion or failure with retention management
-- argv: ns, jobId, status ('completed' | 'failed'), timestamp, result/error (JSON), 
--       keepCompleted, keepFailed, processedOn, finishedOn, attempts, maxAttempts
local ns = ARGV[1]
local jobId = ARGV[2]
local status = ARGV[3]
local timestamp = tonumber(ARGV[4])
local resultOrError = ARGV[5]
local keepCompleted = tonumber(ARGV[6])
local keepFailed = tonumber(ARGV[7])
local processedOn = ARGV[8]
local finishedOn = ARGV[9]
local attempts = ARGV[10]
local maxAttempts = ARGV[11]

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
    -- Ensure idempotence mapping exists
    redis.call("SET", ns .. ":unique:" .. jobId, jobId)
    
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
  
  -- Parse error info from resultOrError JSON
  -- Expected format: {"message":"...", "name":"...", "stack":"..."}
  local errorInfo = cjson.decode(resultOrError)
  
  if keepFailed > 0 then
    -- Store failure metadata
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
    
    -- Note: No retention trimming for failed jobs (let clean() handle it)
  else
    -- keepFailed == 0: Delete immediately
    redis.call("DEL", jobKey)
    redis.call("DEL", ns .. ":unique:" .. jobId)
  end
end

return 1

