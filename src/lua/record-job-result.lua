-- Record job completion or failure with retention management
-- argv: ns, jobId, status ('completed' | 'failed'), timestamp, result/error (JSON), 
--       keepCompleted, keepFailed, processedOn, finishedOn, attempts, maxAttempts
local ns = KEYS[1]
local jobId = ARGV[1]
local status = ARGV[2]
local timestamp = tonumber(ARGV[3])
local resultOrError = ARGV[4]
local keepCompleted = tonumber(ARGV[5])
local keepFailed = tonumber(ARGV[6])
local processedOn = ARGV[7]
local finishedOn = ARGV[8]
local attempts = ARGV[9]
local maxAttempts = ARGV[10]

local jobKey = ns .. ":job:" .. jobId

-- Verify job exists and check current status to prevent race conditions
local currentStatus = redis.call("HGET", jobKey, "status")
if not currentStatus then
  -- Job doesn't exist, likely already cleaned up
  return 0
end

-- If job is in "waiting" state, this might be a late completion after stalled recovery
-- In this case, we should not overwrite the status or delete the job
if currentStatus == "waiting" then
  -- Job was recovered by stalled check and possibly being processed by another worker
  -- Ignore this late completion to prevent corruption
  return 0
end

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
        -- Batch delete old jobs and unique keys
        local keysToDelete = {}
        for i = 1, #oldIds do
          local oldId = oldIds[i]
          table.insert(keysToDelete, ns .. ":job:" .. oldId)
          table.insert(keysToDelete, ns .. ":unique:" .. oldId)
        end
        if #keysToDelete > 0 then
          redis.call("DEL", unpack(keysToDelete))
        end
      end
    end
  else
    -- keepCompleted == 0: Delete immediately (batch operation)
    redis.call("DEL", jobKey, ns .. ":unique:" .. jobId)
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
    -- keepFailed == 0: Delete immediately (batch operation)
    redis.call("DEL", jobKey, ns .. ":unique:" .. jobId)
  end
end

return 1

