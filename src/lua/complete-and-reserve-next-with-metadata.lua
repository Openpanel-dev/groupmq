-- Complete a job with metadata and atomically reserve the next job from the same group
-- argv: ns, completedJobId, groupId, status, timestamp, resultOrError, keepCompleted, keepFailed,
--       processedOn, finishedOn, attempts, maxAttempts, now, vt, orderingDelayMs
local ns = ARGV[1]
local completedJobId = ARGV[2]
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
local now = tonumber(ARGV[13])
local vt = tonumber(ARGV[14])
local orderingDelayMs = tonumber(ARGV[15]) or 0

-- Part 1: Remove completed job from processing
redis.call("DEL", ns .. ":processing:" .. completedJobId)
redis.call("ZREM", ns .. ":processing", completedJobId)

-- Decrement active counter
local activeCountKey = ns .. ":count:active"
redis.call("DECR", activeCountKey)

-- Part 2: Record job metadata (completed or failed)
local jobKey = ns .. ":job:" .. completedJobId

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
    redis.call("ZADD", completedKey, timestamp, completedJobId)
    
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
    redis.call("DEL", ns .. ":unique:" .. completedJobId)
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
    redis.call("ZADD", failedKey, timestamp, completedJobId)
  else
    redis.call("DEL", jobKey)
    redis.call("DEL", ns .. ":unique:" .. completedJobId)
  end
end

-- Part 3: Handle group lock and get next job
local lockKey = ns .. ":lock:" .. gid
local val = redis.call("GET", lockKey)
if val ~= completedJobId then
  return nil
end

local gZ = ns .. ":g:" .. gid
local zpop = redis.call("ZPOPMIN", gZ, 1)
if not zpop or #zpop == 0 then
  redis.call("DEL", lockKey)
  -- Clean up empty group
  local jobCount = redis.call("ZCARD", gZ)
  if jobCount == 0 then
    redis.call("DEL", gZ)
    redis.call("SREM", ns .. ":groups", gid)
    redis.call("ZREM", ns .. ":ready", gid)
  end
  -- No next job
  return nil
end

local nextJobId = zpop[1]
local nextJobKey = ns .. ":job:" .. nextJobId
local job = redis.call("HMGET", nextJobKey, "id","groupId","data","attempts","maxAttempts","seq","timestamp","orderMs","score")
local id, groupId, payload, attempts, maxAttempts, seq, enq, orderMs, score = job[1], job[2], job[3], job[4], job[5], job[6], job[7], job[8], job[9]

if orderingDelayMs > 0 and orderMs then
  local jobOrderMs = tonumber(orderMs)
  if jobOrderMs then
    local eligibleAt = jobOrderMs > now and jobOrderMs or (jobOrderMs + orderingDelayMs)
    if eligibleAt > now then
      local putBackScore = tonumber(score)
      redis.call("ZADD", gZ, putBackScore, nextJobId)
      local remainingDelayMs = eligibleAt - now
      redis.call("SET", lockKey, "ordering-delay", "PX", remainingDelayMs)
      local nextHead = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
      if nextHead and #nextHead >= 2 then
        local nextScore = tonumber(nextHead[2])
        local readyKey = ns .. ":ready"
        redis.call("ZADD", readyKey, nextScore, groupId)
      end
      return nil
    end
  end
end

redis.call("SET", lockKey, id, "PX", vt)

local procKey = ns .. ":processing:" .. id
local deadline = now + vt
redis.call("HSET", procKey, "groupId", groupId, "deadlineAt", tostring(deadline))

local processingKey = ns .. ":processing"
redis.call("ZADD", processingKey, deadline, id)

-- Increment active counter for new job (completed job was already decremented above)
redis.call("INCR", activeCountKey)

local nextHead = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
if nextHead and #nextHead >= 2 then
  local nextScore = tonumber(nextHead[2])
  local readyKey = ns .. ":ready"
  redis.call("ZADD", readyKey, nextScore, groupId)
end

return id .. "||DELIMITER||" .. groupId .. "||DELIMITER||" .. payload .. "||DELIMITER||" .. attempts .. "||DELIMITER||" .. maxAttempts .. "||DELIMITER||" .. seq .. "||DELIMITER||" .. enq .. "||DELIMITER||" .. orderMs .. "||DELIMITER||" .. score .. "||DELIMITER||" .. deadline
