-- argv: ns, completedJobId, groupId, now, vt, orderingDelayMs, keepCompleted
local ns = ARGV[1]
local completedJobId = ARGV[2]
local gid = ARGV[3]
local now = tonumber(ARGV[4])
local vt = tonumber(ARGV[5])
local orderingDelayMs = tonumber(ARGV[6]) or 0
local keepCompleted = tonumber(ARGV[7]) or 0

redis.call("DEL", ns .. ":processing:" .. completedJobId)
redis.call("ZREM", ns .. ":processing", completedJobId)
-- Atomically mark completed and add to completed set for retention
local jobKey = ns .. ":job:" .. completedJobId
local completedKey = ns .. ":completed"

-- If keepCompleted retention is configured, check count BEFORE adding this job
local toRemove = 0
if keepCompleted >= 0 then
  local zcount = redis.call("ZCARD", completedKey)
  toRemove = zcount - keepCompleted + 1  -- +1 because we're about to add this job
end

redis.call("HSET", jobKey, "status", "completed", "finishedOn", tostring(now))
redis.call("ZADD", completedKey, now, completedJobId)

-- Trim old entries if we exceed the limit
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

-- Note: unique keys are only deleted when job hashes are trimmed above

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
  end
  -- No next job; retention trimming already handled above
  return nil
end

local nextJobId = zpop[1]
local jobKey = ns .. ":job:" .. nextJobId
local job = redis.call("HMGET", jobKey, "id","groupId","data","attempts","maxAttempts","seq","timestamp","orderMs","score")
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

local nextHead = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
if nextHead and #nextHead >= 2 then
  local nextScore = tonumber(nextHead[2])
  local readyKey = ns .. ":ready"
  redis.call("ZADD", readyKey, nextScore, groupId)
end

return id .. "||DELIMITER||" .. groupId .. "||DELIMITER||" .. payload .. "||DELIMITER||" .. attempts .. "||DELIMITER||" .. maxAttempts .. "||DELIMITER||" .. seq .. "||DELIMITER||" .. enq .. "||DELIMITER||" .. orderMs .. "||DELIMITER||" .. score .. "||DELIMITER||" .. deadline


