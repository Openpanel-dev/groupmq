-- Atomic reserve operation that checks lock and reserves in one operation
-- argv: ns, nowEpochMs, vtMs, targetGroupId, orderingDelayMs, allowedJobId (optional)
local ns = ARGV[1]
local now = tonumber(ARGV[2])
local vt = tonumber(ARGV[3])
local targetGroupId = ARGV[4]
local orderingDelayMs = tonumber(ARGV[5]) or 0
local allowedJobId = ARGV[6] -- If provided, allow reserve if lock matches this job ID

local readyKey = ns .. ":ready"
local gZ = ns .. ":g:" .. targetGroupId
local lockKey = ns .. ":lock:" .. targetGroupId

-- Respect paused state
if redis.call("GET", ns .. ":paused") then
  return nil
end

-- Check if group is locked
local currentLock = redis.call("GET", lockKey)

if currentLock then
  -- If allowedJobId is provided and matches the lock, we can proceed (grace collection)
  if allowedJobId and currentLock == allowedJobId then
    -- Lock belongs to us, we can reserve more jobs from this group
    -- Extend the lock TTL since we're still working with this group
    redis.call("PEXPIRE", lockKey, vt)
  else
    -- Group is locked by another worker or different job, re-add to ready and return
    local head = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
    if head and #head >= 2 then
      local headScore = tonumber(head[2])
      redis.call("ZADD", readyKey, headScore, targetGroupId)
    end
    return nil
  end
else
  -- No lock exists, try to atomically set it
  local lockSet = redis.call("SET", lockKey, "reserving", "PX", vt, "NX")
  if not lockSet then
    -- Another worker grabbed the lock between our check and set
    local head = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
    if head and #head >= 2 then
      local headScore = tonumber(head[2])
      redis.call("ZADD", readyKey, headScore, targetGroupId)
    end
    return nil
  end
end

-- Try to get a job from the group
local zpop = redis.call("ZPOPMIN", gZ, 1)
if not zpop or #zpop == 0 then
  -- No job available, remove lock and return
  redis.call("DEL", lockKey)
  return nil
end
local headJobId = zpop[1]

local jobKey = ns .. ":job:" .. headJobId
local job = redis.call("HMGET", jobKey, "id","groupId","data","attempts","maxAttempts","seq","timestamp","orderMs","score")
local id, groupId, payload, attempts, maxAttempts, seq, enq, orderMs, score = job[1], job[2], job[3], job[4], job[5], job[6], job[7], job[8], job[9]

if orderingDelayMs > 0 and orderMs then
  local jobOrderMs = tonumber(orderMs)
  if jobOrderMs then
    local eligibleAt = jobOrderMs > now and jobOrderMs or (jobOrderMs + orderingDelayMs)
    if eligibleAt > now then
      local putBackScore = tonumber(score)
      redis.call("ZADD", gZ, putBackScore, headJobId)
      local remainingDelayMs = eligibleAt - now
      redis.call("SET", lockKey, "ordering-delay", "PX", remainingDelayMs)
      return nil
    end
  end
end

-- Update lock with actual job ID (unless we're in grace collection mode)
if allowedJobId then
  -- Keep the original lock during grace collection
  redis.call("PEXPIRE", lockKey, vt)
else
  -- Normal reserve: update lock to this job's ID
  redis.call("SET", lockKey, id, "PX", vt)
end

local procKey = ns .. ":processing:" .. id
local deadline = now + vt
redis.call("HSET", procKey, "groupId", groupId, "deadlineAt", tostring(deadline))

local processingKey = ns .. ":processing"
redis.call("ZADD", processingKey, deadline, id)

-- Increment active counter
local activeCountKey = ns .. ":count:active"
redis.call("INCR", activeCountKey)

local nextHead = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
if nextHead and #nextHead >= 2 then
  local nextScore = tonumber(nextHead[2])
  redis.call("ZADD", readyKey, nextScore, groupId)
end

return id .. "||DELIMITER||" .. groupId .. "||DELIMITER||" .. payload .. "||DELIMITER||" .. attempts .. "||DELIMITER||" .. maxAttempts .. "||DELIMITER||" .. seq .. "||DELIMITER||" .. enq .. "||DELIMITER||" .. orderMs .. "||DELIMITER||" .. score .. "||DELIMITER||" .. deadline
