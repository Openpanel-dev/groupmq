-- argv: ns, nowEpochMs, vtMs, targetGroupId, orderingDelayMs
local ns = ARGV[1]
local now = tonumber(ARGV[2])
local vt = tonumber(ARGV[3])
local targetGroupId = ARGV[4]
local orderingDelayMs = tonumber(ARGV[5]) or 0

local readyKey = ns .. ":ready"
local gZ = ns .. ":g:" .. targetGroupId
local lockKey = ns .. ":lock:" .. targetGroupId

-- Respect paused state
if redis.call("GET", ns .. ":paused") then
  return nil
end

-- Check if group is locked; re-add to ready and return
local lockTtl = redis.call("PTTL", lockKey)
if lockTtl > 0 then
  local head = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
  if head and #head >= 2 then
    local headScore = tonumber(head[2])
    redis.call("ZADD", readyKey, headScore, targetGroupId)
  end
  return nil
end

local zpop = redis.call("ZPOPMIN", gZ, 1)
if not zpop or #zpop == 0 then
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

redis.call("SET", lockKey, id, "PX", vt)

local procKey = ns .. ":processing:" .. id
local deadline = now + vt
redis.call("HSET", procKey, "groupId", groupId, "deadlineAt", tostring(deadline))

local processingKey = ns .. ":processing"
redis.call("ZADD", processingKey, deadline, id)

local nextHead = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
if nextHead and #nextHead >= 2 then
  local nextScore = tonumber(nextHead[2])
  redis.call("ZADD", readyKey, nextScore, groupId)
end

return id .. "||DELIMITER||" .. groupId .. "||DELIMITER||" .. payload .. "||DELIMITER||" .. attempts .. "||DELIMITER||" .. maxAttempts .. "||DELIMITER||" .. seq .. "||DELIMITER||" .. enq .. "||DELIMITER||" .. orderMs .. "||DELIMITER||" .. score .. "||DELIMITER||" .. deadline


