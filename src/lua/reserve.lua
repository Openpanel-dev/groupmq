-- argv: ns, nowEpochMs, vtMs, scanLimit
local ns = ARGV[1]
local now = tonumber(ARGV[2])
local vt = tonumber(ARGV[3])
local scanLimit = tonumber(ARGV[4]) or 20

local readyKey = ns .. ":ready"

-- Respect paused state
if redis.call("GET", ns .. ":paused") then
  return nil
end

-- Check for expired jobs using processing timeline
local processingKey = ns .. ":processing"
local expiredJobs = redis.call("ZRANGEBYSCORE", processingKey, 0, now)
for _, jobId in ipairs(expiredJobs) do
  -- CRITICAL: Verify job is STILL in processing to avoid race conditions
  -- If job was completed between our snapshot and now, don't re-add it
  local stillInProcessing = redis.call("ZSCORE", processingKey, jobId)
  
  if stillInProcessing then
    local procKey = ns .. ":processing:" .. jobId
    local procData = redis.call("HMGET", procKey, "groupId", "deadlineAt")
    local gid = procData[1]
    local deadlineAt = tonumber(procData[2])
    if gid and deadlineAt and now > deadlineAt then
      local jobKey = ns .. ":job:" .. jobId
      local jobScore = redis.call("HGET", jobKey, "score")
      if jobScore then
        local gZ = ns .. ":g:" .. gid
        redis.call("ZADD", gZ, tonumber(jobScore), jobId)
        local head = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
        if head and #head >= 2 then
          local headScore = tonumber(head[2])
          redis.call("ZADD", readyKey, headScore, gid)
        end
        redis.call("DEL", ns .. ":lock:" .. gid)
        redis.call("DEL", procKey)
        redis.call("ZREM", processingKey, jobId)
      end
    end
  end
  -- If not still in processing, it was completed - don't re-add it!
end

-- Get available groups
local groups = redis.call("ZRANGE", readyKey, 0, scanLimit - 1, "WITHSCORES")
if not groups or #groups == 0 then
  return nil
end

local chosenGid = nil
local chosenIndex = nil
for i = 1, #groups, 2 do
  local gid = groups[i]
  local lockKey = ns .. ":lock:" .. gid
  local lockTtl = redis.call("PTTL", lockKey)
  if lockTtl == -2 or lockTtl == -1 then
    chosenGid = gid
    chosenIndex = (i + 1) / 2 - 1
    break
  end
end

if not chosenGid then
  return nil
end

redis.call("ZREMRANGEBYRANK", readyKey, chosenIndex, chosenIndex)

local gZ = ns .. ":g:" .. chosenGid
local zpop = redis.call("ZPOPMIN", gZ, 1)
if not zpop or #zpop == 0 then
  return nil
end
local headJobId = zpop[1]

local jobKey = ns .. ":job:" .. headJobId
local job = redis.call("HMGET", jobKey, "id","groupId","data","attempts","maxAttempts","seq","timestamp","orderMs","score")
local id, groupId, payload, attempts, maxAttempts, seq, enq, orderMs, score = job[1], job[2], job[3], job[4], job[5], job[6], job[7], job[8], job[9]

local lockKey = ns .. ":lock:" .. chosenGid
redis.call("SET", lockKey, id, "PX", vt)

local procKey = ns .. ":processing:" .. id
local deadline = now + vt
redis.call("HSET", procKey, "groupId", chosenGid, "deadlineAt", tostring(deadline))

local processingKey2 = ns .. ":processing"
redis.call("ZADD", processingKey2, deadline, id)

-- Increment active counter
local activeCountKey = ns .. ":count:active"
redis.call("INCR", activeCountKey)

local nextHead = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
if nextHead and #nextHead >= 2 then
  local nextScore = tonumber(nextHead[2])
  redis.call("ZADD", readyKey, nextScore, chosenGid)
end

return id .. "||DELIMITER||" .. groupId .. "||DELIMITER||" .. payload .. "||DELIMITER||" .. attempts .. "||DELIMITER||" .. maxAttempts .. "||DELIMITER||" .. seq .. "||DELIMITER||" .. enq .. "||DELIMITER||" .. orderMs .. "||DELIMITER||" .. score .. "||DELIMITER||" .. deadline


