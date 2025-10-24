-- argv: ns, nowEpochMs
local ns = ARGV[1]
local now = tonumber(ARGV[2])

local readyKey = ns .. ":ready"
local processingKey = ns .. ":processing"
local cleaned = 0

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
        cleaned = cleaned + 1
      end
    end
  end
  -- If not still in processing, it was completed - don't re-add it!
end

return cleaned


