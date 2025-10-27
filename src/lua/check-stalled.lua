-- Check for stalled jobs and move them back to waiting or fail them
-- KEYS: namespace, currentTime, gracePeriod, maxStalledCount
-- Returns: array of [jobId, groupId, action] for each stalled job found
--   action: "recovered" or "failed"

local ns = KEYS[1]
local now = tonumber(ARGV[1])
local gracePeriod = tonumber(ARGV[2]) or 0
local maxStalledCount = tonumber(ARGV[3]) or 1

-- Circuit breaker for high concurrency: limit stalled job recovery
local circuitBreakerKey = ns .. ":stalled:circuit"
local lastCheck = redis.call("GET", circuitBreakerKey)
if lastCheck then
  local lastCheckTime = tonumber(lastCheck)
  local circuitBreakerInterval = 2000
  if lastCheckTime and (now - lastCheckTime) < circuitBreakerInterval then
    return {}
  end
end
redis.call("SET", circuitBreakerKey, now, "PX", 3000)

local processingKey = ns .. ":processing"
local groupsKey = ns .. ":groups"

-- Candidates: jobs whose deadlines are past
local candidates = redis.call("ZRANGEBYSCORE", processingKey, 0, now - gracePeriod, "LIMIT", 0, 100)
if not candidates or #candidates == 0 then
  return {}
end

local results = {}

for _, jobId in ipairs(candidates) do
  local jobKey = ns .. ":job:" .. jobId
  local h = redis.call("HMGET", jobKey, "groupId","stalledCount","maxAttempts","attempts","status","finishedOn","score")
  local groupId = h[1]
  if groupId then
    local stalledCount = tonumber(h[2]) or 0
    local maxAttempts = tonumber(h[3]) or 3
    local status = h[5]
    local finishedOn = tonumber(h[6] or "0")
    -- CRITICAL: Don't recover jobs that are completing (prevents race with completion)
    -- "completing" is a temporary state set by complete-with-metadata.lua to prevent races
    if status == "processing" then
      stalledCount = stalledCount + 1
      redis.call("HSET", jobKey, "stalledCount", stalledCount)
      -- BullMQ-style: Remove from per-group active list
      local groupActiveKey = ns .. ":g:" .. groupId .. ":active"
      redis.call("LREM", groupActiveKey, 1, jobId)
      
      if stalledCount >= maxStalledCount and maxStalledCount > 0 then
        redis.call("ZREM", processingKey, jobId)
        local groupKey = ns .. ":g:" .. groupId
        redis.call("ZREM", groupKey, jobId)
        redis.call("DEL", ns .. ":processing:" .. jobId)
        redis.call("HSET", jobKey, "status","failed","finishedOn", now,
                   "failedReason", "Job stalled " .. stalledCount .. " times (max: " .. maxStalledCount .. ")")
        redis.call("ZADD", ns .. ":failed", now, jobId)
        table.insert(results, jobId); table.insert(results, groupId); table.insert(results, "failed")
      else
        local stillInProcessing = redis.call("ZSCORE", processingKey, jobId)
        if stillInProcessing then
          redis.call("ZREM", processingKey, jobId)
          redis.call("DEL", ns .. ":processing:" .. jobId)
          local score = tonumber(h[7])
          if score then
            local groupKey2 = ns .. ":g:" .. groupId
            redis.call("ZADD", groupKey2, score, jobId)
            local head = redis.call("ZRANGE", groupKey2, 0, 0, "WITHSCORES")
            if head and #head >= 2 then
              local headScore = tonumber(head[2])
              redis.call("ZADD", ns .. ":ready", headScore, groupId)
            end
            redis.call("SADD", groupsKey, groupId)
          end
          redis.call("HSET", jobKey, "status", "waiting")
          table.insert(results, jobId); table.insert(results, groupId); table.insert(results, "recovered")
        end
      end
    end
  end
end

return results

