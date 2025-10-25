-- argv: ns, nowMs, maxToPromote
local ns           = ARGV[1]
local now          = tonumber(ARGV[2]) or 0
local maxToPromote = tonumber(ARGV[3]) or 256

local stageZKey = ns .. ":stage"

if now <= 0 then
  local t = redis.call("TIME")
  now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

local jobIds = redis.call("ZRANGEBYSCORE", stageZKey, "-inf", now, "LIMIT", 0, maxToPromote)
if not jobIds or #jobIds == 0 then
  return 0
end

local promoted = 0

for _, jobId in ipairs(jobIds) do
  local stageJobKey = ns .. ":stage:job:" .. jobId
  local fields = redis.call("HMGET", stageJobKey,
    "ns","groupId","dataJson","maxAttempts","orderMs","delayUntil","jobId","keepCompleted","clientTimestamp"
  )

  if fields and fields[1] then
    local shouldEnqueue = true
    local ns2              = fields[1]
    local groupId          = fields[2]
    local dataJson         = fields[3]
    local maxAttempts      = tonumber(fields[4]) or 0
    local orderMs          = tonumber(fields[5]) or 0
    local delayUntil       = tonumber(fields[6]) or 0
    local jobId2           = fields[7]
    local keepCompleted    = tonumber(fields[8]) or 0
    local clientTimestamp  = tonumber(fields[9]) or now

    -- Inline the essential parts of enqueue.lua (kept in sync):
    local readyKey   = ns2 .. ":ready"
    local delayedKey = ns2 .. ":delayed"
    local jobKey     = ns2 .. ":job:" .. jobId2
    local groupsKey  = ns2 .. ":groups"

    -- Idempotence: unique key guard with stale recovery
    local uniqueKey = ns2 .. ":unique:" .. jobId2
    local uniqueSet = redis.call("SET", uniqueKey, jobId2, "NX")
    if not uniqueSet then
      local exists = redis.call("EXISTS", jobKey)
      if exists == 0 then
        redis.call("DEL", uniqueKey)
        redis.call("SET", uniqueKey, jobId2)
      else
        local gid = redis.call("HGET", jobKey, "groupId")
        local inProcessing = redis.call("ZSCORE", ns2 .. ":processing", jobId2)
        local inDelayed = redis.call("ZSCORE", ns2 .. ":delayed", jobId2)
        local inGroup = nil
        if gid then inGroup = redis.call("ZSCORE", ns2 .. ":g:" .. gid, jobId2) end
        if (not inProcessing) and (not inDelayed) and (not inGroup) then
          if keepCompleted == 0 then
            redis.call("DEL", jobKey)
            redis.call("DEL", uniqueKey)
            redis.call("SET", uniqueKey, jobId2)
          else
            redis.call("SET", uniqueKey, jobId2)
            -- Already enqueued/retained previously; treat as success
            redis.call("ZREM", stageZKey, jobId2)
            redis.call("DEL", stageJobKey)
            promoted = promoted + 1
            shouldEnqueue = false
          end
        else
          if keepCompleted == 0 then
            local status = redis.call("HGET", jobKey, "status")
            if status == "completed" then
              redis.call("DEL", jobKey)
              redis.call("DEL", uniqueKey)
              redis.call("SET", uniqueKey, jobId2)
            else
              redis.call("SET", uniqueKey, jobId2)
              -- Already present somewhere; treat as success
              redis.call("ZREM", stageZKey, jobId2)
              redis.call("DEL", stageJobKey)
              promoted = promoted + 1
              shouldEnqueue = false
            end
          end
          local activeAgain = redis.call("ZSCORE", ns2 .. ":processing", jobId2)
          local delayedAgain = redis.call("ZSCORE", ns2 .. ":delayed", jobId2)
          local inGroupAgain = nil
          if gid then inGroupAgain = redis.call("ZSCORE", ns2 .. ":g:" .. gid, jobId2) end
          local jobStillExists = redis.call("EXISTS", jobKey)
          if jobStillExists == 1 and (activeAgain or delayedAgain or inGroupAgain) then
            redis.call("ZREM", stageZKey, jobId2)
            redis.call("DEL", stageJobKey)
            promoted = promoted + 1
            shouldEnqueue = false
          end
        end
      end
    end

    if shouldEnqueue then
      local gZ = ns2 .. ":g:" .. groupId

      if orderMs == 0 then
        local t = redis.call("TIME")
        orderMs = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
      end
      local baseEpoch = 1704067200000
      local relativeMs = orderMs - baseEpoch

      local daysSinceEpoch = math.floor(orderMs / 86400000)
      local seqKey = ns2 .. ":seq:" .. daysSinceEpoch
      local seq = redis.call("INCR", seqKey)
      local score = relativeMs * 1000 + seq

      local timestamp = clientTimestamp

      redis.call("HMSET", jobKey,
        "id", jobId2,
        "groupId", groupId,
        "data", dataJson,
        "attempts", "0",
        "maxAttempts", tostring(maxAttempts),
        "seq", tostring(seq),
        "timestamp", tostring(timestamp),
        "orderMs", tostring(orderMs),
        "score", tostring(score),
        "delayUntil", tostring(delayUntil)
      )

      redis.call("SADD", groupsKey, groupId)
      redis.call("ZADD", gZ, score, jobId2)

      if delayUntil > 0 and delayUntil > now then
        redis.call("ZADD", delayedKey, delayUntil, jobId2)
      else
        local head = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
        if head and #head >= 2 then
          local headScore = tonumber(head[2])
          redis.call("ZADD", readyKey, headScore, groupId)
        end
      end

      -- cleanup staging only after success
      redis.call("ZREM", stageZKey, jobId2)
      redis.call("DEL", stageJobKey)
      promoted = promoted + 1
    end
  else
    -- stale entry
    redis.call("ZREM", stageZKey, jobId)
  end
end

return promoted


