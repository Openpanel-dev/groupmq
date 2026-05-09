-- Simplified enqueue for non-grouped jobs
-- No group tracking, no staging, just add to simple queue
-- argv: ns, jobId, dataJson, orderMs, maxAttempts, keepCompleted, clientTimestamp
local ns = KEYS[1]
local jobId = ARGV[1]
local data = ARGV[2]
local orderMs = tonumber(ARGV[3])
local maxAttempts = tonumber(ARGV[4])
local keepCompleted = tonumber(ARGV[5]) or 0
local clientTimestamp = tonumber(ARGV[6])

local simpleKey = ns .. ":simple"
local jobKey = ns .. ":job:" .. jobId

-- Idempotence: ensure unique jobId per queue namespace
local uniqueKey = ns .. ":unique:" .. jobId
local uniqueSet = redis.call("SET", uniqueKey, jobId, "NX")
if not uniqueSet then
  -- Duplicate detected - check if job exists
  local exists = redis.call("EXISTS", jobKey)
  if exists == 0 then
    -- Stale unique key, clean up and proceed
    redis.call("DEL", uniqueKey)
    redis.call("SET", uniqueKey, jobId)
  else
    -- Job exists, return existing job data (idempotent).
    -- Read inside the script so the response is atomic with the dedup check —
    -- otherwise retention can trim the hash between this script returning and
    -- the client doing a follow-up HGETALL, causing a spurious "job not found".
    local existing = redis.call("HMGET", jobKey,
      "id", "groupId", "data", "attempts", "maxAttempts", "timestamp", "orderMs", "status")
    return {existing[1] or jobId, existing[2] or "", existing[3], existing[4] or "0",
      existing[5] or tostring(maxAttempts), existing[6] or "0",
      existing[7] or tostring(orderMs), "0", existing[8] or "waiting"}
  end
end

-- Use client timestamp or current time
local timestamp = clientTimestamp or (tonumber(redis.call("TIME")[1]) * 1000)

-- Create job hash (minimal fields for simple jobs)
redis.call("HMSET", jobKey,
  "id", jobId,
  "data", data,
  "attempts", "0",
  "maxAttempts", tostring(maxAttempts),
  "orderMs", tostring(orderMs),
  "timestamp", tostring(timestamp),
  "status", "waiting"
)

-- Add to simple queue (ZSET with score=orderMs)
redis.call("ZADD", simpleKey, orderMs, jobId)

-- Return job data
return {jobId, "", data, "0", tostring(maxAttempts), tostring(timestamp), tostring(orderMs), "0", "waiting"}
