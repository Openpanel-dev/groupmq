-- argv: ns, groupId, dataJson, maxAttempts, orderMs, delayUntil, jobId, keepCompleted, clientTimestamp, orderingWindowMs?
local ns              = ARGV[1]
local groupId         = ARGV[2]
local dataJson        = ARGV[3]
local maxAttempts     = tonumber(ARGV[4])
local orderMs         = tonumber(ARGV[5])
local delayUntilArg   = tonumber(ARGV[6]) or 0
local jobId           = ARGV[7]
local keepCompleted   = tonumber(ARGV[8]) or 0
local clientTimestamp = tonumber(ARGV[9]) or 0
local windowArg       = tonumber(ARGV[10])

local stageZKey   = ns .. ":stage"
local stageJobKey = ns .. ":stage:job:" .. jobId
local windowKey   = ns .. ":orderingWindowMs"

-- determine window in ms (queue-level default)
local windowMs = 0
if windowArg and windowArg >= 0 then
  windowMs = windowArg
else
  local windowStr = redis.call("GET", windowKey)
  if windowStr then
    local parsed = tonumber(windowStr)
    if parsed and parsed > 0 then windowMs = parsed end
  end
end

-- compute releaseAt = max(delayUntil, orderMs + windowMs)
if not orderMs then
  -- fallback to server time when not provided
  local t = redis.call("TIME")
  orderMs = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

local releaseAtCandidate = orderMs + windowMs
local releaseAt = releaseAtCandidate
if delayUntilArg and delayUntilArg > releaseAt then
  releaseAt = delayUntilArg
end

-- persist staged job payload (idempotent-ish: overwrite fields for same id)
redis.call("HMSET", stageJobKey,
  "ns", ns,
  "groupId", groupId,
  "dataJson", dataJson,
  "maxAttempts", tostring(maxAttempts or 0),
  "orderMs", tostring(orderMs or 0),
  "delayUntil", tostring(delayUntilArg or 0),
  "jobId", jobId,
  "keepCompleted", tostring(keepCompleted or 0),
  "clientTimestamp", tostring(clientTimestamp or 0)
)

-- index in staging set by releaseAt
redis.call("ZADD", stageZKey, releaseAt, jobId)

-- optional: set next due timer for expiring-key approach
local head = redis.call("ZRANGE", stageZKey, 0, 0, "WITHSCORES")
if head and #head >= 2 then
  local headScore = tonumber(head[2])
  if headScore then
    -- best-effort; if keyspace notifications are enabled, this enables precise wakeups
    redis.call("PEXPIREAT", ns .. ":stage:timer", headScore)
  end
end

return { jobId, tostring(releaseAt), tostring(windowMs) }


