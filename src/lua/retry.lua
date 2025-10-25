-- argv: ns, jobId, backoffMs
local ns = ARGV[1]
local jobId = ARGV[2]
local backoffMs = tonumber(ARGV[3]) or 0

local jobKey = ns .. ":job:" .. jobId
local gid = redis.call("HGET", jobKey, "groupId")
local attempts = tonumber(redis.call("HINCRBY", jobKey, "attempts", 1))
local maxAttempts = tonumber(redis.call("HGET", jobKey, "maxAttempts"))

redis.call("DEL", ns .. ":processing:" .. jobId)
redis.call("ZREM", ns .. ":processing", jobId)

-- No counter operations - use ZCARD for counts

if attempts > maxAttempts then
  return -1
end

local score = tonumber(redis.call("HGET", jobKey, "score"))
local gZ = ns .. ":g:" .. gid
redis.call("ZADD", gZ, score, jobId)

local head = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
if head and #head >= 2 then
  local headScore = tonumber(head[2])
  local readyKey = ns .. ":ready"
  redis.call("ZADD", readyKey, headScore, gid)
end

if backoffMs > 0 then
  local lockKey = ns .. ":lock:" .. gid
  redis.call("SET", lockKey, jobId, "PX", backoffMs)
end

return attempts
