-- argv: ns, groupId, now
local ns = ARGV[1]
local groupId = ARGV[2]
local now = tonumber(ARGV[3])

local readyKey = ns .. ":ready"
local gZ = ns .. ":g:" .. groupId
local lockKey = ns .. ":lock:" .. groupId

-- Check if group has any jobs at all
local jobCount = redis.call("ZCARD", gZ)
if jobCount == 0 then
  redis.call("ZREM", readyKey, groupId)
  return "empty"
end

-- Check if group is currently locked by another worker
local lockValue = redis.call("GET", lockKey)
if lockValue then
  local lockTtl = redis.call("PTTL", lockKey)
  if lockTtl > 0 then
    return "locked"
  end
end

-- Check if all jobs in the group have exceeded max attempts
local jobs = redis.call("ZRANGE", gZ, 0, -1)
local reservableJobs = 0
for i = 1, #jobs do
  local jobId = jobs[i]
  local jobKey = ns .. ":job:" .. jobId
  local attempts = tonumber(redis.call("HGET", jobKey, "attempts"))
  local maxAttempts = tonumber(redis.call("HGET", jobKey, "maxAttempts"))
  if attempts and maxAttempts and attempts < maxAttempts then
    reservableJobs = reservableJobs + 1
  end
end

if reservableJobs == 0 then
  redis.call("ZREM", readyKey, groupId)
  return "poisoned"
end

return "ok"


