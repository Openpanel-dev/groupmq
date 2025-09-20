-- argv: ns, jobId, groupId, extendMs
local ns = ARGV[1]
local jobId = ARGV[2]
local gid = ARGV[3]
local extendMs = tonumber(ARGV[4])

local lockKey = ns .. ":lock:" .. gid
local val = redis.call("GET", lockKey)
if val == jobId then
  redis.call("PEXPIRE", lockKey, extendMs)
  local procKey = ns .. ":processing:" .. jobId
  local now = tonumber(redis.call("TIME")[1]) * 1000
  redis.call("HSET", procKey, "deadlineAt", tostring(now + extendMs))
  return 1
end
return 0


