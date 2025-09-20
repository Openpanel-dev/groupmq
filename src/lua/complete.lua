-- argv: ns, jobId, groupId, keepCompleted
local ns = ARGV[1]
local jobId = ARGV[2]
local gid = ARGV[3]
local keepCompleted = tonumber(ARGV[4]) or 0

redis.call("DEL", ns .. ":processing:" .. jobId)
redis.call("ZREM", ns .. ":processing", jobId)
if keepCompleted == 0 then
  redis.call("DEL", ns .. ":unique:" .. jobId)
end
local lockKey = ns .. ":lock:" .. gid
local val = redis.call("GET", lockKey)
if val == jobId then
  redis.call("DEL", lockKey)
  return 1
end
return 0


