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
  
  -- Clean up empty groups
  local gZ = ns .. ":g:" .. gid
  local jobCount = redis.call("ZCARD", gZ)
  if jobCount == 0 then
    -- Remove empty group zset and from groups tracking set
    redis.call("DEL", gZ)
    redis.call("SREM", ns .. ":groups", gid)
  end
  
  return 1
end
return 0


