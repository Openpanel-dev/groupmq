-- argv: ns, jobId, groupId, extendMs
local ns = KEYS[1]
local jobId = ARGV[1]
local gid = ARGV[2]
local extendMs = tonumber(ARGV[3])

-- BullMQ-style: only extend processing deadline, no group lock
local procKey = ns .. ":processing:" .. jobId
local exists = redis.call("EXISTS", procKey)
if exists == 1 then
  local now = tonumber(redis.call("TIME")[1]) * 1000
  local newDeadline = now + extendMs
  redis.call("HSET", procKey, "deadlineAt", tostring(newDeadline))
  
  -- Also update the processing ZSET score
  local processingKey = ns .. ":processing"
  redis.call("ZADD", processingKey, newDeadline, jobId)
  return 1
end
return 0


