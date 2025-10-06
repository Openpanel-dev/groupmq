-- argv: ns, now
-- Promotes groups from buffering state to ready when their buffering window has elapsed
local ns = ARGV[1]
local now = tonumber(ARGV[2])

local bufferingKey = ns .. ":buffering"
local readyKey = ns .. ":ready"

-- Get groups whose buffering period has elapsed (score <= now)
local dueGroups = redis.call("ZRANGEBYSCORE", bufferingKey, 0, now)

if not dueGroups or #dueGroups == 0 then
  return 0
end

local promoted = 0

for i = 1, #dueGroups do
  local groupId = dueGroups[i]
  local gZ = ns .. ":g:" .. groupId
  local groupBufferKey = ns .. ":buffer:" .. groupId
  
  -- Check if group still has jobs
  local head = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
  if head and #head >= 2 then
    local headJobId = head[1]
    local headScore = tonumber(head[2])
    
    -- Check if head job is not delayed
    local jobKey = ns .. ":job:" .. headJobId
    local delayUntil = tonumber(redis.call("HGET", jobKey, "delayUntil"))
    
    if not delayUntil or delayUntil == 0 or delayUntil <= now then
      -- Add group to ready queue with the head job's score
      redis.call("ZADD", readyKey, headScore, groupId)
      promoted = promoted + 1
    end
  end
  
  -- Remove from buffering set
  redis.call("ZREM", bufferingKey, groupId)
  
  -- Clean up buffer tracking key
  redis.call("DEL", groupBufferKey)
end

return promoted

