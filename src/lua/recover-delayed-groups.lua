-- argv: ns, now, orderingDelayMs
local ns = ARGV[1]
local now = tonumber(ARGV[2])
local orderingDelayMs = tonumber(ARGV[3])

local recoveredCount = 0
local readyKey = ns .. ":ready"

local groupsKey = ns .. ":groups"
local groupIds = redis.call("SMEMBERS", groupsKey)

for i = 1, #groupIds do
  local groupId = groupIds[i]
  local gZ = ns .. ":g:" .. groupId
  if groupId then
    local lockKey = ns .. ":lock:" .. groupId
    local lockExists = redis.call("EXISTS", lockKey)
    if lockExists == 0 then
      local head = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
      if head and #head >= 2 then
        local headJobId = head[1]
        local headScore = tonumber(head[2])
        local jobKey = ns .. ":job:" .. headJobId
        local orderMs = redis.call("HGET", jobKey, "orderMs")
        if orderMs then
          local jobOrderMs = tonumber(orderMs)
          local eligibleAt
          if jobOrderMs > now then
            eligibleAt = jobOrderMs
          else
            eligibleAt = jobOrderMs + orderingDelayMs
          end
          if jobOrderMs and (eligibleAt <= now) then
            local isInReady = redis.call("ZSCORE", readyKey, groupId)
            if not isInReady then
              redis.call("ZADD", readyKey, headScore, groupId)
              recoveredCount = recoveredCount + 1
            end
          end
        end
      end
    end
  end
end

return recoveredCount


