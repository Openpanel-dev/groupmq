-- argv: ns, nowEpochMs, vtMs, maxBatch, orderingDelayMs
local ns = ARGV[1]
local now = tonumber(ARGV[2])
local vt = tonumber(ARGV[3])
local maxBatch = tonumber(ARGV[4]) or 16
local orderingDelayMs = tonumber(ARGV[5]) or 0

local readyKey = ns .. ":ready"
local processingKey = ns .. ":processing"

-- Early exit if paused
if redis.call("GET", ns .. ":paused") then
  return {}
end

local out = {}

-- Pop up to maxBatch groups from ready set (lowest score first)
local groups = redis.call("ZRANGE", readyKey, 0, maxBatch - 1, "WITHSCORES")
if not groups or #groups == 0 then
  return {}
end

local processedGroups = {}
for i = 1, #groups, 2 do
  local gid = groups[i]
  local gZ = ns .. ":g:" .. gid
  local lockKey = ns .. ":lock:" .. gid

  -- Skip locked groups
  local lockTtl = redis.call("PTTL", lockKey)
  if lockTtl == -2 or lockTtl == -1 then
    -- Pop head job
    local zpop = redis.call("ZPOPMIN", gZ, 1)
    if zpop and #zpop > 0 then
      local headJobId = zpop[1]

      local jobKey = ns .. ":job:" .. headJobId
      local job = redis.call("HMGET", jobKey, "id","groupId","data","attempts","maxAttempts","seq","timestamp","orderMs","score")
      local id, groupId, payload, attempts, maxAttempts, seq, enq, orderMs, score = job[1], job[2], job[3], job[4], job[5], job[6], job[7], job[8], job[9]

      -- Respect orderingDelayMs
      local shouldSkip = false
      if orderingDelayMs > 0 and orderMs then
        local jobOrderMs = tonumber(orderMs)
        if jobOrderMs then
          local eligibleAt = jobOrderMs > now and jobOrderMs or (jobOrderMs + orderingDelayMs)
          if eligibleAt > now then
            -- Put back and set a short lock to avoid spin
            local putBackScore = tonumber(score)
            redis.call("ZADD", gZ, putBackScore, headJobId)
            -- Ensure group is present in ready with the correct score so it can be picked when eligible
            redis.call("ZADD", readyKey, putBackScore, gid)
            local remaining = eligibleAt - now
            redis.call("SET", lockKey, "ordering-delay", "PX", remaining)
            shouldSkip = true
          end
        end
      end

      if not shouldSkip then
        -- Lock group to this job
        redis.call("SET", lockKey, id, "PX", vt)

        local procKey = ns .. ":processing:" .. id
        local deadline = now + vt
        redis.call("HSET", procKey, "groupId", gid, "deadlineAt", tostring(deadline))
        redis.call("ZADD", processingKey, deadline, id)

        -- Re-add group if there is a new head
        local nextHead = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
        if nextHead and #nextHead >= 2 then
          local nextScore = tonumber(nextHead[2])
          redis.call("ZADD", readyKey, nextScore, gid)
        end

        table.insert(out, id .. "||DELIMITER||" .. groupId .. "||DELIMITER||" .. payload .. "||DELIMITER||" .. attempts .. "||DELIMITER||" .. maxAttempts .. "||DELIMITER||" .. seq .. "||DELIMITER||" .. enq .. "||DELIMITER||" .. orderMs .. "||DELIMITER||" .. score .. "||DELIMITER||" .. deadline)
        table.insert(processedGroups, gid)
      end
    end
  end
  -- Note: Locked groups remain in ready queue (from original ZRANGE)
  -- They'll be skipped by next reserve attempt
end

-- Remove only the groups that were actually processed from ready queue
for _, gid in ipairs(processedGroups) do
  redis.call("ZREM", readyKey, gid)
end

return out


