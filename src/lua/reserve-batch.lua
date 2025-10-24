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
    -- Get head job (oldest) to check if eligible
    local head = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
    if head and #head >= 2 then
      local headJobId = head[1]
      local headScore = tonumber(head[2])
      
      -- Check if head job's timestamp is old enough for ordering
      local shouldSkip = false
      if orderingDelayMs > 0 and headScore then
        local elapsed = now - headScore
        if elapsed < orderingDelayMs then
          -- Head job too recent, defer this group
          -- Put group back at same position (it will be picked later)
          redis.call("ZADD", readyKey, headScore, gid)
          -- Set a short lock to avoid immediate re-picking
          redis.call("SET", lockKey, "deferred", "PX", orderingDelayMs - elapsed + 100)
          shouldSkip = true
        end
      end
      
      if not shouldSkip then
        -- Head job is eligible, pop it and process
        local zpop = redis.call("ZPOPMIN", gZ, 1)
        if zpop and #zpop > 0 then
          local jobId = zpop[1]
          
          local jobKey = ns .. ":job:" .. jobId
          local job = redis.call("HMGET", jobKey, "id","groupId","data","attempts","maxAttempts","seq","timestamp","orderMs","score")
          local id, groupId, payload, attempts, maxAttempts, seq, enq, orderMs, score = job[1], job[2], job[3], job[4], job[5], job[6], job[7], job[8], job[9]

          -- Lock group to this job
          redis.call("SET", lockKey, id, "PX", vt)

          local procKey = ns .. ":processing:" .. id
          local deadline = now + vt
          redis.call("HSET", procKey, "groupId", gid, "deadlineAt", tostring(deadline))
          redis.call("ZADD", processingKey, deadline, id)

          -- Re-add group if there is a new head job (next oldest)
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
  end
  -- Note: Locked groups remain in ready queue from original ZRANGE
  -- They'll be skipped by next reserve attempt
end

-- Remove only the groups that were actually processed from ready queue
for _, gid in ipairs(processedGroups) do
  redis.call("ZREM", readyKey, gid)
end

return out


