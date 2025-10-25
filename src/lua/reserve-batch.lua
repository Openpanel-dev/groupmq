-- argv: ns, nowEpochMs, vtMs, maxBatch
local ns = ARGV[1]
local now = tonumber(ARGV[2])
local vt = tonumber(ARGV[3])
local maxBatch = tonumber(ARGV[4]) or 16

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

  -- Try to atomically acquire the group lock
  local head = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
  if head and #head >= 2 then
    local headJobId = head[1]
    local headScore = tonumber(head[2])
    
    -- Try to atomically acquire the lock using the job ID as the lock value
    local lockAcquired = redis.call("SET", lockKey, headJobId, "PX", vt, "NX")
    if lockAcquired then
      -- Successfully acquired lock, now get the job
      local zpop = redis.call("ZPOPMIN", gZ, 1)
      if zpop and #zpop > 0 then
        local jobId = zpop[1]
        
        local jobKey = ns .. ":job:" .. jobId
        local job = redis.call("HMGET", jobKey, "id","groupId","data","attempts","maxAttempts","seq","timestamp","orderMs","score")
        local id, groupId, payload, attempts, maxAttempts, seq, enq, orderMs, score = job[1], job[2], job[3], job[4], job[5], job[6], job[7], job[8], job[9]

        local procKey = ns .. ":processing:" .. id
        local deadline = now + vt
        redis.call("HSET", procKey, "groupId", gid, "deadlineAt", tostring(deadline))
        redis.call("ZADD", processingKey, deadline, id)

        -- No counter operations - use ZCARD for counts

        -- Re-add group if there is a new head job (next oldest)
        local nextHead = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
        if nextHead and #nextHead >= 2 then
          local nextScore = tonumber(nextHead[2])
          redis.call("ZADD", readyKey, nextScore, gid)
        end

        table.insert(out, id .. "||DELIMITER||" .. groupId .. "||DELIMITER||" .. payload .. "||DELIMITER||" .. attempts .. "||DELIMITER||" .. maxAttempts .. "||DELIMITER||" .. seq .. "||DELIMITER||" .. enq .. "||DELIMITER||" .. orderMs .. "||DELIMITER||" .. score .. "||DELIMITER||" .. deadline)
        table.insert(processedGroups, gid)
      else
        -- No job available, release the lock
        redis.call("DEL", lockKey)
      end
    end
  end
  -- Note: Groups that couldn't be locked will be skipped by next reserve attempt
end

-- Remove only the groups that were actually processed from ready queue
for _, gid in ipairs(processedGroups) do
  redis.call("ZREM", readyKey, gid)
end

return out


