-- argv: ns, nowEpochMs, vtMs, maxBatch
local ns = KEYS[1]
local now = tonumber(ARGV[1])
local vt = tonumber(ARGV[2])
local maxBatch = tonumber(ARGV[3]) or 16

local readyKey = ns .. ":ready"
local processingKey = ns .. ":processing"

-- Early exit if paused
if redis.call("GET", ns .. ":paused") then
  return {}
end

local out = {}

-- STALLED JOB RECOVERY WITH THROTTLING
-- Check for stalled jobs periodically to avoid overhead in hot path
-- This ensures stalled jobs are recovered even in high-load systems where ready queue is never empty
-- Check interval is adaptive: 1/4 of jobTimeout (to check 4x during visibility window), max 5s
local stalledCheckKey = ns .. ":stalled:lastcheck"
local lastCheck = tonumber(redis.call("GET", stalledCheckKey)) or 0
local stalledCheckInterval = math.min(math.floor(vt / 4), 5000)

if (now - lastCheck) >= stalledCheckInterval then
  -- Update last check timestamp
  redis.call("SET", stalledCheckKey, tostring(now))
  
  -- Check for expired jobs and recover them
  local expiredJobs = redis.call("ZRANGEBYSCORE", processingKey, 0, now)
  if #expiredJobs > 0 then
    for _, jobId in ipairs(expiredJobs) do
      local procKey = ns .. ":processing:" .. jobId
      local procData = redis.call("HMGET", procKey, "groupId", "deadlineAt")
      local gid = procData[1]
      local deadlineAt = tonumber(procData[2])
      if gid and deadlineAt and now > deadlineAt then
        local jobKey = ns .. ":job:" .. jobId
        local jobScore = redis.call("HGET", jobKey, "score")
        if jobScore then
          local gZ = ns .. ":g:" .. gid
          redis.call("ZADD", gZ, tonumber(jobScore), jobId)
          local head = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
          if head and #head >= 2 then
            local headScore = tonumber(head[2])
            redis.call("ZADD", readyKey, headScore, gid)
          end
          redis.call("DEL", ns .. ":lock:" .. gid)
          redis.call("DEL", procKey)
          redis.call("ZREM", processingKey, jobId)
        end
      end
    end
  end
end

-- Pop up to maxBatch groups from ready set (lowest score first)
local groups = redis.call("ZRANGE", readyKey, 0, maxBatch - 1, "WITHSCORES")
if not groups or #groups == 0 then
  return {}
end

local processedGroups = {}
-- BullMQ-style: use per-group active list instead of group locks
for i = 1, #groups, 2 do
  local gid = groups[i]
  local gZ = ns .. ":g:" .. gid
  local groupActiveKey = ns .. ":g:" .. gid .. ":active"

  -- Check if group has no active jobs (BullMQ-style gating)
  local activeCount = redis.call("LLEN", groupActiveKey)
  if activeCount == 0 then
    local head = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
    if head and #head >= 2 then
      local headJobId = head[1]
      local headScore = tonumber(head[2])
      local headJobKey = ns .. ":job:" .. headJobId
      
      -- Skip if head job is delayed (will be promoted later)
      local jobStatus = redis.call("HGET", headJobKey, "status")
      if jobStatus ~= "delayed" then
        -- Pop the job and push to active list atomically
        local zpop = redis.call("ZPOPMIN", gZ, 1)
        if zpop and #zpop > 0 then
          local jobId = zpop[1]
          
          local jobKey = ns .. ":job:" .. jobId
          local job = redis.call("HMGET", jobKey, "id","groupId","data","attempts","maxAttempts","seq","timestamp","orderMs","score")
          local id, groupId, payload, attempts, maxAttempts, seq, enq, orderMs, score = job[1], job[2], job[3], job[4], job[5], job[6], job[7], job[8], job[9]

          -- Validate job data exists (handle corrupted/missing job hash)
          if not id or id == false then
            -- Job hash is missing/corrupted, skip this job and continue
            -- Re-add next job to ready queue if exists
            local nextHead = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
            if nextHead and #nextHead >= 2 then
              local nextScore = tonumber(nextHead[2])
              redis.call("ZADD", readyKey, nextScore, gid)
            end
          else
            -- Push to group active list (enforces 1-per-group)
            redis.call("LPUSH", groupActiveKey, jobId)
            
            -- Mark job as processing
            redis.call("HSET", jobKey, "status", "processing")
            
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

            table.insert(out, id .. "|||" .. groupId .. "|||" .. payload .. "|||" .. attempts .. "|||" .. maxAttempts .. "|||" .. seq .. "|||" .. enq .. "|||" .. orderMs .. "|||" .. score .. "|||" .. deadline)
            table.insert(processedGroups, gid)
          end
        end
      end
    end
  end
  -- Note: Groups with active jobs will be skipped
end

-- Remove only the groups that were actually processed from ready queue
for _, gid in ipairs(processedGroups) do
  redis.call("ZREM", readyKey, gid)
end

return out


