-- argv: ns, nowEpochMs, vtMs, scanLimit
local ns = KEYS[1]
local now = tonumber(ARGV[1])
local vt = tonumber(ARGV[2])
local scanLimit = tonumber(ARGV[3]) or 20

local readyKey = ns .. ":ready"

-- Respect paused state
if redis.call("GET", ns .. ":paused") then
  return nil
end

-- STALLED JOB RECOVERY WITH THROTTLING
-- Check for stalled jobs periodically to avoid overhead in hot path
-- This ensures stalled jobs are recovered even in high-load systems
-- Check interval is adaptive: 1/4 of jobTimeout (to check 4x during visibility window), max 5s
local processingKey = ns .. ":processing"
local stalledCheckKey = ns .. ":stalled:lastcheck"
local lastCheck = tonumber(redis.call("GET", stalledCheckKey)) or 0
local stalledCheckInterval = math.min(math.floor(vt / 4), 5000)

local shouldCheckStalled = (now - lastCheck) >= stalledCheckInterval

-- Get available groups
local groups = redis.call("ZRANGE", readyKey, 0, scanLimit - 1, "WITHSCORES")

-- Check for stalled jobs if: queue is empty OR it's time for periodic check
if (not groups or #groups == 0) or shouldCheckStalled then
  if shouldCheckStalled then
    redis.call("SET", stalledCheckKey, tostring(now))
  end
  
  local expiredJobs = redis.call("ZRANGEBYSCORE", processingKey, 0, now)
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
  
  -- Refresh groups after recovery (only if we didn't have any before)
  if not groups or #groups == 0 then
    groups = redis.call("ZRANGE", readyKey, 0, scanLimit - 1, "WITHSCORES")
  end
end

if not groups or #groups == 0 then
  return nil
end

local chosenGid = nil
local chosenIndex = nil
local headJobId = nil
local job = nil

-- Try to atomically acquire a group and its head job
-- BullMQ-style: use per-group active list instead of group locks
for i = 1, #groups, 2 do
  local gid = groups[i]
  local gZ = ns .. ":g:" .. gid
  local groupActiveKey = ns .. ":g:" .. gid .. ":active"
  
  -- Check if group has no active jobs (BullMQ-style gating)
  local activeCount = redis.call("LLEN", groupActiveKey)
  if activeCount == 0 then
    -- Check if group has jobs
    local head = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
    if head and #head >= 2 then
      local headJobId = head[1]
      local headJobKey = ns .. ":job:" .. headJobId
      
      -- Skip if head job is delayed (will be promoted later)
      local jobStatus = redis.call("HGET", headJobKey, "status")
      if jobStatus ~= "delayed" then
        -- Pop the job and push to active list atomically
        local zpop = redis.call("ZPOPMIN", gZ, 1)
        if zpop and #zpop > 0 then
          headJobId = zpop[1]
          -- Read the popped job (use headJobId to avoid races)
          headJobKey = ns .. ":job:" .. headJobId
          job = redis.call("HMGET", headJobKey, "id","groupId","data","attempts","maxAttempts","seq","timestamp","orderMs","score")
          
          -- Push to group active list (enforces 1-per-group)
          redis.call("LPUSH", groupActiveKey, headJobId)
          
          chosenGid = gid
          chosenIndex = (i + 1) / 2 - 1
          -- Mark job as processing for accurate stalled detection and idempotency
          redis.call("HSET", headJobKey, "status", "processing")
          break
        end
      end
    end
  end
end

if not chosenGid or not job then
  return nil
end

local id, groupId, payload, attempts, maxAttempts, seq, enq, orderMs, score = job[1], job[2], job[3], job[4], job[5], job[6], job[7], job[8], job[9]

-- Validate job data exists (handle corrupted/missing job hash)
if not id or id == false then
  -- Job hash is missing/corrupted, clean up group active list
  local groupActiveKey = ns .. ":g:" .. chosenGid .. ":active"
  redis.call("LREM", groupActiveKey, 1, headJobId)
  
  -- Re-add next job to ready queue if exists
  local gZ = ns .. ":g:" .. chosenGid
  local nextHead = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
  if nextHead and #nextHead >= 2 then
    local nextScore = tonumber(nextHead[2])
    redis.call("ZADD", readyKey, nextScore, chosenGid)
  end
  
  return nil
end

-- Remove the group from ready queue
redis.call("ZREMRANGEBYRANK", readyKey, chosenIndex, chosenIndex)

local procKey = ns .. ":processing:" .. id
local deadline = now + vt
redis.call("HSET", procKey, "groupId", chosenGid, "deadlineAt", tostring(deadline))

local processingKey2 = ns .. ":processing"
redis.call("ZADD", processingKey2, deadline, id)

local gZ = ns .. ":g:" .. chosenGid
local nextHead = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
if nextHead and #nextHead >= 2 then
  local nextScore = tonumber(nextHead[2])
  redis.call("ZADD", readyKey, nextScore, chosenGid)
end

return id .. "|||" .. groupId .. "|||" .. payload .. "|||" .. attempts .. "|||" .. maxAttempts .. "|||" .. seq .. "|||" .. enq .. "|||" .. orderMs .. "|||" .. score .. "|||" .. deadline


