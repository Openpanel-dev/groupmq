-- Atomic reserve operation that checks lock and reserves in one operation
-- argv: ns, nowEpochMs, vtMs, targetGroupId, allowedJobId (optional)
local ns = KEYS[1]
local now = tonumber(ARGV[1])
local vt = tonumber(ARGV[2])
local targetGroupId = ARGV[3]
local allowedJobId = ARGV[4] -- If provided, allow reserve if lock matches this job ID

local readyKey = ns .. ":ready"
local gZ = ns .. ":g:" .. targetGroupId
local groupActiveKey = ns .. ":g:" .. targetGroupId .. ":active"

-- Respect paused state
if redis.call("GET", ns .. ":paused") then
  return nil
end

-- BullMQ-style: Check if group has active jobs
local activeCount = redis.call("LLEN", groupActiveKey)

if activeCount > 0 then
  -- If allowedJobId is provided, check if it matches the active job (grace collection)
  if allowedJobId then
    local activeJobId = redis.call("LINDEX", groupActiveKey, 0)
    if activeJobId == allowedJobId then
      -- This is grace collection - we're chaining from the same job
      -- Continue to reserve next job
    else
      -- Different job is active, re-add to ready and return
      local head = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
      if head and #head >= 2 then
        local headScore = tonumber(head[2])
        redis.call("ZADD", readyKey, headScore, targetGroupId)
      end
      return nil
    end
  else
    -- Group has active job and this isn't grace collection, can't proceed
    local head = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
    if head and #head >= 2 then
      local headScore = tonumber(head[2])
      redis.call("ZADD", readyKey, headScore, targetGroupId)
    end
    return nil
  end
end

-- Try to get a job from the group
-- First check if head job is delayed
local head = redis.call("ZRANGE", gZ, 0, 0)
if not head or #head == 0 then
  return nil
end
local headJobId = head[1]
local jobKey = ns .. ":job:" .. headJobId

-- Skip if head job is delayed (will be promoted later)
local jobStatus = redis.call("HGET", jobKey, "status")
if jobStatus == "delayed" then
  return nil
end

-- Pop the job
local zpop = redis.call("ZPOPMIN", gZ, 1)
if not zpop or #zpop == 0 then
  -- No job available, return
  return nil
end
headJobId = zpop[1]

local job = redis.call("HMGET", jobKey, "id","groupId","data","attempts","maxAttempts","seq","timestamp","orderMs","score")
local id, groupId, payload, attempts, maxAttempts, seq, enq, orderMs, score = job[1], job[2], job[3], job[4], job[5], job[6], job[7], job[8], job[9]

-- Validate job data exists (handle corrupted/missing job hash)
if not id or id == false then
  -- Job hash is missing/corrupted, clean up if needed
  if not allowedJobId or activeCount == 0 then
    redis.call("LREM", groupActiveKey, 1, headJobId)
  end
  
  -- Re-add next job to ready queue if exists
  local nextHead = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
  if nextHead and #nextHead >= 2 then
    local nextScore = tonumber(nextHead[2])
    redis.call("ZADD", readyKey, nextScore, targetGroupId)
  end
  
  return nil
end

-- BullMQ-style: Push to group active list if not already there (not grace collection)
if not allowedJobId or activeCount == 0 then
  -- Normal reserve: add this job to active list
  redis.call("LPUSH", groupActiveKey, id)
end
-- If this is grace collection and activeCount > 0, the active list already has the job

local procKey = ns .. ":processing:" .. id
local deadline = now + vt
redis.call("HSET", procKey, "groupId", groupId, "deadlineAt", tostring(deadline))

local processingKey = ns .. ":processing"
redis.call("ZADD", processingKey, deadline, id)

-- Mark job as processing for accurate stalled detection and idempotency
redis.call("HSET", jobKey, "status", "processing")

local nextHead = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
if nextHead and #nextHead >= 2 then
  local nextScore = tonumber(nextHead[2])
  redis.call("ZADD", readyKey, nextScore, groupId)
end

return id .. "|||" .. groupId .. "|||" .. payload .. "|||" .. attempts .. "|||" .. maxAttempts .. "|||" .. seq .. "|||" .. enq .. "|||" .. orderMs .. "|||" .. score .. "|||" .. deadline
