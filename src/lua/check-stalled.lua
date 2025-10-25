-- Check for stalled jobs and move them back to waiting or fail them
-- KEYS: namespace, currentTime, gracePeriod, maxStalledCount
-- Returns: array of [jobId, groupId, action] for each stalled job found
--   action: "recovered" or "failed"

local ns = ARGV[1]
local now = tonumber(ARGV[2])
local gracePeriod = tonumber(ARGV[3]) or 0
local maxStalledCount = tonumber(ARGV[4]) or 1

-- Circuit breaker for high concurrency: limit stalled job recovery
-- to prevent excessive Redis load and race conditions
local circuitBreakerKey = ns .. ":stalled:circuit"
local lastCheck = redis.call("GET", circuitBreakerKey)
if lastCheck then
  local lastCheckTime = tonumber(lastCheck)
  -- More aggressive circuit breaker for high concurrency: 2 seconds instead of 1
  local circuitBreakerInterval = 2000
  if lastCheckTime and (now - lastCheckTime) < circuitBreakerInterval then
    -- Circuit breaker: only check stalled jobs once per 2 seconds
    return {}
  end
end
redis.call("SET", circuitBreakerKey, now, "PX", 3000)

local processingKey = ns .. ':processing'
local groupsKey = ns .. ':groups'
local stalledKey = ns .. ':stalled'

-- BullMQ-inspired: Two-phase stalled detection for better accuracy
-- Phase 1: Get potentially stalled jobs (jobs past their deadline)
local potentiallyStalled = redis.call("ZRANGEBYSCORE", processingKey, 0, now - gracePeriod, "LIMIT", 0, 100)
if not potentiallyStalled or #potentiallyStalled == 0 then
  return {}
end

local results = {}

-- Get all jobs in processing state
local processingJobs = redis.call('ZRANGEBYSCORE', processingKey, 0, now - gracePeriod, 'LIMIT', 0, 100)

for _, jobId in ipairs(processingJobs) do
  local jobKey = ns .. ':job:' .. jobId
  local jobData = redis.call('HMGET', jobKey, 'groupId', 'stalledCount', 'maxAttempts', 'attempts')
  
  if jobData[1] then
    local groupId = jobData[1]
    local stalledCount = tonumber(jobData[2]) or 0
    local maxAttempts = tonumber(jobData[3]) or 3
    local attempts = tonumber(jobData[4]) or 0
    
    -- Increment stalled count
    stalledCount = stalledCount + 1
    redis.call('HSET', jobKey, 'stalledCount', stalledCount)
    
    -- Check if we should fail the job
    if stalledCount >= maxStalledCount and maxStalledCount > 0 then
      -- Job has stalled too many times, move to failed
      -- Remove from processing
      redis.call('ZREM', processingKey, jobId)
      
      -- No counter operations - use ZCARD for counts
      
      -- Remove from group if it's there
      local groupKey = ns .. ':g:' .. groupId
      redis.call('ZREM', groupKey, jobId)
      
      -- Check if this group had a lock for this job and release it
      local lockKey = ns .. ':lock:' .. groupId
      local lockedJobId = redis.call('GET', lockKey)
      if lockedJobId == jobId then
        redis.call('DEL', lockKey)
      end
      
      -- Update job status
      redis.call('HSET', jobKey, 'status', 'failed', 'finishedOn', now, 
                'failedReason', 'Job stalled ' .. stalledCount .. ' times (max: ' .. maxStalledCount .. ')')
      
      -- Add to failed set
      local failedKey = ns .. ':failed'
      redis.call('ZADD', failedKey, now, jobId)
      
      -- Track that we failed this job
      table.insert(results, jobId)
      table.insert(results, groupId)
      table.insert(results, 'failed')
    else
      -- Recover the job: move back to waiting
      -- CRITICAL: First verify job is STILL in processing to avoid race conditions
      -- If job was completed between our snapshot and now, don't re-add it
      local stillInProcessing = redis.call('ZSCORE', processingKey, jobId)
      
      -- Additional safety: check if job status is still 'processing' or 'waiting'
      -- If it's 'completed' or 'failed', don't recover it
      local currentStatus = redis.call('HGET', jobKey, 'status')
      
      -- CRITICAL: For high concurrency, add extra safety checks
      -- Check if job was recently completed (within last 5 seconds)
      local finishedOn = redis.call('HGET', jobKey, 'finishedOn')
      local recentlyCompleted = false
      if finishedOn then
        local finishedTime = tonumber(finishedOn)
        if finishedTime and (now - finishedTime) < 5000 then
          recentlyCompleted = true
        end
      end
      
      if stillInProcessing and (currentStatus == 'processing' or currentStatus == 'waiting' or not currentStatus) and not recentlyCompleted then
        -- Job is confirmed to still be in processing, safe to recover
        redis.call('ZREM', processingKey, jobId)
        
        -- No counter operations - use ZCARD for counts
        
        -- Release group lock if this job holds it
        local lockKey = ns .. ':lock:' .. groupId
        local lockedJobId = redis.call('GET', lockKey)
        if lockedJobId == jobId then
          redis.call('DEL', lockKey)
        end
        
        -- Re-add to group's waiting queue using original score (not orderMs)
        -- This preserves FIFO ordering with sequence numbers
        local score = redis.call('HGET', jobKey, 'score')
        if score then
          local groupKey = ns .. ':g:' .. groupId
          redis.call('ZADD', groupKey, tonumber(score), jobId)
          
          -- Add group to ready queue with the head job's score
          local head = redis.call('ZRANGE', groupKey, 0, 0, 'WITHSCORES')
          if head and #head >= 2 then
            local headScore = tonumber(head[2])
            local readyKey = ns .. ':ready'
            redis.call('ZADD', readyKey, headScore, groupId)
          end
          
          -- Ensure group is in groups set
          redis.call('SADD', groupsKey, groupId)
        end
        
        -- Update job status
        redis.call('HSET', jobKey, 'status', 'waiting')
        
        -- Track that we recovered this job
        table.insert(results, jobId)
        table.insert(results, groupId)
        table.insert(results, 'recovered')
      end
      -- If not still in processing, it was completed - don't re-add it!
    end
  end
end

return results

