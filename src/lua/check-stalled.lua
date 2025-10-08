-- Check for stalled jobs and move them back to waiting or fail them
-- KEYS: namespace, currentTime, gracePeriod, maxStalledCount
-- Returns: array of [jobId, groupId, action] for each stalled job found
--   action: "recovered" or "failed"

local ns = ARGV[1]
local now = tonumber(ARGV[2])
local gracePeriod = tonumber(ARGV[3]) or 0
local maxStalledCount = tonumber(ARGV[4]) or 1

local processingKey = ns .. ':processing'
local groupsKey = ns .. ':groups'
local stalledKey = ns .. ':stalled'

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
      -- Remove from processing
      redis.call('ZREM', processingKey, jobId)
      
      -- Release group lock if this job holds it
      local lockKey = ns .. ':lock:' .. groupId
      local lockedJobId = redis.call('GET', lockKey)
      if lockedJobId == jobId then
        redis.call('DEL', lockKey)
      end
      
      -- Re-add to group's waiting queue (use orderMs as score)
      local orderMs = redis.call('HGET', jobKey, 'orderMs')
      if orderMs then
        local groupKey = ns .. ':g:' .. groupId
        redis.call('ZADD', groupKey, tonumber(orderMs), jobId)
        
        -- Add group to ready queue
        local readyKey = ns .. ':ready'
        redis.call('ZADD', readyKey, tonumber(orderMs), groupId)
        
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
  end
end

return results

