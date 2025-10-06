-- ACK job completion and promote next job from same group (v2 - expert's design)
-- KEYS: jobH, jobLeaseK, groupsInflightS, groupReadyL, workReadyL, stateActiveZ, stateCompletedZ, stateWaitingZ, signalChannel, dueEpochsZ
-- ARGV: gid, jobKey, nowMs
-- Returns: nextJobKey if promoted, nil if no more jobs

local gid = ARGV[1]
local jobKey = ARGV[2]
local nowMs = tonumber(ARGV[3])

-- Remove lease and mark job as completed
redis.call('DEL', KEYS[2])  -- job lease
redis.call('ZREM', KEYS[6], jobKey)  -- remove from active
redis.call('ZADD', KEYS[7], nowMs, jobKey)  -- add to completed
redis.call('HSET', KEYS[1], 'state', 'completed', 'completedAt', nowMs)

-- Release group from inflight
redis.call('SREM', KEYS[3], gid)

-- Try to promote next job from the same group
local nextJobId = redis.call('LPOP', KEYS[4])
if nextJobId then
  -- Construct full job key from job ID
  local nextJobKey = 'job:' .. gid .. ':' .. nextJobId
  
  -- Mark group as inflight again
  redis.call('SADD', KEYS[3], gid)
  
  -- Move from waiting to active state
  redis.call('ZREM', KEYS[8], nextJobKey)  -- remove from waiting (state:waiting)
  redis.call('ZADD', KEYS[6], nowMs + 30000, nextJobKey)  -- 30s lease
  
  -- Add to global work queue
  redis.call('RPUSH', KEYS[5], nextJobKey)
  
  -- Send wake-up signal
  redis.call('PUBLISH', KEYS[9], '1')
  
  return nextJobKey
end

-- Opportunistic epoch flush: if work:ready is empty, check for due epochs
local workReadyLen = redis.call('LLEN', KEYS[5])
if workReadyLen == 0 then
  -- Check for any due epochs (limited to 2 groups to keep ACK fast)
  local dueEpochs = redis.call('ZRANGEBYSCORE', KEYS[10], 0, nowMs, 'LIMIT', 0, 2)
  for i = 1, #dueEpochs do
    local dueGid = dueEpochs[i]
    -- Check if this group is not inflight and has jobs ready
    local isInflight = redis.call('SISMEMBER', KEYS[3], dueGid)
    if isInflight == 0 then
      local groupReadyKey = 'group:' .. dueGid .. ':ready'
      local groupReadyLen = redis.call('LLEN', groupReadyKey)
      if groupReadyLen > 0 then
        -- Promote one job from this group
        local nextJobKey = redis.call('LPOP', groupReadyKey)
        if nextJobKey then
          redis.call('SADD', KEYS[3], dueGid)  -- mark as inflight
          redis.call('ZREM', KEYS[8], nextJobKey)  -- remove from waiting
          redis.call('ZADD', KEYS[6], nowMs + 30000, nextJobKey)  -- add to active
          redis.call('RPUSH', KEYS[5], nextJobKey)  -- add to work queue
          redis.call('PUBLISH', KEYS[9], '1')  -- send wake signal
          break  -- Only promote one job to keep ACK fast
        end
      end
    end
  end
end

return nil
