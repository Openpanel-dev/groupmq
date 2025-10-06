-- Get due epochs (score <= nowMs) with their scores
local nowMs = tonumber(ARGV[1])
local limit = tonumber(ARGV[2]) or 10

-- Get due epochs (score <= nowMs) with their scores
local dueEpochsWithScores = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', nowMs, 'WITHSCORES', 'LIMIT', 0, limit)

local dueEpochs = {}
-- Extract just the group IDs (every other element)
for i = 1, #dueEpochsWithScores, 2 do
  table.insert(dueEpochs, dueEpochsWithScores[i])
end

-- Don't remove epochs here - they should be removed after successful flushing
-- if #dueEpochs > 0 then
--   redis.call('ZREM', KEYS[1], unpack(dueEpochs))
-- end

return dueEpochs
