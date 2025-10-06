-- argv: ns
local ns = ARGV[1]

-- Check processing jobs
local processingCount = redis.call("ZCARD", ns .. ":processing")
if processingCount > 0 then
  return 0
end

-- Check delayed jobs
local delayedCount = redis.call("ZCARD", ns .. ":delayed")
if delayedCount > 0 then
  return 0
end

-- Check ready groups (jobs waiting)
local readyCount = redis.call("ZCARD", ns .. ":ready")
if readyCount > 0 then
  return 0
end

-- Check buffering groups
local bufferingCount = redis.call("ZCARD", ns .. ":buffering")
if bufferingCount > 0 then
  return 0
end

-- Check all groups for waiting jobs
local groups = redis.call("SMEMBERS", ns .. ":groups")
for _, gid in ipairs(groups) do
  local gZ = ns .. ":g:" .. gid
  local jobCount = redis.call("ZCARD", gZ)
  if jobCount > 0 then
    return 0
  end
end

-- Queue is completely empty
return 1

