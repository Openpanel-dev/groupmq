-- argv: ns
local ns = ARGV[1]
local groupsKey = ns .. ":groups"
local groupIds = redis.call("SMEMBERS", groupsKey)
local count = 0
for _, groupId in ipairs(groupIds) do
  local gZ = ns .. ":g:" .. groupId
  local jobCount = redis.call("ZCARD", gZ)
  if jobCount > 0 then
    count = count + 1
  end
end
return count