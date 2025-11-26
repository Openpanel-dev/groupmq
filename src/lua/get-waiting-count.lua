local ns = KEYS[1]
local groupsKey = ns .. ":groups"
local groupIds = redis.call("SMEMBERS", groupsKey)
local total = 0
for _, gid in ipairs(groupIds) do
  local gk = ns .. ":g:" .. gid
  total = total + (redis.call("ZCARD", gk) or 0)
end
return total


