-- argv: ns
local ns = ARGV[1]
local groupsKey = ns .. ":groups"
local groupIds = redis.call("SMEMBERS", groupsKey)
local jobs = {}
for _, gid in ipairs(groupIds) do
  local gZ = ns .. ":g:" .. gid
  local groupJobs = redis.call("ZRANGE", gZ, 0, -1)
  for _, jobId in ipairs(groupJobs) do
    table.insert(jobs, jobId)
  end
end
return jobs


