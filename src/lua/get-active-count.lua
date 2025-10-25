-- argv: ns
local ns = ARGV[1]
local activeCountKey = ns .. ":count:active"
local count = redis.call("GET", activeCountKey)
return tonumber(count) or 0


