-- argv: ns
local ns = ARGV[1]
local processingKey = ns .. ":processing"
return redis.call("ZRANGE", processingKey, 0, -1)


