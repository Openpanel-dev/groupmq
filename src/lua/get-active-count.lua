-- argv: ns
local ns = ARGV[1]
local processingKey = ns .. ":processing"
return redis.call("ZCARD", processingKey)


