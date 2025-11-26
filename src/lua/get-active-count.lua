-- argv: ns
local ns = KEYS[1]
local processingKey = ns .. ":processing"
return redis.call("ZCARD", processingKey)


