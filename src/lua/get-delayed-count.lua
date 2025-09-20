local ns = ARGV[1]
local delayedKey = ns .. ":delayed"
return redis.call("ZCARD", delayedKey)


