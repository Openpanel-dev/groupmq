local ns = KEYS[1]
local delayedKey = ns .. ":delayed"
return redis.call("ZCARD", delayedKey)


