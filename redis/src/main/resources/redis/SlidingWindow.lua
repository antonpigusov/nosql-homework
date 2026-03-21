local label = KEYS[1]

local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local request_id = ARGV[4]

redis.call("ZREMRANGEBYSCORE", label, 0, now - window)

local current = redis.call("ZCARD", label)

if current < limit then
    redis.call("ZADD", label, now, request_id)
    redis.call("EXPIRE", label, math.ceil(window / 1000))
    return 1
else
    return 0
end
