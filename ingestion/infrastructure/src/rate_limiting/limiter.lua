-- limiter.lua
--
-- Redis-based distributed rate limiter using a sliding window algorithm.
--
-- Keys:
-- KEYS[1] = requests_zset_window_1 (e.g., 10m)
-- KEYS[2] = requests_zset_window_2 (e.g., 1m)
-- KEYS[3] = requests_zset_window_3 (e.g., 1s)
-- ... (can be extended)
--
-- Args:
-- ARGV[1] = limit_window_1
-- ARGV[2] = duration_secs_window_1
-- ARGV[3] = limit_window_2
-- ARGV[4] = duration_secs_window_2
-- ARGV[5] = limit_window_3
-- ARGV[6] = duration_secs_window_3
-- ...
-- ARGV[N] = unique_request_id

-- Get Redis server time for a consistent clock source. This is the single source of truth.
local redis_time = redis.call('TIME')
local now_micros = (redis_time[1] * 1000000) + redis_time[2]
local now_millis = math.floor(now_micros / 1000)

local request_id = ARGV[#ARGV]
local score = now_millis

-- Iterate through each window (key, limit, duration)
for i = 1, #KEYS do
    local key = KEYS[i]
    local limit = tonumber(ARGV[(i - 1) * 2 + 1])
    local duration_secs = tonumber(ARGV[(i - 1) * 2 + 2])
    local duration_millis = duration_secs * 1000

    local min_score = now_millis - duration_millis
    redis.call('ZREMRANGEBYSCORE', key, '-inf', min_score)

    local current_count = redis.call('ZCARD', key)
    if current_count >= limit then
        return 0 -- Denied
    end
end

for i = 1, #KEYS do
    local key = KEYS[i]
    local duration_secs = tonumber(ARGV[(i - 1) * 2 + 2])
    redis.call('ZADD', key, score, request_id)
    -- Set an expiration on the key itself to garbage collect old sets
    redis.call('EXPIRE', key, duration_secs + 5)
end

return 1 -- Allowed
