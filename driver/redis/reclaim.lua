-- Return jobs whose lease expired to the available set.
-- KEYS: 1 lease zset, 2 queue key prefix
-- ARGV: none

local leases = KEYS[1]
local pfx    = KEYS[2]
local t      = redis.call('TIME')
local now    = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local n      = 0

for _, id in ipairs(redis.call('ZRANGEBYSCORE', leases, '-inf', now)) do
  local key = pfx .. 'job:' .. id
  if redis.call('HGET', key, 'state') == 'running' then
    local level = tonumber(redis.call('HGET', key, 'priority')) or 0
    if level < 0 then level = 0 end
    if level > 9 then level = 9 end

    redis.call('HSET', key, 'state', 'available', 'owner', '', 'leased_until', '0')
    if tonumber(redis.call('HGET', key, 'attempt')) > 0 then
      redis.call('ZADD', pfx .. 'retrying', redis.call('HGET', key, 'scheduled_at'), id)
    end
    -- Back at its original due time, not now: a job that was already overdue
    -- when it was claimed stays overdue, and keeps its place in the ordering.
    redis.call('ZADD', pfx .. 'p' .. level, redis.call('HGET', key, 'scheduled_at'), id)
    n = n + 1
  end
  redis.call('ZREM', leases, id)
end

return n
