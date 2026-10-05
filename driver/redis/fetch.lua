-- Claim up to n due jobs from one queue, walking priority levels in order.
-- KEYS: 1..N level zsets (ascending priority), N+1 lease zset, N+2 retrying zset
-- ARGV: 1 n, 2 owner, 3 lease_ms, 4 job key prefix, 5 queue key prefix

local want    = tonumber(ARGV[1])
local owner   = ARGV[2]
-- Due-ness is the storage's clock, matching the SQL drivers. Taking it from the
-- caller would let two servers with skewed clocks disagree about what is ready,
-- and the point of a shared queue is that they cannot.
local t       = redis.call('TIME')
local now     = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local expiry  = now + tonumber(ARGV[3])
local prefix  = ARGV[4]
local qpfx    = ARGV[5]
local retrying = KEYS[#KEYS]
local leases   = KEYS[#KEYS - 1]
local out      = {}

for level = 1, #KEYS - 2 do
  if #out >= want then break end

  -- Scored by due time, so this is both the ordering and the due filter. Taking
  -- them lowest-score-first is the ScheduledAt tiebreak within a priority level.
  local ids = redis.call('ZRANGEBYSCORE', KEYS[level], '-inf', now,
                         'LIMIT', 0, want - #out)

  for _, id in ipairs(ids) do
    -- ZREM is the claim. Its return value is the exclusivity guarantee: if a
    -- concurrent script already removed this id, we get 0 and skip it. Lua runs
    -- atomically, so no other client can observe the half-claimed state.
    if redis.call('ZREM', KEYS[level], id) == 1 then
      local key = prefix .. id
      redis.call('HSET', key, 'state', 'running', 'owner', owner,
                              'leased_until', expiry)
      redis.call('ZADD', leases, expiry, id)
      redis.call('ZREM', retrying, id)
      redis.call('RPUSH', qpfx .. 'hist:' .. id,
                 cjson.encode({state = 'running', at = now, reason = ''}))
      table.insert(out, redis.call('HGETALL', key))
    end
  end
end

return out
