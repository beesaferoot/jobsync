-- Apply a result to a job this owner holds.
-- KEYS: 1 job hash, 2 lease zset, 3 retrying zset, 4 queue key prefix
-- ARGV: 1 owner, 2 state, 3 err, 4 retry_at_ms, 5 retention_ms, 6 id

local key   = KEYS[1]
local pfx   = KEYS[4]
local owner = ARGV[1]
local state = ARGV[2]

-- Not ours, or already finished. The lease was reclaimed and the job may be
-- running elsewhere, so this result is about a superseded attempt: drop it.
-- Silently, because an error here would make every reclaimed job log forever.
if redis.call('HGET', key, 'owner') ~= owner then return 0 end
if redis.call('HGET', key, 'state') ~= 'running' then return 0 end

redis.call('ZREM', KEYS[2], ARGV[6])

local t   = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

redis.call('RPUSH', pfx .. 'hist:' .. ARGV[6],
           cjson.encode({state = state, at = now, reason = ARGV[3]}))
redis.call('PEXPIRE', pfx .. 'hist:' .. ARGV[6], ARGV[5])

if state == 'retrying' then
  local attempt = tonumber(redis.call('HGET', key, 'attempt')) + 1
  local level   = tonumber(redis.call('HGET', key, 'priority'))
  if level < 0 then level = 0 end
  if level > 9 then level = 9 end

  redis.call('HSET', key, 'state', 'available', 'attempt', attempt,
             'scheduled_at', ARGV[4], 'last_error', ARGV[3],
             'owner', '', 'leased_until', '0')
  redis.call('ZADD', pfx .. 'p' .. level, ARGV[4], ARGV[6])
  -- Indexed so Counts can tell a retry apart from a first-time scheduled job
  -- without reading every hash.
  redis.call('ZADD', KEYS[3], ARGV[4], ARGV[6])
  return 1
end

-- Terminal. Release the unique key, or tomorrow's run of a daily job is blocked
-- by yesterday's success.
local unique = redis.call('HGET', key, 'unique_key')
if unique and unique ~= '' then
  redis.call('DEL', pfx .. 'unique:' .. unique)
end

redis.call('HSET', key, 'state', state, 'last_error', ARGV[3],
           'owner', '', 'leased_until', '0', 'unique_key', '', 'finished_at', now)

-- Scored by finish time, which is exactly what Throughput buckets on.
redis.call('ZADD', pfx .. 'done:' .. state, now, ARGV[6])

-- Retention is a TTL rather than a janitor: Redis expires the hash on its own,
-- so unlike the SQL drivers there is no sweep to schedule.
redis.call('PEXPIRE', key, ARGV[5])
return 1
