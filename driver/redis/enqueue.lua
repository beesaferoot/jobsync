-- Enqueue one job.
-- KEYS: 1 job hash, 2 priority level zset, 3 queues set, 4 unique key ('' if none)
-- ARGV: 1 id, 2 kind, 3 queue, 4 payload, 5 priority, 6 attempt, 7 max_attempts,
--       8 unique_key, 9 tags, 10 created_at_ms, 11 scheduled_at_ms

-- A job whose id already exists is ignored. HSET below would otherwise
-- overwrite it, resurrecting a finished job as a fresh one and running the work
-- twice — which is exactly what the scheduler's deterministic ids rely on not
-- happening.
if redis.call('EXISTS', KEYS[1]) == 1 then
  return 0
end

-- A duplicate unique key is dropped silently: the caller asked for the work to
-- happen once, and it is going to. SET NX is the claim; it is released by
-- finish.lua when the job reaches a terminal state.
if KEYS[4] ~= '' then
  if redis.call('SET', KEYS[4], ARGV[1], 'NX') == false then
    return 0
  end
end

redis.call('HSET', KEYS[1],
  'id', ARGV[1], 'kind', ARGV[2], 'queue', ARGV[3], 'payload', ARGV[4],
  'priority', ARGV[5], 'attempt', ARGV[6], 'max_attempts', ARGV[7],
  'unique_key', ARGV[8], 'tags', ARGV[9],
  'created_at', ARGV[10], 'scheduled_at', ARGV[11],
  'state', 'available', 'owner', '', 'last_error', '', 'leased_until', '0')

-- Score is the due time, so ZRANGEBYSCORE -inf now is exactly "what is due".
redis.call('ZADD', KEYS[2], ARGV[11], ARGV[1])
redis.call('SADD', KEYS[3], ARGV[3])

-- {q}:all is the dashboard's index, scored by creation so a page is a ZREVRANGE
-- rather than a SCAN over the keyspace.
redis.call('ZADD', KEYS[5], ARGV[10], ARGV[1])
return 1
