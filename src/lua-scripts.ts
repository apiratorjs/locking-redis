import { RedisScript } from "./redis-script";

// Returns {1} when acquired, {0, PTTL of the current lock} when it is held.
export const MUTEX_ACQUIRE_SCRIPT = new RedisScript(`
  local acquired
  if ARGV[2] == 'inf' then
    acquired = redis.call('set', KEYS[1], ARGV[1], 'NX')
  else
    acquired = redis.call('set', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2])
  end

  if acquired then
    return {1}
  end

  return {0, redis.call('pttl', KEYS[1])}
`);

/**
 * Only release if the lock key's value matches our lock token.
 */
export const MUTEX_RELEASE_SCRIPT = new RedisScript(`
  if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
  end
  return 0
`);

export const MUTEX_EXTEND_SCRIPT = new RedisScript(`
  if redis.call('get', KEYS[1]) ~= ARGV[1] then
    return 0
  end

  if ARGV[2] == 'inf' then
    redis.call('persist', KEYS[1])
  else
    redis.call('pexpire', KEYS[1], ARGV[2])
  end

  return 1
`);

// PTTL convention: -1 for no TTL, -2 when the token does not hold the lock.
export const MUTEX_REMAINING_TTL_SCRIPT = new RedisScript(`
  if redis.call('get', KEYS[1]) ~= ARGV[1] then
    return -2
  end

  return redis.call('pttl', KEYS[1])
`);

// Scores go back to Redis as the strings it returned: tostring() of a Lua number keeps only 14 digits.
const SEMAPHORE_SCRIPT_PRELUDE = `
  local time = redis.call('time')
  local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)

  local function refreshKeyExpiry(key)
    local last = redis.call('zrange', key, -1, -1, 'WITHSCORES')
    if #last == 0 then
      return
    end

    if last[2] == 'inf' then
      redis.call('persist', key)
    else
      redis.call('pexpireat', key, last[2])
    end
  end

  local function nextExpiryIn(key)
    local first = redis.call('zrange', key, 0, 0, 'WITHSCORES')
    if #first == 0 or first[2] == 'inf' then
      return -1
    end

    return math.max(0, tonumber(first[2]) - now)
  end

  local function isAlive(score)
    return score and (score == 'inf' or tonumber(score) > now)
  end

  local function scoreFor(ttl)
    if ttl == 'inf' then
      return '+inf'
    end

    return now + tonumber(ttl)
  end
`;

// Returns {1} when acquired, {0, ms until the next expiry or -1} when full.
export const SEMAPHORE_ACQUIRE_SCRIPT = new RedisScript(`${SEMAPHORE_SCRIPT_PRELUDE}
  redis.call('zremrangebyscore', KEYS[1], '-inf', now)

  if redis.call('zcard', KEYS[1]) < tonumber(ARGV[1]) then
    redis.call('zadd', KEYS[1], scoreFor(ARGV[3]), ARGV[2])
    refreshKeyExpiry(KEYS[1])
    return {1}
  end

  return {0, nextExpiryIn(KEYS[1])}
`);

export const SEMAPHORE_RELEASE_SCRIPT = new RedisScript(`${SEMAPHORE_SCRIPT_PRELUDE}
  local score = redis.call('zscore', KEYS[1], ARGV[1])
  if not score then
    return 0
  end

  redis.call('zrem', KEYS[1], ARGV[1])
  refreshKeyExpiry(KEYS[1])

  if isAlive(score) then
    return 1
  end

  return 0
`);

export const SEMAPHORE_EXTEND_SCRIPT = new RedisScript(`${SEMAPHORE_SCRIPT_PRELUDE}
  if not isAlive(redis.call('zscore', KEYS[1], ARGV[1])) then
    return 0
  end

  redis.call('zadd', KEYS[1], 'XX', scoreFor(ARGV[2]), ARGV[1])
  refreshKeyExpiry(KEYS[1])
  return 1
`);

// PTTL convention: -1 for no TTL, -2 for no live permit (Lua cannot return inf).
export const SEMAPHORE_REMAINING_TTL_SCRIPT = new RedisScript(`${SEMAPHORE_SCRIPT_PRELUDE}
  local score = redis.call('zscore', KEYS[1], ARGV[1])
  if not isAlive(score) then
    return -2
  end

  if score == 'inf' then
    return -1
  end

  return tonumber(score) - now
`);

export const SEMAPHORE_COUNT_SCRIPT = new RedisScript(`${SEMAPHORE_SCRIPT_PRELUDE}
  redis.call('zremrangebyscore', KEYS[1], '-inf', now)
  return {redis.call('zcard', KEYS[1]), nextExpiryIn(KEYS[1])}
`);
