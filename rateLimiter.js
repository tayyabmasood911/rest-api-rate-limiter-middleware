'use strict';

/**
 * Enterprise Sliding Window Rate Limiter Middleware for Express / Node.js
 *
 * Implements the Redis Sorted Set (ZSET) Sliding Window Log algorithm via an atomic Lua script:
 * 1. ZREMRANGEBYSCORE: Evict request timestamps older than (now - windowMs)
 * 2. ZCARD: Count active requests remaining in the sliding window
 * 3. Conditional ZADD: If below limit, record current timestamp with unique member ID
 * 4. PEXPIRE: Set TTL on key to auto-garbage collect idle keys
 * 5. Returns: [allowed (0/1), currentCount, oldestTimestamp]
 *
 * Fully compliant with IETF / RFC 6585 rate limiting headers:
 * - X-RateLimit-Limit
 * - X-RateLimit-Remaining
 * - X-RateLimit-Reset
 * - Retry-After (on HTTP 429)
 *
 * Includes automatic thread-safe in-memory sliding window fallback when Redis is offline.
 */

const crypto = require('crypto');

// Redis Lua script for atomic sliding window evaluation
const SLIDING_WINDOW_LUA = `
local key = KEYS[1]
local now = tonumber(ARGV[1])
local windowMs = tonumber(ARGV[2])
local maxRequests = tonumber(ARGV[3])
local memberId = ARGV[4]

local clearBefore = now - windowMs
redis.call('ZREMRANGEBYSCORE', key, '-inf', clearBefore)

local currentRequests = redis.call('ZCARD', key)

if currentRequests < maxRequests then
    redis.call('ZADD', key, now, memberId)
    redis.call('PEXPIRE', key, windowMs)
    return {1, currentRequests + 1, now}
else
    local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
    local oldestTime = now
    if #oldest > 1 then
        oldestTime = tonumber(oldest[2])
    end
    return {0, currentRequests, oldestTime}
end
`;

// In-memory fallback map: Map<string, number[]>
const localMemoryStore = new Map();

function localSlidingWindowCheck(key, now, windowMs, maxRequests) {
  const clearBefore = now - windowMs;
  let timestamps = localMemoryStore.get(key) || [];
  timestamps = timestamps.filter(ts => ts > clearBefore);

  if (timestamps.length < maxRequests) {
    timestamps.push(now);
    localMemoryStore.set(key, timestamps);
    return { allowed: true, count: timestamps.length, oldest: timestamps[0] };
  } else {
    localMemoryStore.set(key, timestamps);
    return { allowed: false, count: timestamps.length, oldest: timestamps[0] };
  }
}

/**
 * Creates an Express rate limiter middleware
 * @param {Object} options Configuration parameters
 * @param {Object} [options.redisClient] ioredis or redis client instance
 * @param {number} [options.windowMs] Window duration in milliseconds (default: 60000)
 * @param {number} [options.maxRequests] Allowed request quota in window (default: 100)
 * @param {Function} [options.keyGenerator] Function extracting client key from req
 * @param {string} [options.prefix] Key prefix in Redis (default: 'ratelimit:')
 * @returns {Function} Express middleware function
 */
function createRateLimiter(options = {}) {
  const {
    redisClient = null,
    windowMs = 60000,
    maxRequests = 100,
    prefix = 'ratelimit:',
    keyGenerator = (req) => {
      return req.headers['x-forwarded-for'] ||
             req.socket?.remoteAddress ||
             req.ip ||
             '127.0.0.1';
    },
    message = {
      error: 'Too Many Requests',
      message: 'Rate limit quota exceeded. Please slow down.'
    }
  } = options;

  return async (req, res, next) => {
    const clientIdentifier = keyGenerator(req);
    const key = `${prefix}${clientIdentifier}`;
    const now = Date.now();
    const memberId = `${now}-${crypto.randomBytes(4).toString('hex')}`;

    let allowed = true;
    let currentCount = 0;
    let oldestTimestamp = now;

    if (redisClient && typeof redisClient.eval === 'function') {
      try {
        const result = await redisClient.eval(
          SLIDING_WINDOW_LUA,
          1,
          key,
          now.toString(),
          windowMs.toString(),
          maxRequests.toString(),
          memberId
        );
        allowed = result[0] === 1;
        currentCount = Number(result[1]);
        oldestTimestamp = Number(result[2]);
      } catch (redisErr) {
        console.warn('[RateLimiter] Redis call failed, engaging in-memory fallback:', redisErr.message);
        const local = localSlidingWindowCheck(key, now, windowMs, maxRequests);
        allowed = local.allowed;
        currentCount = local.count;
        oldestTimestamp = local.oldest;
      }
    } else {
      // Use local in-memory sliding window
      const local = localSlidingWindowCheck(key, now, windowMs, maxRequests);
      allowed = local.allowed;
      currentCount = local.count;
      oldestTimestamp = local.oldest;
    }

    const remaining = Math.max(0, maxRequests - currentCount);
    const resetTimeEpochSec = Math.ceil((oldestTimestamp + windowMs) / 1000);
    const retryAfterSec = Math.max(1, Math.ceil((oldestTimestamp + windowMs - now) / 1000));

    // Standard RFC RateLimit headers
    res.setHeader('X-RateLimit-Limit', maxRequests);
    res.setHeader('X-RateLimit-Remaining', remaining);
    res.setHeader('X-RateLimit-Reset', resetTimeEpochSec);

    if (!allowed) {
      res.setHeader('Retry-After', retryAfterSec);
      return res.status(429).json({
        ...message,
        retryAfter: retryAfterSec,
        resetTime: new Date(resetTimeEpochSec * 1000).toISOString()
      });
    }

    next();
  };
}

module.exports = createRateLimiter;
