"""Production Sliding Window Rate Limiter for FastAPI & Starlette

Implements high-performance atomic sliding window rate limiting:
- Atomic evaluation via Redis Sorted Sets (ZSET) Lua script
- Automatic fallback to high-throughput in-memory sliding window deque
- Standard RFC 6585 headers: X-RateLimit-Limit, X-RateLimit-Remaining, X-RateLimit-Reset, Retry-After
- Dependency injection friendly for FastAPI endpoint routes
"""

import math
import os
import time
import uuid
from collections import defaultdict, deque
from threading import Lock
from typing import Callable, Optional
from fastapi import HTTPException, Request, Response, status

try:
    import redis.asyncio as aioredis
except ImportError:
    aioredis = None

# Redis Lua Script for Atomic Sliding Window
SLIDING_WINDOW_LUA = """
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
"""

# Local thread-safe in-memory sliding window fallback
_local_lock = Lock()
_local_memory_store: dict[str, deque[float]] = defaultdict(deque)


def get_remote_ip(request: Request) -> str:
    """Extracts client IP prioritizing forward headers."""
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "127.0.0.1"


class RateLimiter:
    """FastAPI route dependency for sliding window rate limiting."""

    def __init__(
        self,
        times: int = 60,
        seconds: int = 60,
        key_func: Callable[[Request], str] = get_remote_ip,
        prefix: str = "ratelimit:fastapi:"
    ):
        self.times = times
        self.window_ms = seconds * 1000
        self.seconds = seconds
        self.key_func = key_func
        self.prefix = prefix
        self.redis_client = None

    async def get_redis(self):
        if self.redis_client is None and aioredis:
            redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
            try:
                self.redis_client = aioredis.from_url(redis_url, encoding="utf-8", decode_responses=True)
            except Exception:
                self.redis_client = None
        return self.redis_client

    def _local_check(self, key: str, now: float) -> tuple[bool, int, float]:
        window_sec = self.seconds
        threshold = now - window_sec
        with _local_lock:
            q = _local_memory_store[key]
            while q and q[0] <= threshold:
                q.popleft()
            if len(q) < self.times:
                q.append(now)
                return True, len(q), q[0]
            else:
                return False, len(q), q[0]

    async def __call__(self, request: Request, response: Response):
        client_key = self.key_func(request)
        storage_key = f"{self.prefix}{client_key}"
        now_sec = time.time()
        now_ms = int(now_sec * 1000)
        member_id = f"{now_ms}-{uuid.uuid4().hex[:8]}"

        redis = await self.get_redis()
        allowed = True
        current_count = 0
        oldest_ts_ms = now_ms

        if redis:
            try:
                res = await redis.eval(
                    SLIDING_WINDOW_LUA,
                    1,
                    storage_key,
                    str(now_ms),
                    str(self.window_ms),
                    str(self.times),
                    member_id
                )
                allowed = res[0] == 1
                current_count = int(res[1])
                oldest_ts_ms = int(res[2])
            except Exception:
                # Fallback to local
                allowed, current_count, oldest_sec = self._local_check(storage_key, now_sec)
                oldest_ts_ms = int(oldest_sec * 1000)
        else:
            allowed, current_count, oldest_sec = self._local_check(storage_key, now_sec)
            oldest_ts_ms = int(oldest_sec * 1000)

        remaining = max(0, self.times - current_count)
        reset_time_epoch = math.ceil((oldest_ts_ms + self.window_ms) / 1000)
        retry_after = max(1, math.ceil((oldest_ts_ms + self.window_ms - now_ms) / 1000))

        # Attach rate limiting headers to outgoing response
        response.headers["X-RateLimit-Limit"] = str(self.times)
        response.headers["X-RateLimit-Remaining"] = str(remaining)
        response.headers["X-RateLimit-Reset"] = str(reset_time_epoch)

        if not allowed:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Rate limit quota exceeded. Please slow down.",
                headers={
                    "X-RateLimit-Limit": str(self.times),
                    "X-RateLimit-Remaining": "0",
                    "X-RateLimit-Reset": str(reset_time_epoch),
                    "Retry-After": str(retry_after)
                }
            )

        return True
