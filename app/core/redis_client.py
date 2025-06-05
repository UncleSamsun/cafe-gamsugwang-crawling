# app/core/redis_client.py

import redis
import os

# 실제 Redis 호스트/포트/DB 번호에 맞게 수정하세요.
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
_redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

def get_redis():
    """
    Redis client 객체를 반환합니다.
    """
    return _redis