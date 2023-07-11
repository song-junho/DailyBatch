from fastapi import FastAPI

import redis

app = FastAPI()

# redis 생성
redis_client = redis.StrictRedis(host="127.0.0.1", port=6379, db=0, max_connections=4)
redis_client.flushdb()  # 초기화
