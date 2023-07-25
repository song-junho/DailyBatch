from fastapi import FastAPI
import pymysql
from sqlalchemy import create_engine
import config

import redis

app = FastAPI()

# redis 생성
redis_client = redis.StrictRedis(host="127.0.0.1", port=6379, db=0, max_connections=4)
redis_client.flushdb()  # 초기화

# mysql db
user_nm = config.MYSQL_KEY["USER_NM"]
user_pw = config.MYSQL_KEY["USER_PW"]

host_nm = "127.0.0.1:3306"
engine = create_engine("mysql+pymysql://"+user_nm+":"+user_pw+"@"+host_nm, encoding="utf-8")

conn = engine.connect()
