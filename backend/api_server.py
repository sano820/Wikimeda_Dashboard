import os
import json
from typing import Optional

import redis
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware

# Redis 설정
REDIS_HOST = os.getenv("REDIS_HOST", "redis")   # 로컬이면 localhost
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_KEY = os.getenv("REDIS_KEY", "dashboard:latest")

CORS_ORIGINS = os.getenv(
    "CORS_ORIGINS",
    "http://localhost:5173,http://localhost:3000"
).split(",")

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

app = FastAPI(title="Dashboard Backend API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in CORS_ORIGINS],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/api/dashboard/latest")
def latest_dashboard(response: Response):
    """
    Redis의 dashboard:latest(JSON string)를 읽어 그대로 반환
    """
    val: Optional[str] = r.get(REDIS_KEY)
    if not val:
        response.status_code = 204
        return None

    return json.loads(val)
