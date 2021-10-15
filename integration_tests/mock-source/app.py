
import os
import random
import asyncio
import hashlib
from fastapi import FastAPI, Response

CACHE_URL = os.getenv("CACHE_URL","http://mock-cache")
RETRIEVAL_TIME = int(os.getenv("RETRIEVAL_TIME", "1"))
RETRIEVAL_NOISE = int(os.getenv("RETRIEVAL_NOISE", "0"))

app = FastAPI()

@app.get("/{path:path}")
async def return_something(path: str):
    sleep_time = RETRIEVAL_TIME - (RETRIEVAL_NOISE / 2) + (random.random() * RETRIEVAL_NOISE)
    await asyncio.sleep(sleep_time)
    return Response(hashlib.md5(path.encode()).hexdigest())
