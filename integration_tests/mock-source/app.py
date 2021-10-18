
import os
import random
import asyncio
import hashlib
from fastapi import FastAPI, Response

CACHE_URL = os.getenv("CACHE_URL","http://mock-cache")
RETRIEVAL_TIME = int(os.getenv("RETRIEVAL_TIME", "1"))
RETRIEVAL_NOISE = int(os.getenv("RETRIEVAL_NOISE", "0"))

app = FastAPI()

STATE = {
    "number_of_requests": 0
    }

@app.delete("/requests/")
def clear_n_requests():
    STATE["number_of_requests"] = 0

@app.get("/requests/")
def show_n_requests():
    return STATE


@app.get("/{path:path}")
async def return_something(path: str):
    sleep_time = RETRIEVAL_TIME - (RETRIEVAL_NOISE / 2) + (random.random() * RETRIEVAL_NOISE)
    await asyncio.sleep(sleep_time)
    STATE["number_of_requests"] += 1
    return Response(hashlib.md5(path.encode()).hexdigest())
