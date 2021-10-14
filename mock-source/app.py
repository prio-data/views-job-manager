
import random
import asyncio
import os
import hashlib
from fastapi import FastAPI, Response

CACHE_URL = os.getenv("CACHE_URL","http://mock-cache")

app = FastAPI()

@app.get("/{path:path}")
async def return_something(path: str):
    await asyncio.sleep(random.random()*.5)
    return Response(hashlib.md5(path.encode()).hexdigest())
