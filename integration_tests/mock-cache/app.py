
import os
import asyncio
import random
from fastapi import FastAPI, Response, UploadFile, File, Depends

app = FastAPI()

CACHE = {}

RETRIEVAL_TIME = int(os.getenv("RETRIEVAL_TIME", "1"))
RETRIEVAL_NOISE = int(os.getenv("RETRIEVAL_NOISE", "0"))

async def with_sleep_time():
    return RETRIEVAL_TIME - (RETRIEVAL_NOISE / 2) + (random.random() * RETRIEVAL_NOISE)

@app.get("/clear/")
def clear_cache():
    for k in [*CACHE.keys()]:
        del CACHE[k]
    return Response(f"clear: {str(CACHE)}",status_code=202)

@app.get("/files/")
async def list_all(sleep_time = Depends(with_sleep_time)):
    await asyncio.sleep(sleep_time)
    return [*CACHE.keys()]

@app.get("/files/{path:path}")
async def get_something(path: str, sleep_time = Depends(with_sleep_time)):
    try:
        content = CACHE[path]
        await asyncio.sleep(sleep_time)
        return Response(content)
    except KeyError:
        return Response(status_code = 404)

@app.post("/files/{path:path}")
async def post_something(path: str, file: UploadFile = File(None), sleep_time = Depends(with_sleep_time)):
    # await asyncio.sleep(sleep_time)
    if file is None:
        return Response(status_code = 400)
    else:
        CACHE[path] = await file.read()
        return Response(status_code = 201)
