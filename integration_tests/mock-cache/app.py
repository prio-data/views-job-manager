
import logging
import os
import asyncio
import random
from fastapi import FastAPI, Response, UploadFile, File, Depends

app = FastAPI()

CACHE = {}

class Cache():
    def __init__(self, path: str = "/cache"):
        self._path = path
        try:
            os.makedirs(self._path)
        except FileExistsError:
            pass

    def _translate(self, key):
        return key.replace("/","_")

    def _key_path(self, key):
        return os.path.join(self._path, self._translate(key))

    def __getitem__(self, key):
        try:
            with open(self._key_path(key),"rb") as f:
                return f.read()
        except FileNotFoundError:
            raise KeyError

    def keys(self):
        return os.listdir(self._path)

    def __delitem__(self, key):
        os.unlink(self._key_path(key))

    def __setitem__(self, key, value):
        with open(self._key_path(key),"wb") as f:
            f.write(value)

RETRIEVAL_TIME = int(os.getenv("RETRIEVAL_TIME", "1"))
RETRIEVAL_NOISE = int(os.getenv("RETRIEVAL_NOISE", "0"))
CACHE = Cache()

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
async def post_something(path: str, file: UploadFile = File(None)):
    if file is None:
        return Response(status_code = 400)
    else:
        CACHE[path] = await file.read()
        return Response(status_code = 201)
