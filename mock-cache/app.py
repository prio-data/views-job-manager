
import asyncio
import random
from fastapi import FastAPI, Response, UploadFile, File

app = FastAPI()

CACHE = {}

@app.get("/files/")
async def list_all():
    await asyncio.sleep(random.random()*.5)
    return [*CACHE.keys()]

@app.get("/files/{path:path}")
async def get_something(path: str):
    await asyncio.sleep(random.random()*.5)
    try:
        return Response(CACHE[path])
    except KeyError:
        return Response(status_code = 404)

@app.post("/files/{path:path}")
async def post_something(path: str, file: UploadFile = File(None)):
    await asyncio.sleep(random.random()*.5)
    if file is None:
        return Response(status_code = 400)
    else:
        CACHE[path] = await file.read()
        return Response(status_code = 201)
