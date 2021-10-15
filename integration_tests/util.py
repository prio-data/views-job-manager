from typing import List
import aiohttp
import settings

def steps_as_path(steps: List[str])->str:
    return "/f/" + "/".join(["/".join([s]*3) for s in steps])

async def clear_cache():
    async with aiohttp.ClientSession() as session:
        async with session.get(settings.CACHE_URL + "/clear/") as resp:
            print(await resp.text())
