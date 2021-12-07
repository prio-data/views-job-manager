from typing import Tuple
import os
import aiohttp

class Api:
    def __init__(self, url):
        self.url = url
    async def touch(self,path: str)-> Tuple[int, str]:
        url = os.path.join(self.url, path)
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                content = await response.read()
                return (response.status, content)
