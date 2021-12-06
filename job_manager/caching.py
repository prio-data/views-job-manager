from io import BytesIO
import logging
import aiohttp

logger = logging.getLogger(__name__)

class NotCached(Exception):
    pass

class RESTCache:
    def __init__(self, url):
        self._url = url

    def url(self, path):
        return self._url + "/" + path

    async def set(self,key,content):
        async with aiohttp.ClientSession() as session:
            async with session.post(self.url(key), data = {"file": BytesIO(content)}) as resp:
                text = await resp.text()
                try:
                    assert str(resp.status)[0] == "2"
                except AssertionError:
                    raise ValueError(f"Remote returned {resp.status}: {text} when trying to cache {key}")

    async def get(self,key):
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url(key)) as resp:
                try:
                    assert resp.status != 404
                except AssertionError:
                    raise NotCached
                else:
                    return await resp.read()

    async def exists(self,key: str):
        async with aiohttp.ClientSession() as session:
            async with session.head(self.url(key)) as response:
                return str(response.status)[0] == "2"
