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
                await resp.text()
                assert str(resp.status)[0] == "2"

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
        try:
            await self.get(key)
        except NotCached:
            return False
        else:
            return True

