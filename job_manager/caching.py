from io import BytesIO
import logging
import requests
logger = logging.getLogger(__name__)

class NotCached(Exception):
    pass

class RESTCache:
    def __init__(self, url):
        self._url = url

    def url(self, path):
        return self._url + "/" + path

    def store(self,content,key):
        rsp = requests.post(self.url(key), files = {"file": BytesIO(content)})
        rsp.raise_for_status()

    def get(self,key):
        rsp = requests.get(self.url(key))

        try:
            assert rsp.status_code != 404
        except AssertionError:
            raise NotCached

        return rsp.content

    def exists(self,key: str):
        try:
            self.get(key)
        except NotCached:
            return False
        else:
            return True
