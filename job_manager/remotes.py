import os
import requests

class Api:
    def __init__(self, url):
        self.url = url

    def touch(self,path,cache):
        url = os.path.join(self.url, path)
        try:
            response = requests.get(url+"?touch=true")
            assert response.status_code == 200
        except AssertionError:
            raise requests.HTTPError(response = response) from AssertionError

        cache.store(response.content, path)

