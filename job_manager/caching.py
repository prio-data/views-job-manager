from io import BytesIO
import logging
import requests
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError

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

class BlobStorageCache:
    def __init__(self, connection_string, container_name):
        self.client = BlobServiceClient.from_connection_string(connection_string)
        self.container_client = self.client.get_container_client(container_name)

    def set(self,k,v):
        blob_client = self.container_client.get_blob_client(k)
        blob_client.upload_blob(v)

    def get(self,k):
        try:
            blob = (self.container_client
                    .get_blob_client(k)
                    .download_blob()
                )
        except ResourceNotFoundError as rnf:
            raise NotCached from rnf

        return blob.content_as_bytes()

    def exists(self,k: str):
        blob_client = self.container_client.get_blob_client(k)
        return blob_client.exists()
