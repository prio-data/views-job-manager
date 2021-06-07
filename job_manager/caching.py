import logging
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError

from . import models

logger = logging.getLogger(__name__)

class NotCached(Exception):
    pass

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
        try:
            self.container_client.get_blob_client(k)
        except ResourceNotFoundError:
            return False
        else:
            return True

