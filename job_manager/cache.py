from typing import List
from abc import ABC,abstractmethod
import os
import logging
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
from settings import config

class NotCached(Exception):
    pass

class FileSystemCache:
    def __init__(self,base_path):
        self.base_path = base_path
        try:
            os.makedirs(self.base_path)
        except FileExistsError:
            pass

    def set(self,k,v):
        folder,_ = os.path.split(k)
        logging.debug("Caching to %s",os.path.abspath(k))

        try:
            os.makedirs(folder)
        except FileExistsError:
            pass

        with open(self.resolve(k),"wb") as f:
            f.write(v)

    def exists(self,k):
        return os.path.exists(self.resolve(k))

    def get(self,k):
        try:
            with open(self.resolve(k),"rb") as f:
                return f.read()
        except FileNotFoundError as fnf:
            raise KeyError from fnf

    def resolve(self,k):
        return os.path.join(self.base_path,k)

class BlobStorageCache:
    def __init__(self,*_,**__):
        self.client = BlobServiceClient.from_connection_string(
                    config("BLOB_STORAGE_CONNECTION_STRING"),
                )
        self.container_client = self.client.get_container_client(
                    config("BLOB_STORAGE_ROUTER_CACHE"),
                )
    def exists(self,k):
        try: 
            self.get(k)
        except NotCached:
            return False
        else:
            return True

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

cache = BlobStorageCache()
