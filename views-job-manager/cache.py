import os
import pickle
import logging

import settings

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
            #pickle.dump(v,f)

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

cache = FileSystemCache(settings.CACHE_DIR)
