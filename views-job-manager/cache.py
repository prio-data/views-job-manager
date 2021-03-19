import os
import pickle

import settings
import hashlib
digest = lambda k: hashlib.md5(k.encode()).hexdigest()

class FileSystemCache:
    def __init__(self,base_path):
        self.base_path = base_path
        try:
            os.makedirs(self.base_path)
        except FileExistsError:
            pass
    def set(self,k,v):
        folder,_ = os.path.split(k)
        try:
            os.makedirs(folder)
        except FileExistsError:
            pass

        with open(self.resolve(k),"wb") as f:
            pickle.dump(v,f)
    def get(self,k):
        try:
            with open(self.resolve(k),"rb") as f:
                return pickle.load(f)
        except FileNotFoundError:
            raise KeyError
    def resolve(self,k):
        return os.path.join(self.base_path,digest(k))

cache = FileSystemCache(settings.CACHE_DIR)
