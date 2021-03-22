import os
import pickle

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
        except FileNotFoundError as fnf:
            raise KeyError from fnf

    def resolve(self,k):
        return os.path.join(self.base_path,k)

cache = FileSystemCache(settings.CACHE_DIR)
