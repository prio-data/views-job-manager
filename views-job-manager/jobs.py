
import os
import requests
import settings

def touch_router(path):
    requests.get(os.path.join(settings.ROUTER_URL,path))

