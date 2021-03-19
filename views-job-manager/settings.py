import os
import environs
import requests
env = environs.Env()
env.read_env()

PROD = env.bool("PRODUCTION","false")

if not PROD:
    REST_ENV_URL = env.str("REST_ENV_URL")

    if REST_ENV_URL is not None:
        get_config = lambda k: requests.get(os.path.join(REST_ENV_URL),k).content.decode()
    else:
        env_friendly = lambda k: k.replace("-","_").upper()
        get_config = lambda k: env.str(env_friendly(k))

    CACHE_DIR = env.str("CACHE_DIR","cache")

ROUTER_URL = get_config("data-router-url")
