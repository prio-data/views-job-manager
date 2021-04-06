import os
import environs
import requests
env = environs.Env()
env.read_env()

PROD = env.bool("PRODUCTION","false")

if not PROD:
    REST_ENV_URL = env.str("REST_ENV_URL",None)

    if REST_ENV_URL is not None:
        get_config = lambda k: requests.get(os.path.join(REST_ENV_URL),k).content.decode()
    else:
        env_friendly = lambda k: k.replace("-","_").upper()
        get_config = lambda k: env.str(env_friendly(k))

    CACHE_DIR = env.str("CACHE_DIR","cache")
    LOG_LEVEL="DEBUG"

ROUTER_URL = get_config("data-router-url")
JOB_TIMEOUT = int(get_config("job-timeout"))
JOB_RETRY = int(get_config("job-retry"))
