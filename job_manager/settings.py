"""
Required settings:
* Env:
    - KEY_VAULT_URL
* Config:
    - ROUTER_URL
    - JOB_TIMEOUT
    - JOB_RETRY
"""
import environs

env = environs.Env()
env.read_env()
config = env

LOG_LEVEL = "WARNING"

DATA_CACHE_URL = env.str("DATA_CACHE_URL")
ROUTER_URL = env.str("ROUTER_URL")

DATABASE_CONNECTION_STRING = env.str("DATABASE_CONNECTION_STRING","sqlite:///db.sqlite")

JOB_TIMEOUT = env.int("JOB_TIMEOUT", 40000)
JOB_RETRY = env.int("JOB_RETRY", 10)
