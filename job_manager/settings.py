
from environs import Env

env = Env()
env.read_env()

REDIS_HOST = env.str("REDIS_HOST", "jobman-redis")
REDIS_PORT = env.int("REDIS_PORT", 6379)
REDIS_DB = env.int("REDIS_DB", 0)

REDIS_ERROR_KEY_PREFIX = env.str("REDIS_ERROR_SET_KEY", "jobman/errors:")
REDIS_JOB_KEY_PREFIX = env.str("REDIS_ERROR_SET_KEY", "jobman/jobs:")

LOG_LEVEL = env.str("LOG_LEVEL", "WARNING").upper()

DATA_CACHE_URL = env.str("DATA_CACHE_URL")
ROUTER_URL = env.str("ROUTER_URL")

DATABASE_CONNECTION_STRING = env.str("DATABASE_CONNECTION_STRING","sqlite:///db.sqlite")

JOB_TIMEOUT = env.int("JOB_TIMEOUT", 40000)
JOB_RETRY = env.int("JOB_RETRY", 10)

MAX_RETRIES = env.int("MAX_RETRIES", 50)
