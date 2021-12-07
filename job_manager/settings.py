
from environs import Env

env                    = Env()
env.read_env()

REDIS_HOST             = env.str("REDIS_HOST", "jobman-redis")
REDIS_PORT             = env.int("REDIS_PORT", 6379)
REDIS_DB               = env.int("REDIS_DB", 0)
REDIS_ERROR_KEY_PREFIX = env.str("REDIS_ERROR_SET_KEY", "jobman/errors:")
REDIS_JOB_KEY_PREFIX   = env.str("REDIS_ERROR_SET_KEY", "jobman/jobs:")

MAX_RETRIES            = env.int("MAX_RETRIES", 50)
RETRY_SLEEP            = env.int("RETRY_SLEEP", 5)
CHECK_ERRORS_EVERY     = env.int("CHECK_ERROR_DIVISOR", 5)

MAX_TIMEOUT_RETRIES    = env.int("MAX_TIMEOUT_RETRIES", 50)
TIMEOUT_COOLDOWN       = env.int("TIMEOUT_RETRY_SLEEP",7)

DATA_CACHE_URL         = env.str("DATA_CACHE_URL", "http://data-cache")
ROUTER_URL             = env.str("ROUTER_URL", "http://router")

LOG_LEVEL              = env.str("LOG_LEVEL", "WARNING").upper()
